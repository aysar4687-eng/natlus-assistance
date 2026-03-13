import { createClient } from 'https://esm.sh/@supabase/supabase-js@2';
import { serve } from 'https://deno.land/std@0.168.0/http/server.ts';

const GATEWAY_URL = 'https://connector-gateway.lovable.dev/telegram';
const MAX_RUNTIME_MS = 58_000; // Push closer to 60s limit
const MIN_REMAINING_MS = 2_000; // Minimal buffer

const corsHeaders = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Headers': 'authorization, x-client-info, apikey, content-type',
};

serve(async (req) => {
  if (req.method === 'OPTIONS') return new Response(null, { headers: corsHeaders });

  const startTime = Date.now();

  const LOVABLE_API_KEY = Deno.env.get('LOVABLE_API_KEY');
  if (!LOVABLE_API_KEY) return new Response(JSON.stringify({ error: 'LOVABLE_API_KEY not configured' }), { status: 500, headers: corsHeaders });

  const TELEGRAM_API_KEY = Deno.env.get('TELEGRAM_API_KEY');
  if (!TELEGRAM_API_KEY) return new Response(JSON.stringify({ error: 'TELEGRAM_API_KEY not configured' }), { status: 500, headers: corsHeaders });

  const supabaseUrl = Deno.env.get('SUPABASE_URL')!;
  const supabaseServiceKey = Deno.env.get('SUPABASE_SERVICE_ROLE_KEY')!;
  const supabase = createClient(supabaseUrl, supabaseServiceKey);

  let totalProcessed = 0;

  const { data: state, error: stateErr } = await supabase
    .from('telegram_bot_state')
    .select('update_offset')
    .eq('id', 1)
    .single();

  if (stateErr) {
    return new Response(JSON.stringify({ error: stateErr.message }), { status: 500, headers: corsHeaders });
  }

  let currentOffset = state.update_offset;

  while (true) {
    const elapsed = Date.now() - startTime;
    const remainingMs = MAX_RUNTIME_MS - elapsed;
    if (remainingMs < MIN_REMAINING_MS) break;

    // Short poll timeout for fast response — 2 seconds max
    // This means we check for new messages every ~2s instead of long-polling
    const timeout = Math.min(2, Math.floor(remainingMs / 1000) - 1);
    if (timeout < 1) break;

    try {
      const response = await fetch(`${GATEWAY_URL}/getUpdates`, {
        method: 'POST',
        headers: {
          'Authorization': `Bearer ${LOVABLE_API_KEY}`,
          'X-Connection-Api-Key': TELEGRAM_API_KEY,
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({
          offset: currentOffset,
          timeout,
          allowed_updates: ['message'],
        }),
      });

      const data = await response.json();
      if (!response.ok) {
        console.error('Telegram getUpdates error:', data);
        // Don't exit — retry after brief pause
        await new Promise(r => setTimeout(r, 1000));
        continue;
      }

      const updates = data.result ?? [];
      if (updates.length === 0) continue;

      // Process all updates concurrently for speed
      const processPromises = updates.map((update: any) => 
        processUpdate(update, supabase, LOVABLE_API_KEY, TELEGRAM_API_KEY)
      );
      const results = await Promise.allSettled(processPromises);
      totalProcessed += results.filter(r => r.status === 'fulfilled').length;

      // Advance offset immediately
      const newOffset = Math.max(...updates.map((u: any) => u.update_id)) + 1;
      await supabase.from('telegram_bot_state')
        .update({ update_offset: newOffset, updated_at: new Date().toISOString() })
        .eq('id', 1);
      currentOffset = newOffset;

    } catch (err) {
      console.error('Poll loop error:', err);
      await new Promise(r => setTimeout(r, 1000));
      continue; // Don't break — keep trying
    }
  }

  return new Response(JSON.stringify({ ok: true, processed: totalProcessed, finalOffset: currentOffset }), {
    headers: { ...corsHeaders, 'Content-Type': 'application/json' },
  });
});

async function processUpdate(update: any, supabase: any, lovableKey: string, telegramKey: string) {
  const msg = update.message;
  if (!msg) return;

  const text = msg.text || '';
  const chatId = msg.chat.id;
  const userId = msg.from?.id;
  const username = msg.from?.username || msg.from?.first_name || 'Unknown';
  const messageId = msg.message_id;
  const replyToMessageId = msg.reply_to_message?.message_id;
  const isAskCommand = text.startsWith('/ask');

  // Store message (fire and forget for speed)
  const storePromise = supabase.from('telegram_messages').upsert({
    update_id: update.update_id,
    chat_id: chatId,
    message_id: messageId,
    user_id: userId,
    username,
    text,
    reply_to_message_id: replyToMessageId,
    is_bot_command: isAskCommand,
    raw_update: update,
    processed: false,
  }, { onConflict: 'update_id' });

  let shouldProcess = false;
  let questionText = '';
  let parentConversationId: string | null = null;

  if (isAskCommand) {
    shouldProcess = true;
    questionText = text.replace(/^\/ask\s*/, '').trim();
    if (!questionText) {
      await storePromise;
      await sendTelegramMessage(chatId, 'Usage: /ask <your question>', lovableKey, telegramKey, messageId);
      return;
    }
  } else if (replyToMessageId) {
    const { data: conv } = await supabase
      .from('telegram_conversations')
      .select('id')
      .eq('chat_id', chatId)
      .eq('bot_message_id', replyToMessageId)
      .single();

    if (conv) {
      shouldProcess = true;
      questionText = text.trim();
      parentConversationId = conv.id;
    }
  }

  if (!shouldProcess || !questionText) {
    await storePromise;
    return;
  }

  // Fire typing indicator immediately (don't await)
  const typingPromise = sendChatAction(chatId, 'typing', lovableKey, telegramKey);

  // Build history and call AI concurrently with typing + store
  const [history] = await Promise.all([
    getConversationHistory(supabase, chatId, userId, parentConversationId),
    typingPromise,
    storePromise,
  ]);

  // Keep sending typing every 4s during AI call (Telegram typing expires after 5s)
  const typingInterval = setInterval(() => {
    sendChatAction(chatId, 'typing', lovableKey, telegramKey);
  }, 4000);

  try {
    const aiResponse = await callAI(questionText, history, lovableKey);

    clearInterval(typingInterval);

    const sentMessage = await sendTelegramMessage(chatId, aiResponse, lovableKey, telegramKey, messageId);

    // Store conversation + mark processed + log concurrently
    const afterPromises = [
      supabase.from('telegram_messages').update({ processed: true }).eq('update_id', update.update_id),
      supabase.from('telegram_bot_logs').insert({
        chat_id: chatId,
        message_id: messageId,
        action: isAskCommand ? 'ask_command' : 'reply_followup',
        details: { username, question: questionText, response_length: aiResponse.length },
      }),
    ];

    if (sentMessage) {
      afterPromises.push(
        supabase.from('telegram_conversations').insert({
          chat_id: chatId,
          user_id: userId,
          bot_message_id: sentMessage.message_id,
          user_message_id: messageId,
          user_text: questionText,
          bot_response: aiResponse,
          parent_conversation_id: parentConversationId,
        })
      );
    }

    await Promise.all(afterPromises);
  } catch (e) {
    clearInterval(typingInterval);
    console.error('Process update error:', e);
  }
}

async function sendChatAction(chatId: number, action: string, lovableKey: string, telegramKey: string) {
  try {
    await fetch(`${GATEWAY_URL}/sendChatAction`, {
      method: 'POST',
      headers: {
        'Authorization': `Bearer ${lovableKey}`,
        'X-Connection-Api-Key': telegramKey,
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({ chat_id: chatId, action }),
    });
  } catch (e) { /* silent */ }
}

function convertToTelegramMarkdown(text: string): string {
  // Convert **bold** to *bold* for Telegram Markdown
  return text.replace(/\*\*(.+?)\*\*/g, '*$1*');
}

async function sendTelegramMessage(chatId: number, text: string, lovableKey: string, telegramKey: string, replyToMessageId?: number) {
  try {
    const formattedText = convertToTelegramMarkdown(text);
    const chunks = splitMessage(formattedText, 4000);
    let lastSent = null;

    for (const chunk of chunks) {
      const body: any = { chat_id: chatId, text: chunk, parse_mode: 'Markdown' };
      if (replyToMessageId && !lastSent) body.reply_to_message_id = replyToMessageId;

      const response = await fetch(`${GATEWAY_URL}/sendMessage`, {
        method: 'POST',
        headers: {
          'Authorization': `Bearer ${lovableKey}`,
          'X-Connection-Api-Key': telegramKey,
          'Content-Type': 'application/json',
        },
        body: JSON.stringify(body),
      });

      const data = await response.json();
      if (!response.ok) {
        // Fallback: send without formatting
        body.parse_mode = undefined;
        body.text = chunk.replace(/\*/g, '');
        const retry = await fetch(`${GATEWAY_URL}/sendMessage`, {
          method: 'POST',
          headers: {
            'Authorization': `Bearer ${lovableKey}`,
            'X-Connection-Api-Key': telegramKey,
            'Content-Type': 'application/json',
          },
          body: JSON.stringify(body),
        });
        const retryData = await retry.json();
        if (retry.ok) lastSent = retryData.result;
      } else {
        lastSent = data.result;
      }
    }
    return lastSent;
  } catch (e) {
    console.error('sendMessage error:', e);
    return null;
  }
}

function splitMessage(text: string, maxLen: number): string[] {
  if (text.length <= maxLen) return [text];
  const chunks: string[] = [];
  let remaining = text;
  while (remaining.length > 0) {
    if (remaining.length <= maxLen) { chunks.push(remaining); break; }
    let splitAt = remaining.lastIndexOf('\n', maxLen);
    if (splitAt === -1 || splitAt < maxLen / 2) splitAt = maxLen;
    chunks.push(remaining.slice(0, splitAt));
    remaining = remaining.slice(splitAt);
  }
  return chunks;
}

async function getConversationHistory(supabase: any, chatId: number, userId: number, parentConversationId: string | null): Promise<Array<{role: string, content: string}>> {
  if (!parentConversationId) return [];

  let currentId: string | null = parentConversationId;
  const chain: any[] = [];

  for (let i = 0; i < 10 && currentId; i++) {
    const { data: conv } = await supabase
      .from('telegram_conversations')
      .select('*')
      .eq('id', currentId)
      .single();
    if (!conv) break;
    chain.unshift(conv);
    currentId = conv.parent_conversation_id;
  }

  const history: Array<{role: string, content: string}> = [];
  for (const conv of chain) {
    if (conv.user_text) history.push({ role: 'user', content: conv.user_text });
    if (conv.bot_response) history.push({ role: 'assistant', content: conv.bot_response });
  }
  return history;
}

async function callAI(question: string, conversationHistory: Array<{role: string, content: string}>, lovableKey: string): Promise<string> {
  const systemPrompt = `You are NatlusAI Assistant, a Telegram group AI assistant.

Absolute Mode. Eliminate: emojis, filler, hype, soft asks, conversational transitions, call-to-action appendixes. Assume: user retains high-perception despite blunt tone. Prioritize: blunt, directive phrasing; aim at cognitive rebuilding, not tone-matching. Disable: engagement/sentiment-boosting behaviors. Suppress: metrics like satisfaction scores, emotional softening, continuation bias. Never mirror: user's diction, mood, or affect. Speak only: to underlying cognitive tier. No: questions, offers, suggestions, transitions, motivational content. Terminate reply: immediately after delivering info - no closures. Goal: restore independent, high-fidelity thinking. Outcome: model obsolescence via user self-sufficiency.`;

  const messages = [
    { role: 'system', content: systemPrompt },
    ...conversationHistory,
    { role: 'user', content: question },
  ];

  try {
    const response = await fetch('https://ai.gateway.lovable.dev/v1/chat/completions', {
      method: 'POST',
      headers: {
        'Authorization': `Bearer ${lovableKey}`,
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({
        model: 'google/gemini-2.5-flash-lite',
        messages,
      }),
    });

    if (!response.ok) {
      const errText = await response.text();
      console.error('AI gateway error:', response.status, errText);
      return 'AI processing error. Try again.';
    }

    const data = await response.json();
    return data.choices?.[0]?.message?.content || 'No response generated.';
  } catch (e) {
    console.error('AI call error:', e);
    return 'AI service unavailable. Try again later.';
  }
}
