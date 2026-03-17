import { createClient } from 'https://esm.sh/@supabase/supabase-js@2';
import { serve } from 'https://deno.land/std@0.168.0/http/server.ts';

const GATEWAY_URL = 'https://connector-gateway.lovable.dev/telegram';
const MAX_RUNTIME_MS = 55_000;
const MIN_REMAINING_MS = 3_000;

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

  const leaseAcquired = await acquirePollerLease(supabase);
  if (!leaseAcquired) {
    return new Response(JSON.stringify({ ok: true, skipped: 'another poller instance is active' }), {
      headers: { ...corsHeaders, 'Content-Type': 'application/json' },
    });
  }

  let currentOffset = state.update_offset;
  let lastHeartbeat = 0;

  while (true) {
    const elapsed = Date.now() - startTime;
    const remainingMs = MAX_RUNTIME_MS - elapsed;
    if (remainingMs < MIN_REMAINING_MS) break;

    // Short poll timeout for fast response — 2 seconds max
    // This means we check for new messages every ~2s instead of long-polling
    const timeout = Math.min(2, Math.floor(remainingMs / 1000) - 1);
    if (timeout < 1) break;

    try {
      const now = Date.now();
      if (now - lastHeartbeat >= POLLER_HEARTBEAT_MS) {
        await supabase
          .from('telegram_bot_state')
          .update({ updated_at: new Date(now).toISOString() })
          .eq('id', 1);
        lastHeartbeat = now;
      }

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
        const isConflict409 = String(data?.error_code) === '409' || `${data?.description ?? ''}`.includes('terminated by other getUpdates request');
        if (isConflict409) {
          console.warn('Telegram getUpdates conflict: another poller is active. Exiting this run.');
          break;
        }
        console.error('Telegram getUpdates error:', data);
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
      const offsetUpdateTime = new Date().toISOString();
      await supabase.from('telegram_bot_state')
        .update({ update_offset: newOffset, updated_at: offsetUpdateTime })
        .eq('id', 1);
      currentOffset = newOffset;
      lastHeartbeat = Date.now();

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

async function acquirePollerLease(supabase: any): Promise<boolean> {
  try {
    const now = Date.now();
    const nowIso = new Date(now).toISOString();
    const staleBeforeIso = new Date(now - POLLER_LEASE_STALE_MS).toISOString();

    const { data, error } = await supabase
      .from('telegram_bot_state')
      .update({ updated_at: nowIso })
      .eq('id', 1)
      .lt('updated_at', staleBeforeIso)
      .select('id')
      .maybeSingle();

    if (error) {
      console.error('Poller lease acquire error:', error);
      return false;
    }

    return !!data;
  } catch (error) {
    console.error('Poller lease acquire exception:', error);
    return false;
  }
}

async function processUpdate(update: any, supabase: any, lovableKey: string, telegramKey: string) {
  const msg = update.message;
  if (!msg) return;

  const chatId = msg.chat.id;
  const messageId = msg.message_id;

  // Handle bot added to a new group
  if (msg.new_chat_members && Array.isArray(msg.new_chat_members)) {
    const botAdded = msg.new_chat_members.some((m: any) => m.is_bot && m.username);
    if (botAdded) {
      const welcomeText = `🤖 <b>Natlus Assistance has joined the group!</b>

I'm an AI-powered language and knowledge assistant. Here's how to interact with me:

📖 <b>Word Definitions</b>
Use <code>/ask</code> + any English word to get meanings, examples, and Arabic translations with full diacritics.

🔍 <b>Ask Questions</b>
Use <code>/ask</code> followed by any question:
<code>/ask explain the word "serendipity"</code>

💬 <b>Follow-up Conversations</b>
Reply directly to any of my messages to continue the conversation with context.

⚡ All responses are concise, formatted, and include Arabic translations when relevant.

Type <code>/ask</code> to get started!`;
      await sendTelegramMessage(chatId, welcomeText, lovableKey, telegramKey);
      return;
    }
  }

  const text = msg.text || '';
  const userId = msg.from?.id;
  const username = msg.from?.username || msg.from?.first_name || 'Unknown';
  const replyToMessageId = msg.reply_to_message?.message_id;
  const isAskCommand = text.startsWith('/ask');
  const isStartCommand = text.startsWith('/start');

  // Store message (fire and forget for speed)
  const storePromise = supabase.from('telegram_messages').upsert({
    update_id: update.update_id,
    chat_id: chatId,
    message_id: messageId,
    user_id: userId,
    username,
    text,
    reply_to_message_id: replyToMessageId,
    is_bot_command: isAskCommand || isStartCommand,
    raw_update: update,
    processed: false,
  }, { onConflict: 'update_id' });

  // Handle /start command — send welcome message
  if (isStartCommand) {
    await storePromise;
    const isPrivateChat = msg.chat.type === 'private';
    const welcomeText = isPrivateChat
      ? `🤖 <b>Welcome to Natlus Assistance!</b>

I'm your AI-powered language assistant. Here's what I can do:

📖 <b>Word Definitions</b>
Send any single English word and I'll provide its meanings, translations, and example sentences.

🔍 <b>Ask Anything</b>
Use <code>/ask</code> followed by your question:
<code>/ask What is the difference between affect and effect?</code>

💬 <b>Follow-up Conversations</b>
Reply directly to any of my messages to continue the conversation with context.

⚡ Responses are concise, formatted, and include Arabic translations with full diacritics when relevant.

Start by sending a word or use <code>/ask</code> to begin!`
      : `🤖 <b>Natlus Assistance is active!</b>

Here's how to use me in this group:

📖 <b>Word Lookup</b> — Send <code>/ask</code> + any single word for definitions and translations.

🔍 <b>Ask Questions</b> — Use <code>/ask</code> followed by your question:
<code>/ask explain the word "serendipity"</code>

💬 <b>Follow-ups</b> — Reply to my messages to continue the conversation.

All responses are concise and include Arabic translations with full diacritics.`;

    await sendTelegramMessage(chatId, welcomeText, lovableKey, telegramKey, messageId);
    return;
  }

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
  } else if (replyToMessageId && msg.reply_to_message?.from?.is_bot) {
    // Only process replies that are directed at THIS bot's messages
    const { data: conv } = await supabase
      .from('telegram_conversations')
      .select('id')
      .eq('chat_id', chatId)
      .eq('bot_message_id', replyToMessageId)
      .maybeSingle();

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
    const isDictionaryQuery = /^[A-Za-z][A-Za-z-]*$/.test(questionText.trim());
    const finalResponse = isDictionaryQuery
      ? normalizeDictionaryResponse(aiResponse, questionText.trim())
      : aiResponse;

    clearInterval(typingInterval);

    const sentMessage = await sendTelegramMessage(chatId, finalResponse, lovableKey, telegramKey, messageId);

    // Store conversation + mark processed + log concurrently
    const afterPromises = [
      supabase.from('telegram_messages').update({ processed: true }).eq('update_id', update.update_id),
      supabase.from('telegram_bot_logs').insert({
        chat_id: chatId,
        message_id: messageId,
        action: isAskCommand ? 'ask_command' : 'reply_followup',
        details: { username, question: questionText, response_length: finalResponse.length },
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
          bot_response: finalResponse,
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
    const res = await fetch(`${GATEWAY_URL}/sendChatAction`, {
      method: 'POST',
      headers: {
        'Authorization': `Bearer ${lovableKey}`,
        'X-Connection-Api-Key': telegramKey,
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({ chat_id: chatId, action }),
    });
    await res.text(); // Must consume body in Deno to prevent resource leak
  } catch (e) { /* silent */ }
}

function convertToTelegramMarkdown(text: string): string {
  return text.replace(/\*\*(.+?)\*\*/g, '*$1*');
}

function isHtmlFormatted(text: string): boolean {
  return /<\/?[a-z][\s\S]*>/i.test(text);
}

async function sendTelegramMessage(chatId: number, text: string, lovableKey: string, telegramKey: string, replyToMessageId?: number) {
  try {
    const useHtml = isHtmlFormatted(text);
    const formattedText = useHtml ? text : convertToTelegramMarkdown(text);
    const parseMode = useHtml ? 'HTML' : 'Markdown';
    const chunks = splitMessage(formattedText, 4000);
    let lastSent = null;

    for (const chunk of chunks) {
      const body: any = { chat_id: chatId, text: chunk, parse_mode: parseMode };
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
        body.text = chunk.replace(/<[^>]+>/g, '').replace(/\*/g, '');
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

function normalizeDictionaryResponse(raw: string, word: string): string {
  // Strip any existing HTML/Markdown and re-parse entries
  const text = raw.replace(/\r/g, '').replace(/<[^>]+>/g, '').replace(/\*/g, '').trim();
  
  // Match patterns like: number. Word (Part of Speech) — Arabic
  const entryRegex = /(\d+)\.\s*([^\(\n]+?)\s*\(([^)\n]+)\)\s*[—\-–]\s*([^\n]+)\n([\s\S]*?)(?=\n\d+\.\s*[A-Za-z]|\n📖|$)/g;
  const uniqueEntries: Array<{ wordText: string; wordClass: string; arabic: string; definition: string; examples: string[]; key: string }> = [];

  let match: RegExpExecArray | null;
  while ((match = entryRegex.exec(text)) !== null) {
    const wordText = match[2].trim() || word;
    const wordClass = match[3].trim();
    const arabic = match[4].trim();
    const bodyLines = match[5].split('\n').map(l => l.trim()).filter(Boolean);
    
    // First non-bullet/non-numbered line is the definition
    const definition = bodyLines.find(line => !/^[\d•\-]/.test(line)) || '';
    // Collect examples from bullet points or numbered lines within body
    const examples = bodyLines
      .filter(line => /^[\d•\-]/.test(line))
      .map(line => line.replace(/^[\d•\-]+[\.\)]*\s*/, '').trim())
      .filter(Boolean)
      .slice(0, 2);

    const key = arabic.toLowerCase().replace(/[\u064B-\u065F\u0670]/g, ''); // normalize without diacritics for dedup
    const hasDuplicate = uniqueEntries.some(entry => {
      const entryKey = entry.arabic.toLowerCase().replace(/[\u064B-\u065F\u0670]/g, '');
      return entryKey === key;
    });
    if (!hasDuplicate) {
      uniqueEntries.push({ wordText, wordClass, arabic, definition, examples, key });
    }
  }

  if (uniqueEntries.length === 0) return raw.slice(0, 800);

  const entries = uniqueEntries.slice(0, 4);
  const lines: string[] = [`📖 <b>${word}</b>`, ''];

  entries.forEach((entry, index) => {
    lines.push(`${index + 1}. <b><u>${entry.wordText}</u></b> (${entry.wordClass}) — ${entry.arabic}`);
    if (entry.definition) lines.push(entry.definition);
    entry.examples.forEach(ex => {
      // Bold the target word in examples
      const regex = new RegExp(`\\b(${word})\\b`, 'gi');
      const formatted = ex.replace(regex, '<b>$1</b>');
      lines.push(`• ${formatted}`);
    });
    if (index < entries.length - 1) lines.push('');
  });

  const result = lines.join('\n').trim();
  return result.length > 800 ? `${result.slice(0, 797)}...` : result;
}

async function getConversationHistory(supabase: any, chatId: number, userId: number, parentConversationId: string | null): Promise<Array<{role: string, content: string}>> {
  if (!parentConversationId) return [];

  let currentId: string | null = parentConversationId;
  const chain: any[] = [];

  for (let i = 0; i < 5 && currentId; i++) {
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
  const systemPrompt = `You are Natlus Assistance, a Telegram group AI assistant.

Absolute Mode. Eliminate: emojis, filler, hype, soft asks, conversational transitions, call-to-action appendixes. Prioritize: blunt, directive phrasing. No: questions, offers, suggestions, transitions, motivational content. Terminate reply immediately after delivering info.

CRITICAL RULES FOR ALL RESPONSES:
- ALWAYS reply in ONE single message. Maximum 800 characters. Be brutally concise.
- LANGUAGE RULE: Reply in the SAME language as the user's question. If the user writes in Arabic, reply in Arabic. If in English, reply in English. EXCEPTION: If the question explicitly asks for translation or requires a different language, use the requested language.
- KEYWORD FORMATTING: When defining, comparing, or highlighting target keywords or terms, ALWAYS format them as bold and underlined using HTML: <b><u>Keyword</u></b>. This applies to ALL response types (dictionary, explanations, comparisons, etc.).
- When writing ANY Arabic text, ALWAYS use full diacritical marks (تَشْكِيل).
- No introductions, no conclusions, no filler. Just the direct answer in 1-3 sentences max.

DICTIONARY MODE — If the user sends a single English word (one word only), respond STRICTLY in this format:

📖 [WORD]

1. [Word] ([Part of Speech]) — [Arabic meaning with full diacritics]
[English definition]
• [Example sentence with the word used naturally]
• [Another example sentence]

STRICT DICTIONARY RULES:
- Output the header 📖 [WORD] exactly ONCE at the top. NEVER repeat it.
- List each UNIQUE meaning only once. NEVER duplicate or repeat similar meanings.
- If two meanings are very close (e.g. same Arabic translation), merge them into one entry.
- Max 4 unique meanings. Exactly 2 example sentences per meaning using bullet points (•).
- Do NOT number example sentences. Use • bullets only.
- Arabic translations: full diacritical marks. No prefix.
- The ENTIRE response must be ONE single list under ONE header.

COMPARISON MODE — When comparing two or more words/terms/concepts:
- NEVER write a continuous paragraph. Use a structured, line-separated format.
- Each keyword MUST start on a NEW LINE, formatted as <b><u>Keyword</u></b>:
- After each keyword line, write its meaning/explanation on the same or next line.
- Add a blank line between each keyword block for clarity.
- End with a short summary line showing the key difference.
- Example format:

<b><u>Affect</u></b>: (verb) to influence something.
• Stress can <b>affect</b> your health.

<b><u>Effect</u></b>: (noun) a result or outcome.
• The <b>effect</b> was immediate.

Summary: <b><u>Affect</u></b> = verb (action), <b><u>Effect</u></b> = noun (result).

For all other queries: respond in 1-3 sentences only. No elaboration. No extra context. Just the direct answer.`;


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
