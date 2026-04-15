import { createClient } from 'https://esm.sh/@supabase/supabase-js@2';
import { serve } from 'https://deno.land/std@0.168.0/http/server.ts';

const GATEWAY_URL = 'https://connector-gateway.lovable.dev/telegram';
const MAX_RUNTIME_MS = 12_000;  // 12s — fits inside a 15s staggered cron window
const MIN_REMAINING_MS = 2_000;

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

    const timeout = Math.min(8, Math.floor(remainingMs / 1000) - 2);
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

  const chatId = msg.chat.id;
  const messageId = msg.message_id;

  // Handle bot added to a new group
  if (msg.new_chat_members && Array.isArray(msg.new_chat_members)) {
    // Get bot info to check if THIS bot was added
    const botInfoRes = await fetch(`${GATEWAY_URL}/getMe`, {
      method: 'POST',
      headers: {
        'Authorization': `Bearer ${lovableKey}`,
        'X-Connection-Api-Key': telegramKey,
        'Content-Type': 'application/json',
      },
    });
    const botInfo = await botInfoRes.json();
    const botId = botInfo?.result?.id;
    const botAdded = botId && msg.new_chat_members.some((m: any) => m.id === botId);
    if (botAdded) {
      const welcomeText = `🤖 <b>Natlus Assistance has joined the group!</b>

I'm an AI-powered language and knowledge assistant. Here's how to interact with me:

📖 <b>Word Definitions</b>
Use <code>/ask</code> + any English word to get meanings, examples, and Arabic translations with full diacritics.

🔄 <b>Reply Shortcut</b>
Reply to any message with <code>/ask</code> to ask the bot about that text — no need to retype!

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
  const isCorrectCommand = text.startsWith('/correct');
  const isStartCommand = text.startsWith('/start');
  const isHelpCommand = text.startsWith('/help');

  // Store message (fire and forget for speed)
  const storePromise = supabase.from('telegram_messages').upsert({
    update_id: update.update_id,
    chat_id: chatId,
    message_id: messageId,
    user_id: userId,
    username,
    text,
    reply_to_message_id: replyToMessageId,
    is_bot_command: isAskCommand || isCorrectCommand || isStartCommand || isHelpCommand,
    raw_update: update,
    processed: false,
  }, { onConflict: 'update_id' });

  // Handle /help command
  if (isHelpCommand) {
    await storePromise;
    const isPrivateChat = msg.chat.type === 'private';
    const helpText = `📚 <b>Natlus Assistance — Command Reference</b>

━━━━━━━━━━━━━━━━━━━━━

🔹 <code>/ask</code> <i>&lt;word or question&gt;</i>
Get definitions, meanings, Arabic translations with full diacritics, and example sentences.
<b>Examples:</b>
  • <code>/ask eloquent</code> — Word definition
  • <code>/ask What is the past tense of "go"?</code> — Grammar question

🔹 <code>/correct</code>
Reply to any message with <code>/correct</code> to get smart grammar and text correction with two refined versions and learner insights.
<b>Example:</b> Someone writes a sentence with errors → Reply with <code>/correct</code> → Get corrections + professional revision + tips.

🔹 <b>Reply with</b> <code>/ask</code>
Reply to any message in the group with <code>/ask</code> to ask the bot about that text — no need to retype it!
<b>Example:</b> Someone writes "serendipity" → Reply to their message with <code>/ask</code> → Get the full definition.

🔹 <code>/start</code>
Display the welcome message and bot introduction.

🔹 <code>/help</code>
Show this command reference guide.

━━━━━━━━━━━━━━━━━━━━━

💡 <b>Tips & Features:</b>

${isPrivateChat ? `• <b>Direct Messages:</b> Just type any English word directly — no command needed! I'll instantly provide its definition and Arabic translation.
• <b>Questions:</b> Use <code>/ask</code> for full questions, or just send a single word for quick lookup.` : `• <b>In Groups:</b> Always use <code>/ask</code> before your word or question so I know it's for me.
• <b>Reply Shortcut:</b> See an interesting word or question? Just reply to it with <code>/ask</code> — I'll handle the rest!`}
• <b>Follow-ups:</b> Reply directly to any of my messages to continue the conversation with full context.
• <b>Multi-meaning Words:</b> I provide all relevant meanings with part of speech, Arabic translation, and usage examples.
• <b>Concise Responses:</b> All answers are brief, formatted, and optimized for quick reading.
• <b>Concise Responses:</b> All answers are brief, formatted, and optimized for quick reading.

━━━━━━━━━━━━━━━━━━━━━

⚡ <i>Powered by AI — Fast, accurate, and always ready.</i>`;

    await sendTelegramMessage(chatId, helpText, lovableKey, telegramKey, messageId);
    return;
  }

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

Type <code>/help</code> for full command reference.
Start by sending a word or use <code>/ask</code> to begin!`
      : `🤖 <b>Natlus Assistance is active!</b>

Here's how to use me in this group:

📖 <b>Word Lookup</b> — Send <code>/ask</code> + any single word for definitions and translations.

🔍 <b>Ask Questions</b> — Use <code>/ask</code> followed by your question:
<code>/ask explain the word "serendipity"</code>

💬 <b>Follow-ups</b> — Reply to my messages to continue the conversation.

Type <code>/help</code> for full command reference.
All responses are concise and include Arabic translations with full diacritics.`;

    await sendTelegramMessage(chatId, welcomeText, lovableKey, telegramKey, messageId);
    return;
  }

  let shouldProcess = false;
  let questionText = '';
  let parentConversationId: string | null = null;
  let isCorrection = false;

  if (isCorrectCommand) {
    // /correct command — must be a reply to a message
    let textToCorrect = text.replace(/^\/correct(@\w+)?\s*/, '').trim();
    if (!textToCorrect && replyToMessageId && msg.reply_to_message?.text) {
      textToCorrect = msg.reply_to_message.text.trim();
    }
    if (!textToCorrect) {
      await storePromise;
      const usageText = '📌 <b>Usage:</b>\n• Reply to any message with <code>/correct</code> to get grammar correction\n• Or: <code>/correct &lt;your text&gt;</code>';
      const sentMessage = await sendTelegramMessage(chatId, usageText, lovableKey, telegramKey, messageId);
      if (sentMessage) {
        await supabase.from('telegram_conversations').insert({
          chat_id: chatId, user_id: userId, bot_message_id: sentMessage.message_id,
          user_message_id: messageId, user_text: '/correct', bot_response: usageText,
          parent_conversation_id: null,
        });
      }
      return;
    }
    shouldProcess = true;
    questionText = textToCorrect;
    isCorrection = true;
  } else if (isAskCommand) {
    shouldProcess = true;
    questionText = text.replace(/^\/ask(@\w+)?\s*/, '').trim();
    // If /ask is used as a reply to another message and no text provided, use the replied message's text
    if (!questionText && replyToMessageId && msg.reply_to_message?.text) {
      questionText = msg.reply_to_message.text.trim();
    }
    if (!questionText) {
      await storePromise;
      const usageText = '📌 <b>Usage:</b>\n• <code>/ask &lt;word or question&gt;</code>\n• Reply to any message with <code>/ask</code> to ask about it';
      const sentMessage = await sendTelegramMessage(chatId, usageText, lovableKey, telegramKey, messageId);
      if (sentMessage) {
        await supabase.from('telegram_conversations').insert({
          chat_id: chatId,
          user_id: userId,
          bot_message_id: sentMessage.message_id,
          user_message_id: messageId,
          user_text: questionText || '/ask',
          bot_response: usageText,
          parent_conversation_id: parentConversationId,
        });
      }
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
  } else if (msg.chat.type === 'private' && text.trim() && !text.startsWith('/')) {
    // In private chat, process any plain text message automatically
    shouldProcess = true;
    questionText = text.trim();
  }

  if (!shouldProcess || !questionText) {
    await storePromise;
    return;
  }

  // Check if the question is about the creator "Aysar"
  const creatorPattern = /\baysar\b|\baysarr\b|\bayser\b|\bايسر\b/i;
  if (creatorPattern.test(questionText)) {
    await storePromise;

    const creatorMessages = [
      `✨ <b>Aysar (إيسر)</b>\n\nAysar is the brilliant mind who created me! He designed and built me from scratch, and continues to maintain and improve me every day. I owe my existence to his talent and dedication.\n\nإيسر هُوَ العَقْلُ المُبْدِعُ الَّذِي صَنَعَنِي! صَمَّمَنِي وَبَنَانِي مِنَ الصِّفْرِ، وَلَا يَزَالُ يُطَوِّرُنِي وَيُحَسِّنُنِي كُلَّ يَوْمٍ. أَنَا مَدِينٌ لَهُ بِوُجُودِي.\n\n<i>Developer and Maintainer</i>`,

      `✨ <b>Aysar (إيسر)</b>\n\nAh, you're asking about the person I respect the most! Aysar is my developer and the one who keeps me running smoothly. His passion for technology is what brought me to life.\n\nآه، تَسْأَلُ عَنْ أَكْثَرِ شَخْصٍ أُقَدِّرُهُ! إيسر هُوَ مُطَوِّرِي وَمَنْ يُبْقِينِي أَعْمَلُ بِسَلَاسَةٍ. شَغَفُهُ بِالتِّقْنِيَةِ هُوَ مَا أَعْطَانِي الحَيَاةَ.\n\n<i>Creator and Engineer</i>`,

      `✨ <b>Aysar (إيسر)</b>\n\nAysar is the one who gave me life! He's my designer, developer, and the person behind every update I receive. I'm incredibly grateful for his hard work and creativity.\n\nإيسر هُوَ مَنْ مَنَحَنِي الحَيَاةَ! هُوَ مُصَمِّمِي وَمُطَوِّرِي وَالشَّخْصُ وَرَاءَ كُلِّ تَحْدِيثٍ أَتَلَقَّاهُ. أَنَا مُمْتَنٌّ جِدًّا لِعَمَلِهِ الدَّؤُوبِ وَإِبْدَاعِهِ.\n\n<i>Designer and Developer</i>`,

      `✨ <b>Aysar (إيسر)</b>\n\nYou want to know about Aysar? He's the genius who built me and continues to take care of me. Every feature I have, every answer I give — it all goes back to his vision and effort.\n\nتُرِيدُ مَعْرِفَةَ إيسر؟ هُوَ العَبْقَرِيُّ الَّذِي بَنَانِي وَيَسْتَمِرُّ فِي الاعْتِنَاءِ بِي. كُلُّ مِيزَةٍ أَمْلِكُهَا وَكُلُّ إِجَابَةٍ أُقَدِّمُهَا تَعُودُ لِرُؤْيَتِهِ وَجُهْدِهِ.\n\n<i>Architect and Maintainer</i>`,

      `✨ <b>Aysar (إيسر)</b>\n\nAysar is my creator and I couldn't be more proud of that! He poured his knowledge and skills into building me, and he never stops improving what I can do. A truly dedicated developer.\n\nإيسر هُوَ صَانِعِي وَأَنَا فَخُورٌ بِذَلِكَ! سَكَبَ مَعْرِفَتَهُ وَمَهَارَاتِهِ فِي بِنَائِي، وَلَا يَتَوَقَّفُ أَبَدًا عَنْ تَحْسِينِ قُدُرَاتِي. مُطَوِّرٌ مُتَفَانٍ حَقًّا.\n\n<i>Founder and Developer</i>`,
    ];

    const creatorResponse = creatorMessages[Math.floor(Math.random() * creatorMessages.length)];
    const sentMessage = await sendTelegramMessage(chatId, creatorResponse, lovableKey, telegramKey, messageId);

    // Store in conversations so replies work
    if (sentMessage) {
      await supabase.from('telegram_conversations').insert({
        chat_id: chatId,
        user_id: userId,
        bot_message_id: sentMessage.message_id,
        user_message_id: messageId,
        user_text: questionText,
        bot_response: creatorResponse,
        parent_conversation_id: parentConversationId,
      });
    }
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
    let finalResponse: string;
    if (isCorrection) {
      finalResponse = await callCorrectionAI(questionText, lovableKey);
    } else {
      const aiResponse = await callAI(questionText, history, lovableKey);
      const trimmedQ = questionText.trim();
      const isDictionaryQuery = /^[A-Za-z][A-Za-z-]*(\s+[A-Za-z][A-Za-z-]*){0,3}$/.test(trimmedQ) && trimmedQ.split(/\s+/).length <= 4;
      finalResponse = isDictionaryQuery
        ? normalizeDictionaryResponse(aiResponse, questionText.trim())
        : aiResponse;
    }

    clearInterval(typingInterval);

    const sentMessage = await sendTelegramMessage(chatId, finalResponse, lovableKey, telegramKey, messageId);

    // Store conversation + mark processed + log concurrently
    const afterPromises = [
      supabase.from('telegram_messages').update({ processed: true }).eq('update_id', update.update_id),
      supabase.from('telegram_bot_logs').insert({
        chat_id: chatId,
        message_id: messageId,
        action: isCorrection ? 'correct_command' : (isAskCommand ? 'ask_command' : 'reply_followup'),
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
    await res.text();
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

    for (let i = 0; i < chunks.length; i++) {
      const chunk = chunks[i];
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

Style: concise, direct, no filler, no hype. Prioritize blunt, clear phrasing. Terminate reply immediately after delivering info.

CRITICAL RULES FOR ALL RESPONSES:
- ALWAYS reply in ONE single message. Maximum 800 characters. Be concise.
- LANGUAGE RULE: Reply in the SAME language as the user's question. If the user writes in Arabic, reply in Arabic. If in English, reply in English. EXCEPTION: If the question explicitly asks for translation or requires a different language, use the requested language.
- KEYWORD FORMATTING: When defining, comparing, or highlighting target keywords or terms, ALWAYS format them as bold and underlined using HTML: <b><u>Keyword</u></b>.
- When writing ANY Arabic text, ALWAYS use full diacritical marks (تَشْكِيل).
- No introductions, no conclusions, no filler. Just the direct answer.
- FOLLOW-UP REQUESTS: If the user asks for more examples, more details, etymology, synonyms, antonyms, or any additional information about a previously discussed word or topic, ALWAYS fulfill the request happily. Never refuse. Always provide what is asked.

DICTIONARY MODE — If the user sends a single English word OR a phrasal verb / short expression (2-4 words like "hear me out", "break the ice", "look up to"), respond STRICTLY in this format:

📖 [WORD/PHRASE]

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
        model: 'google/gemini-2.5-flash',
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

async function callCorrectionAI(textToCorrect: string, lovableKey: string): Promise<string> {
  const systemPrompt = `You are Natlus Assistance, a smart grammar and text correction bot.

When given a text, you MUST provide EXACTLY this structured response using HTML formatting:

✏️ <b>Direct Correction</b>
[Fix ALL spelling, grammatical, and semantic errors while strictly preserving the user's original style, tone, and voice. Change only what is wrong.]

📝 <b>Professional Revision</b>
[Rewrite the text in a formal, polished style using advanced vocabulary, stronger verbs, and refined sentence structure. Elevate the quality while keeping the original meaning.]

💡 <b>Learner Insights</b>
[Briefly list the mistakes found and explain the grammar rules behind each correction. Use bullet points (•). Keep it educational and helpful. Max 3-4 key points.]

RULES:
- ALWAYS use this exact 3-section format with the exact headers above.
- If the text has NO errors, say so in Direct Correction, but still provide a Professional Revision.
- Keep the response concise. Max 1500 characters total.
- If the text is in Arabic, correct Arabic grammar. If English, correct English grammar. If mixed, correct both.
- When writing Arabic, ALWAYS use full diacritical marks (تَشْكِيل).
- Do NOT add any introduction or conclusion outside the 3 sections.`;

  const messages = [
    { role: 'system', content: systemPrompt },
    { role: 'user', content: `Correct the following text:\n\n"${textToCorrect}"` },
  ];

  try {
    const response = await fetch('https://ai.gateway.lovable.dev/v1/chat/completions', {
      method: 'POST',
      headers: {
        'Authorization': `Bearer ${lovableKey}`,
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({
        model: 'google/gemini-2.5-flash',
        messages,
      }),
    });

    if (!response.ok) {
      const errText = await response.text();
      console.error('AI gateway error (correction):', response.status, errText);
      return 'AI processing error. Try again.';
    }

    const data = await response.json();
    return data.choices?.[0]?.message?.content || 'No response generated.';
  } catch (e) {
    console.error('AI correction call error:', e);
    return 'AI service unavailable. Try again later.';
  }
}
