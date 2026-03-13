
-- Singleton table for polling offset
CREATE TABLE public.telegram_bot_state (
  id int PRIMARY KEY CHECK (id = 1),
  update_offset bigint NOT NULL DEFAULT 0,
  updated_at timestamptz NOT NULL DEFAULT now()
);

INSERT INTO telegram_bot_state (id, update_offset) VALUES (1, 0);

-- Incoming messages table
CREATE TABLE public.telegram_messages (
  id UUID NOT NULL DEFAULT gen_random_uuid() PRIMARY KEY,
  update_id bigint UNIQUE NOT NULL,
  chat_id bigint NOT NULL,
  message_id bigint,
  user_id bigint,
  username text,
  text text,
  reply_to_message_id bigint,
  is_bot_command boolean DEFAULT false,
  raw_update jsonb NOT NULL,
  processed boolean DEFAULT false,
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX idx_telegram_messages_chat_id ON telegram_messages (chat_id);
CREATE INDEX idx_telegram_messages_processed ON telegram_messages (processed);

-- Conversation memory for context tracking
CREATE TABLE public.telegram_conversations (
  id UUID NOT NULL DEFAULT gen_random_uuid() PRIMARY KEY,
  chat_id bigint NOT NULL,
  user_id bigint NOT NULL,
  bot_message_id bigint,
  user_message_id bigint,
  user_text text,
  bot_response text,
  parent_conversation_id UUID REFERENCES telegram_conversations(id),
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX idx_telegram_conversations_bot_msg ON telegram_conversations (chat_id, bot_message_id);
CREATE INDEX idx_telegram_conversations_user ON telegram_conversations (chat_id, user_id);

-- Bot response log
CREATE TABLE public.telegram_bot_logs (
  id UUID NOT NULL DEFAULT gen_random_uuid() PRIMARY KEY,
  chat_id bigint NOT NULL,
  message_id bigint,
  action text NOT NULL,
  details jsonb,
  created_at timestamptz NOT NULL DEFAULT now()
);

-- Enable RLS on all tables
ALTER TABLE public.telegram_bot_state ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.telegram_messages ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.telegram_conversations ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.telegram_bot_logs ENABLE ROW LEVEL SECURITY;

-- Service role only policies (these tables are accessed by edge functions only)
CREATE POLICY "Service role access" ON public.telegram_bot_state FOR ALL USING (true) WITH CHECK (true);
CREATE POLICY "Service role access" ON public.telegram_messages FOR ALL USING (true) WITH CHECK (true);
CREATE POLICY "Service role access" ON public.telegram_conversations FOR ALL USING (true) WITH CHECK (true);
CREATE POLICY "Service role access" ON public.telegram_bot_logs FOR ALL USING (true) WITH CHECK (true);
