
-- Drop overly permissive policies and restrict to service_role only
DROP POLICY "Service role access" ON public.telegram_bot_state;
DROP POLICY "Service role access" ON public.telegram_messages;
DROP POLICY "Service role access" ON public.telegram_conversations;
DROP POLICY "Service role access" ON public.telegram_bot_logs;

-- These tables are only accessed via edge functions using service_role key
-- No anon/authenticated access needed
CREATE POLICY "Service role only" ON public.telegram_bot_state FOR ALL TO service_role USING (true) WITH CHECK (true);
CREATE POLICY "Service role only" ON public.telegram_messages FOR ALL TO service_role USING (true) WITH CHECK (true);
CREATE POLICY "Service role only" ON public.telegram_conversations FOR ALL TO service_role USING (true) WITH CHECK (true);
CREATE POLICY "Service role only" ON public.telegram_bot_logs FOR ALL TO service_role USING (true) WITH CHECK (true);
