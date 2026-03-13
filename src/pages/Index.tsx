import { useEffect, useState } from "react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Bot, MessageCircle, Users, Activity, Zap, Clock } from "lucide-react";
import { supabase } from "@/integrations/supabase/client";

interface ConversationLog {
  id: string;
  chat_id: number;
  user_text: string;
  bot_response: string;
  created_at: string;
}

interface BotLog {
  id: string;
  action: string;
  details: any;
  created_at: string;
}

const Index = () => {
  const [conversations, setConversations] = useState<ConversationLog[]>([]);
  const [logs, setLogs] = useState<BotLog[]>([]);
  const [stats, setStats] = useState({ totalMessages: 0, totalConversations: 0, uniqueChats: 0 });

  useEffect(() => {
    fetchData();
    const interval = setInterval(fetchData, 15000);
    return () => clearInterval(interval);
  }, []);

  const fetchData = async () => {
    const [convRes, logRes, msgCountRes] = await Promise.all([
      supabase.from("telegram_conversations").select("*").order("created_at", { ascending: false }).limit(20),
      supabase.from("telegram_bot_logs").select("*").order("created_at", { ascending: false }).limit(20),
      supabase.from("telegram_messages").select("chat_id", { count: "exact" }),
    ]);

    if (convRes.data) setConversations(convRes.data as any);
    if (logRes.data) setLogs(logRes.data as any);

    const uniqueChats = new Set(msgCountRes.data?.map((m: any) => m.chat_id) || []).size;
    setStats({
      totalMessages: msgCountRes.count || 0,
      totalConversations: convRes.data?.length || 0,
      uniqueChats,
    });
  };

  const timeAgo = (date: string) => {
    const diff = Date.now() - new Date(date).getTime();
    const mins = Math.floor(diff / 60000);
    if (mins < 1) return "just now";
    if (mins < 60) return `${mins}m ago`;
    const hours = Math.floor(mins / 60);
    if (hours < 24) return `${hours}h ago`;
    return `${Math.floor(hours / 24)}d ago`;
  };

  return (
    <div className="min-h-screen bg-background">
      {/* Header */}
      <header className="border-b border-border bg-card">
        <div className="container mx-auto px-4 py-6">
          <div className="flex items-center gap-3">
            <div className="flex h-12 w-12 items-center justify-center rounded-xl bg-primary">
              <Bot className="h-7 w-7 text-primary-foreground" />
            </div>
            <div>
              <h1 className="text-2xl font-bold text-foreground">Natlus Assistance</h1>
              <p className="text-sm text-muted-foreground">Telegram Bot Dashboard</p>
            </div>
            <Badge className="ml-auto bg-success text-success-foreground border-0">
              <Activity className="mr-1 h-3 w-3" /> Live
            </Badge>
          </div>
        </div>
      </header>

      <main className="container mx-auto px-4 py-8 space-y-8">
        {/* Stats */}
        <div className="grid grid-cols-1 sm:grid-cols-3 gap-4">
          <Card>
            <CardContent className="flex items-center gap-4 p-6">
              <div className="flex h-10 w-10 items-center justify-center rounded-lg bg-accent">
                <MessageCircle className="h-5 w-5 text-accent-foreground" />
              </div>
              <div>
                <p className="text-2xl font-bold text-foreground">{stats.totalMessages}</p>
                <p className="text-sm text-muted-foreground">Total Messages</p>
              </div>
            </CardContent>
          </Card>
          <Card>
            <CardContent className="flex items-center gap-4 p-6">
              <div className="flex h-10 w-10 items-center justify-center rounded-lg bg-accent">
                <Zap className="h-5 w-5 text-accent-foreground" />
              </div>
              <div>
                <p className="text-2xl font-bold text-foreground">{stats.totalConversations}</p>
                <p className="text-sm text-muted-foreground">AI Responses</p>
              </div>
            </CardContent>
          </Card>
          <Card>
            <CardContent className="flex items-center gap-4 p-6">
              <div className="flex h-10 w-10 items-center justify-center rounded-lg bg-accent">
                <Users className="h-5 w-5 text-accent-foreground" />
              </div>
              <div>
                <p className="text-2xl font-bold text-foreground">{stats.uniqueChats}</p>
                <p className="text-sm text-muted-foreground">Active Chats</p>
              </div>
            </CardContent>
          </Card>
        </div>

        {/* Recent Conversations */}
        <Card>
          <CardHeader>
            <CardTitle className="flex items-center gap-2 text-lg">
              <MessageCircle className="h-5 w-5 text-primary" />
              Recent Conversations
            </CardTitle>
          </CardHeader>
          <CardContent className="space-y-4">
            {conversations.length === 0 ? (
              <p className="text-center text-muted-foreground py-8">
                No conversations yet. Send <code className="bg-muted px-2 py-1 rounded text-sm">/ask</code> in your Telegram group to get started.
              </p>
            ) : (
              conversations.map((conv) => (
                <div key={conv.id} className="rounded-lg border border-border p-4 space-y-2">
                  <div className="flex items-center justify-between">
                    <Badge variant="outline" className="text-xs">Chat {conv.chat_id}</Badge>
                    <span className="flex items-center gap-1 text-xs text-muted-foreground">
                      <Clock className="h-3 w-3" />
                      {timeAgo(conv.created_at)}
                    </span>
                  </div>
                  <div className="space-y-1">
                    <p className="text-sm font-medium text-foreground">Q: {conv.user_text}</p>
                    <p className="text-sm text-muted-foreground line-clamp-2">A: {conv.bot_response}</p>
                  </div>
                </div>
              ))
            )}
          </CardContent>
        </Card>

        {/* Activity Log */}
        <Card>
          <CardHeader>
            <CardTitle className="flex items-center gap-2 text-lg">
              <Activity className="h-5 w-5 text-primary" />
              Activity Log
            </CardTitle>
          </CardHeader>
          <CardContent>
            {logs.length === 0 ? (
              <p className="text-center text-muted-foreground py-8">No activity yet.</p>
            ) : (
              <div className="space-y-2">
                {logs.map((log) => (
                  <div key={log.id} className="flex items-center justify-between rounded-md bg-muted/50 px-4 py-2">
                    <div className="flex items-center gap-2">
                      <Badge variant={log.action === "ask_command" ? "default" : "secondary"} className="text-xs">
                        {log.action === "ask_command" ? "/ask" : "reply"}
                      </Badge>
                      <span className="text-sm text-foreground">
                        {log.details?.username || "User"} — {log.details?.question?.slice(0, 60) || "..."}
                      </span>
                    </div>
                    <span className="text-xs text-muted-foreground">{timeAgo(log.created_at)}</span>
                  </div>
                ))}
              </div>
            )}
          </CardContent>
        </Card>
      </main>
    </div>
  );
};

export default Index;
