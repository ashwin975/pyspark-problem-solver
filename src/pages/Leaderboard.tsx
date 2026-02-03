import { useEffect, useState } from 'react';
import { Link } from 'react-router-dom';
import Header from '@/components/Header';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from '@/components/ui/table';
import { Avatar, AvatarFallback, AvatarImage } from '@/components/ui/avatar';
import { Badge } from '@/components/ui/badge';
import { Skeleton } from '@/components/ui/skeleton';
import { Trophy, Medal, Award, Sparkles } from 'lucide-react';
import { supabase } from '@/integrations/supabase/client';
import { useAuth } from '@/contexts/AuthContext';

interface LeaderboardEntry {
  id: string;
  username: string;
  display_name: string | null;
  avatar_url: string | null;
  score: number;
  problems_solved: number;
}

const Leaderboard = () => {
  const { user } = useAuth();
  const [entries, setEntries] = useState<LeaderboardEntry[]>([]);
  const [loading, setLoading] = useState(true);
  const [userRank, setUserRank] = useState<number | null>(null);

  useEffect(() => {
    fetchLeaderboard();
  }, [user]);

  const fetchLeaderboard = async () => {
    try {
      const { data, error } = await supabase
        .from('profiles')
        .select('id, username, display_name, avatar_url, score, problems_solved')
        .order('score', { ascending: false })
        .limit(100);

      if (error) throw error;

      setEntries(data || []);

      // Find current user's rank
      if (user && data) {
        const rank = data.findIndex((entry) => entry.id === user.id);
        setUserRank(rank !== -1 ? rank + 1 : null);
      }
    } catch (error) {
      console.error('Error fetching leaderboard:', error);
    } finally {
      setLoading(false);
    }
  };

  const getRankIcon = (rank: number) => {
    switch (rank) {
      case 1:
        return <Trophy className="h-5 w-5 text-yellow-500" />;
      case 2:
        return <Medal className="h-5 w-5 text-gray-400" />;
      case 3:
        return <Award className="h-5 w-5 text-amber-600" />;
      default:
        return <span className="text-muted-foreground font-medium">#{rank}</span>;
    }
  };

  const getRankBadge = (rank: number) => {
    if (rank === 1) return 'bg-yellow-500/10 text-yellow-500 border-yellow-500/30';
    if (rank === 2) return 'bg-gray-400/10 text-gray-400 border-gray-400/30';
    if (rank === 3) return 'bg-amber-600/10 text-amber-600 border-amber-600/30';
    return '';
  };

  return (
    <div className="min-h-screen bg-background">
      <Header />

      <div className="container py-8">
        <div className="mb-8 text-center">
          <h1 className="text-3xl font-bold mb-2">Leaderboard</h1>
          <p className="text-muted-foreground">
            Top PySpark problem solvers ranked by score
          </p>
        </div>

        {user && userRank && (
          <Card className="mb-6 border-primary/30 bg-primary/5">
            <CardContent className="py-4">
              <div className="flex items-center justify-between">
                <div className="flex items-center gap-3">
                  <Sparkles className="h-5 w-5 text-primary" />
                  <span className="font-medium">Your Rank</span>
                </div>
                <Badge variant="outline" className="text-lg px-4 py-1">
                  #{userRank}
                </Badge>
              </div>
            </CardContent>
          </Card>
        )}

        <Card>
          <CardHeader>
            <CardTitle className="flex items-center gap-2">
              <Trophy className="h-5 w-5 text-primary" />
              Top 100 Solvers
            </CardTitle>
          </CardHeader>
          <CardContent>
            {loading ? (
              <div className="space-y-4">
                {[...Array(10)].map((_, i) => (
                  <div key={i} className="flex items-center gap-4">
                    <Skeleton className="h-10 w-10 rounded-full" />
                    <Skeleton className="h-4 w-32" />
                    <Skeleton className="h-4 w-16 ml-auto" />
                  </div>
                ))}
              </div>
            ) : entries.length === 0 ? (
              <div className="text-center py-8 text-muted-foreground">
                <p>No entries yet. Be the first to solve a problem!</p>
                <Link to="/problems" className="text-primary hover:underline mt-2 inline-block">
                  Start solving →
                </Link>
              </div>
            ) : (
              <Table>
                <TableHeader>
                  <TableRow>
                    <TableHead className="w-16">Rank</TableHead>
                    <TableHead>User</TableHead>
                    <TableHead className="text-right">Problems</TableHead>
                    <TableHead className="text-right">Score</TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {entries.map((entry, index) => {
                    const rank = index + 1;
                    const isCurrentUser = user?.id === entry.id;
                    
                    return (
                      <TableRow 
                        key={entry.id}
                        className={isCurrentUser ? 'bg-primary/5' : ''}
                      >
                        <TableCell>
                          <div className={`flex items-center justify-center w-8 h-8 rounded-full ${getRankBadge(rank)}`}>
                            {getRankIcon(rank)}
                          </div>
                        </TableCell>
                        <TableCell>
                          <div className="flex items-center gap-3">
                            <Avatar className="h-8 w-8">
                              <AvatarImage src={entry.avatar_url || undefined} />
                              <AvatarFallback>
                                {(entry.display_name || entry.username)?.slice(0, 2).toUpperCase() || '??'}
                              </AvatarFallback>
                            </Avatar>
                            <span className={`font-medium ${isCurrentUser ? 'text-primary' : ''}`}>
                              {entry.display_name || entry.username}
                              {isCurrentUser && <span className="text-xs text-muted-foreground ml-2">(you)</span>}
                            </span>
                          </div>
                        </TableCell>
                        <TableCell className="text-right">
                          <Badge variant="secondary">{entry.problems_solved}</Badge>
                        </TableCell>
                        <TableCell className="text-right font-bold text-primary">
                          {entry.score.toLocaleString()}
                        </TableCell>
                      </TableRow>
                    );
                  })}
                </TableBody>
              </Table>
            )}
          </CardContent>
        </Card>
      </div>
    </div>
  );
};

export default Leaderboard;
