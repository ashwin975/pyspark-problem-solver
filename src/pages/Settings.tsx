import { useState, useEffect } from 'react';
import { useNavigate } from 'react-router-dom';
import { useAuth } from '@/contexts/AuthContext';
import { useTheme } from '@/hooks/useTheme';
import { supabase } from '@/integrations/supabase/client';
import Header from '@/components/Header';
import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';
import { Label } from '@/components/ui/label';
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card';
import { RadioGroup, RadioGroupItem } from '@/components/ui/radio-group';
import { Alert, AlertDescription } from '@/components/ui/alert';
import { Loader2, Save, User, Palette, Calendar } from 'lucide-react';
import { toast } from 'sonner';

interface UserSettings {
  display_name: string;
  birthday: string;
  theme_preference: 'dark' | 'light' | 'system';
}

const Settings = () => {
  const navigate = useNavigate();
  const { user, loading: authLoading } = useAuth();
  const { theme, setTheme } = useTheme();
  
  const [settings, setSettings] = useState<UserSettings>({
    display_name: '',
    birthday: '',
    theme_preference: 'system',
  });
  const [loading, setLoading] = useState(true);
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    if (!authLoading && !user) {
      navigate('/auth');
    }
  }, [user, authLoading, navigate]);

  useEffect(() => {
    if (user) {
      fetchSettings();
    }
  }, [user]);

  const fetchSettings = async () => {
    if (!user) return;

    try {
      const { data, error } = await supabase
        .from('profiles')
        .select('display_name, birthday, theme_preference')
        .eq('id', user.id)
        .single();

      if (error) throw error;

      if (data) {
        setSettings({
          display_name: data.display_name || '',
          birthday: data.birthday || '',
          theme_preference: (data.theme_preference as UserSettings['theme_preference']) || 'system',
        });
        
        // Apply saved theme
        if (data.theme_preference) {
          setTheme(data.theme_preference as 'dark' | 'light' | 'system');
        }
      }
    } catch (err) {
      console.error('Error fetching settings:', err);
    } finally {
      setLoading(false);
    }
  };

  const handleSave = async () => {
    if (!user) return;

    setError(null);
    setSaving(true);

    try {
      const { error } = await supabase
        .from('profiles')
        .update({
          display_name: settings.display_name || null,
          birthday: settings.birthday || null,
          theme_preference: settings.theme_preference,
        })
        .eq('id', user.id);

      if (error) throw error;

      // Apply theme immediately
      setTheme(settings.theme_preference);
      
      toast.success('Settings saved successfully!');
    } catch (err: any) {
      console.error('Error saving settings:', err);
      setError(err.message || 'Failed to save settings');
    } finally {
      setSaving(false);
    }
  };

  const handleThemeChange = (value: string) => {
    const themeValue = value as 'dark' | 'light' | 'system';
    setSettings(prev => ({ ...prev, theme_preference: themeValue }));
    // Preview theme immediately
    setTheme(themeValue);
  };

  if (authLoading || loading) {
    return (
      <div className="min-h-screen bg-background">
        <Header />
        <div className="flex items-center justify-center py-20">
          <Loader2 className="h-8 w-8 animate-spin text-primary" />
        </div>
      </div>
    );
  }

  return (
    <div className="min-h-screen bg-background">
      <Header />

      <div className="container py-8 max-w-2xl">
        <div className="mb-8">
          <h1 className="text-3xl font-bold mb-2">Settings</h1>
          <p className="text-muted-foreground">
            Manage your profile and preferences
          </p>
        </div>

        <div className="space-y-6">
          {/* Profile Settings */}
          <Card>
            <CardHeader>
              <CardTitle className="flex items-center gap-2">
                <User className="h-5 w-5 text-primary" />
                Profile
              </CardTitle>
              <CardDescription>
                Your public profile information
              </CardDescription>
            </CardHeader>
            <CardContent className="space-y-4">
              <div className="space-y-2">
                <Label htmlFor="display_name">Display Name</Label>
                <Input
                  id="display_name"
                  placeholder="Enter your display name"
                  value={settings.display_name}
                  onChange={(e) => setSettings(prev => ({ ...prev, display_name: e.target.value }))}
                  maxLength={50}
                />
                <p className="text-xs text-muted-foreground">
                  This name will be shown on the leaderboard instead of your username
                </p>
              </div>

              <div className="space-y-2">
                <Label htmlFor="birthday" className="flex items-center gap-2">
                  <Calendar className="h-4 w-4" />
                  Birthday
                </Label>
                <Input
                  id="birthday"
                  type="date"
                  value={settings.birthday}
                  onChange={(e) => setSettings(prev => ({ ...prev, birthday: e.target.value }))}
                />
              </div>
            </CardContent>
          </Card>

          {/* Theme Settings */}
          <Card>
            <CardHeader>
              <CardTitle className="flex items-center gap-2">
                <Palette className="h-5 w-5 text-primary" />
                Appearance
              </CardTitle>
              <CardDescription>
                Customize how PySpark Katas looks for you
              </CardDescription>
            </CardHeader>
            <CardContent>
              <RadioGroup
                value={settings.theme_preference}
                onValueChange={handleThemeChange}
                className="grid gap-4"
              >
                <div className="flex items-center space-x-3 rounded-lg border border-border p-4 cursor-pointer hover:bg-secondary/50 transition-colors">
                  <RadioGroupItem value="light" id="light" />
                  <Label htmlFor="light" className="flex-1 cursor-pointer">
                    <div className="font-medium">Light</div>
                    <div className="text-sm text-muted-foreground">
                      A bright theme for daytime use
                    </div>
                  </Label>
                </div>
                <div className="flex items-center space-x-3 rounded-lg border border-border p-4 cursor-pointer hover:bg-secondary/50 transition-colors">
                  <RadioGroupItem value="dark" id="dark" />
                  <Label htmlFor="dark" className="flex-1 cursor-pointer">
                    <div className="font-medium">Dark</div>
                    <div className="text-sm text-muted-foreground">
                      Easy on the eyes, perfect for coding
                    </div>
                  </Label>
                </div>
                <div className="flex items-center space-x-3 rounded-lg border border-border p-4 cursor-pointer hover:bg-secondary/50 transition-colors">
                  <RadioGroupItem value="system" id="system" />
                  <Label htmlFor="system" className="flex-1 cursor-pointer">
                    <div className="font-medium">System</div>
                    <div className="text-sm text-muted-foreground">
                      Automatically match your device settings
                    </div>
                  </Label>
                </div>
              </RadioGroup>
            </CardContent>
          </Card>

          {error && (
            <Alert variant="destructive">
              <AlertDescription>{error}</AlertDescription>
            </Alert>
          )}

          {/* Save Button */}
          <Button onClick={handleSave} disabled={saving} className="w-full" size="lg">
            {saving ? (
              <>
                <Loader2 className="h-4 w-4 animate-spin mr-2" />
                Saving...
              </>
            ) : (
              <>
                <Save className="h-4 w-4 mr-2" />
                Save Settings
              </>
            )}
          </Button>
        </div>
      </div>
    </div>
  );
};

export default Settings;
