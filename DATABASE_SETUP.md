# PySpark Learnables Database Setup

Run the following SQL in your Supabase SQL Editor (Dashboard → SQL Editor → New query):

## 1. Create Profiles Table

```sql
-- Create profiles table for user data
CREATE TABLE public.profiles (
  id UUID PRIMARY KEY REFERENCES auth.users(id) ON DELETE CASCADE,
  username TEXT NOT NULL UNIQUE,
  display_name TEXT,
  birthday DATE,
  theme_preference TEXT DEFAULT 'system',
  avatar_url TEXT,
  score INTEGER DEFAULT 0 NOT NULL,
  problems_solved INTEGER DEFAULT 0 NOT NULL,
  created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW() NOT NULL,
  updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW() NOT NULL
);

-- Enable RLS
ALTER TABLE public.profiles ENABLE ROW LEVEL SECURITY;

-- Policies for profiles
CREATE POLICY "Public profiles are viewable by everyone"
  ON public.profiles FOR SELECT
  USING (true);

CREATE POLICY "Users can insert their own profile"
  ON public.profiles FOR INSERT
  WITH CHECK (auth.uid() = id);

CREATE POLICY "Users can update their own profile"
  ON public.profiles FOR UPDATE
  USING (auth.uid() = id);
```

## 2. Create Solved Problems Table

```sql
-- Create solved_problems table to track user progress
CREATE TABLE public.solved_problems (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  user_id UUID NOT NULL REFERENCES auth.users(id) ON DELETE CASCADE,
  problem_id TEXT NOT NULL,
  score INTEGER NOT NULL,
  solved_at TIMESTAMP WITH TIME ZONE DEFAULT NOW() NOT NULL,
  UNIQUE(user_id, problem_id)
);

-- Enable RLS
ALTER TABLE public.solved_problems ENABLE ROW LEVEL SECURITY;

-- Policies for solved_problems
CREATE POLICY "Users can view their own solved problems"
  ON public.solved_problems FOR SELECT
  USING (auth.uid() = user_id);

CREATE POLICY "Users can insert their own solved problems"
  ON public.solved_problems FOR INSERT
  WITH CHECK (auth.uid() = user_id);
```

## 3. Create Updated At Trigger

```sql
-- Function to automatically update updated_at
CREATE OR REPLACE FUNCTION public.handle_updated_at()
RETURNS TRIGGER AS $$
BEGIN
  NEW.updated_at = NOW();
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Trigger for profiles
CREATE TRIGGER on_profiles_updated
  BEFORE UPDATE ON public.profiles
  FOR EACH ROW
  EXECUTE FUNCTION public.handle_updated_at();
```

## 4. Add New Columns (If profiles table already exists)

If you already have the profiles table from a previous setup, run this to add the new columns:

```sql
-- Add new settings columns to existing profiles table
ALTER TABLE public.profiles 
ADD COLUMN IF NOT EXISTS display_name TEXT,
ADD COLUMN IF NOT EXISTS birthday DATE,
ADD COLUMN IF NOT EXISTS theme_preference TEXT DEFAULT 'system';
```

## 5. Configure Authentication

1. Go to **Authentication → Settings** in your Supabase dashboard
2. Under **Email**, you may want to disable "Confirm email" for easier testing
3. Under **URL Configuration**, set:
   - **Site URL**: Your app's preview URL (e.g., `https://your-preview-url.lovable.app`)
   - **Redirect URLs**: Add both your preview URL and any custom domains

## That's it!

After running these SQL commands, your PySpark Learnables app will:
- ✅ Allow users to sign up and sign in
- ✅ Track solved problems and scores
- ✅ Display a live leaderboard
- ✅ Support user settings (display name, birthday, theme preference)
