import { createClient } from '@supabase/supabase-js';

const SUPABASE_URL = "https://nkcxxbmnflfxjvfrhpwt.supabase.co";
const SUPABASE_ANON_KEY = "sb_publishable_I1VJTvbDPo2E4eRBLlOhWQ_kRKpXtEi";

export const supabase = createClient(SUPABASE_URL, SUPABASE_ANON_KEY, {
  auth: {
    storage: typeof window !== 'undefined' ? window.localStorage : undefined,
    persistSession: true,
    autoRefreshToken: true,
  },
});
