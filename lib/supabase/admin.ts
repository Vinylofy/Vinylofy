import { createClient } from "@supabase/supabase-js";

export function createSupabaseAdminClient() {
  const url =
    process.env.NEXT_PUBLIC_SUPABASE_URL?.trim();

  const adminKey =
    process.env.SUPABASE_SECRET_KEY?.trim() ||
    process.env.SUPABASE_SERVICE_ROLE_KEY?.trim();

  if (!url || !adminKey) {
    throw new Error(
      "Supabase admin environment variables are missing.",
    );
  }

  return createClient(url, adminKey, {
    auth: {
      persistSession: false,
      autoRefreshToken: false,
    },
  });
}
