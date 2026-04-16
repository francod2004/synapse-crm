-- v6 Cold Email Agent (Loom Pivot) schema migration
-- Run in Supabase SQL editor. Each statement is idempotent.

-- 1. New prospect columns for follow-up tracking + Loom link
ALTER TABLE prospects ADD COLUMN IF NOT EXISTS touch_count integer DEFAULT 0;
ALTER TABLE prospects ADD COLUMN IF NOT EXISTS last_touch_at timestamptz;
ALTER TABLE prospects ADD COLUMN IF NOT EXISTS loom_link text;

-- 2. DB trigger that bumps touch_count + stamps last_touch_at whenever an
--    agent_queue entry transitions to status='sent'. Keeps the three writes
--    (status flip + touch_count + last_touch_at) inside a single Postgres
--    transaction so they can't drift. Required by the v6 follow-up clock.
CREATE OR REPLACE FUNCTION bump_touch_on_send()
RETURNS TRIGGER AS $$
BEGIN
  IF NEW.status = 'sent'
     AND OLD.status IS DISTINCT FROM 'sent'
     AND NEW.prospect_id IS NOT NULL THEN
    UPDATE prospects
       SET last_touch_at = now(),
           touch_count   = COALESCE(touch_count, 0) + 1
     WHERE id = NEW.prospect_id;
  END IF;
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS agent_queue_bump_touch ON agent_queue;
CREATE TRIGGER agent_queue_bump_touch
AFTER UPDATE OF status ON agent_queue
FOR EACH ROW
EXECUTE FUNCTION bump_touch_on_send();

-- 3. Verification -- all three columns should return
SELECT column_name, data_type, column_default
FROM information_schema.columns
WHERE table_name = 'prospects'
  AND column_name IN ('touch_count', 'last_touch_at', 'loom_link')
ORDER BY column_name;

-- 4. Verify the trigger is installed
SELECT trigger_name, event_manipulation, action_timing, action_statement
FROM information_schema.triggers
WHERE trigger_name = 'agent_queue_bump_touch';
