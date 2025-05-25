ALTER TABLE memo ADD COLUMN agent_task_id TEXT;
ALTER TABLE memo ADD COLUMN agent_status INTEGER;
ALTER TABLE memo ADD COLUMN agent_query_text TEXT;
ALTER TABLE memo ADD COLUMN agent_plan_json TEXT;
ALTER TABLE memo ADD COLUMN agent_steps_json TEXT;
ALTER TABLE memo ADD COLUMN agent_result_json TEXT;
ALTER TABLE memo ADD COLUMN agent_error_message TEXT;

-- Index for faster lookup by agent_task_id
CREATE INDEX IF NOT EXISTS idx_memo_agent_task_id ON memo (agent_task_id); 