ALTER TABLE memo ADD COLUMN agent_task_id TEXT;
ALTER TABLE memo ADD COLUMN agent_status INTEGER;
ALTER TABLE memo ADD COLUMN agent_query_text TEXT;
ALTER TABLE memo ADD COLUMN agent_plan_json TEXT;
ALTER TABLE memo ADD COLUMN agent_steps_json TEXT;
ALTER TABLE memo ADD COLUMN agent_result_json TEXT;
ALTER TABLE memo ADD COLUMN agent_error_message TEXT; 