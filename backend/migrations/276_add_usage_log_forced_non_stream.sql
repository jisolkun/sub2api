-- 「过载后强制非流式」改写标记：该请求的上游 body 被规则改写为 stream=false
--（客户端协议保持流式），用于使用记录展示「流式(非流)」。
ALTER TABLE usage_logs ADD COLUMN IF NOT EXISTS forced_non_stream boolean NOT NULL DEFAULT false;
