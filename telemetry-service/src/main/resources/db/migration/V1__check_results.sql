CREATE EXTENSION IF NOT EXISTS timescaledb;
CREATE TABLE check_results (
 time timestamptz NOT NULL, monitor_id uuid NOT NULL, org_id uuid NOT NULL,
 region text NOT NULL, outcome text NOT NULL, latency_ms integer NOT NULL,
 http_status integer, error_code text, error_message text, checker_instance text,
 UNIQUE (monitor_id, region, time)
);
SELECT create_hypertable('check_results', by_range('time', INTERVAL '1 day'));
CREATE INDEX check_results_org_monitor_time_idx ON check_results(org_id, monitor_id, time DESC);
