CREATE TABLE IF NOT EXISTS activity_enrichments_initiation (
    minute_received int,
    activity_id uuid,
    received_at timestamp,
    user_id uuid,
    PRIMARY KEY (minute_received, activity_id)
) WITH default_time_to_live = 3600;
