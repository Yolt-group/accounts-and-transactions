CREATE TABLE IF NOT EXISTS bulk_update_sessions (
    user_id uuid,
    update_session_id uuid,
    account_id uuid,
    date text,
    transaction_id text,
    details frozen<map<text,text>>,
    PRIMARY KEY (user_id, update_session_id)
) WITH CLUSTERING ORDER BY (update_session_id ASC) AND default_time_to_live = 900;
