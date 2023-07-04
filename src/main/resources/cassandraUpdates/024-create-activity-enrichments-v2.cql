CREATE TABLE IF NOT EXISTS activity_enrichments_v2 (
    activity_id uuid,
    received_at timestamp,
    enrichment_type text,
    time_zone text,
    user_id uuid,
    account_id_to_first_change frozen<map<uuid, text>>,
    checksum bigint,
    PRIMARY KEY (activity_id, received_at, enrichment_type)
) WITH CLUSTERING ORDER BY (received_at DESC, enrichment_type ASC) AND default_time_to_live = 3600;
