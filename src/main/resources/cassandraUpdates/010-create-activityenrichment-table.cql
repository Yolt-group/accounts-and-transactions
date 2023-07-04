CREATE TABLE IF NOT EXISTS activity_enrichments (
    activity_id uuid,
    enrichment_type text,
    PRIMARY KEY (activity_id, enrichment_type)
) WITH CLUSTERING ORDER BY (enrichment_type ASC) AND default_time_to_live = 3600;
