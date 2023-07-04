CREATE TABLE IF NOT EXISTS transaction_enrichments (
    user_id uuid,
    account_id uuid,
    date text,
    id text,
    enrichment_category text,
    enrichment_merchant_name text,
    enrichment_cycle_id uuid,
    enrichment_labels frozen<set<text>>,
    PRIMARY KEY (user_id, account_id, date, id)
) WITH CLUSTERING ORDER BY (account_id ASC, date DESC, id ASC);