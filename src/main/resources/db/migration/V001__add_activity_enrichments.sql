CREATE TYPE activity_enrichment_type AS enum (
    'REFRESH',
    'FEEDBACK_CATEGORIES',
    'FEEDBACK_COUNTERPARTIES',
    'FEEDBACK_TRANSACTION_CYCLES'
);
CREATE CAST (VARCHAR AS activity_enrichment_type) WITH INOUT AS implicit;

CREATE TABLE IF NOT EXISTS activity_enrichments
(
    activity_id                 UUID                        PRIMARY KEY,
    started_at                  TIMESTAMP WITH TIME ZONE    NOT NULL,
    enrichment_type             activity_enrichment_type    NOT NULL,
    user_id                     UUID                        NOT NULL,
    checksum                    INTEGER                     NOT NULL
);

CREATE TABLE IF NOT EXISTS activity_enrichments_accounts
(
    activity_id                 UUID                        NOT NULL,
    account_id                  UUID                        NOT NULL,
    oldest_transaction_ts       TIMESTAMP WITH TIME ZONE    NOT NULL,
    PRIMARY KEY (activity_id, account_id),
    foreign key (activity_id) references activity_enrichments (activity_id) ON DELETE CASCADE
);
