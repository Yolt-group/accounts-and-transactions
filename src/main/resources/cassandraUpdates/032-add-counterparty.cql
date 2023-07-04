ALTER TABLE transaction_enrichments
    ADD enrichment_counterparty_name text;
ALTER TABLE transaction_enrichments
    ADD enrichment_counterparty_known_merchant boolean;