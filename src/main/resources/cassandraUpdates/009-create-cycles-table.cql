CREATE TABLE IF NOT EXISTS transaction_cycles (
    cycle_id uuid,
    cycle_type text,
    amount decimal,
    currency text,
    period text,
    model_amount decimal,
    model_currency text,
    model_period text,
    predicted_occurrences set<date>,
    label text,
    subscription boolean,
    counterparty text,
    PRIMARY KEY (cycle_id)
);