CREATE TABLE IF NOT EXISTS transaction_cycles_v2 (
    user_id uuid,
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
    PRIMARY KEY (user_id, cycle_id)
);