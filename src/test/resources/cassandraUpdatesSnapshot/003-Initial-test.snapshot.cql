/*
NOTE!
This snapshot file is not an ordinary snapshot.
This application "needs" the presence of a datascience keyspace. It writes to it, but it does NOT own it. Therefore, there is no DDL in
production scripts.

This file is used to 'fake' the existence of the datascience keyspace with the associated table.
 */
CREATE keyspace IF NOT EXISTS datascience WITH REPLICATION = {'class':'SimpleStrategy', 'replication_factor':1} AND DURABLE_WRITES=false;
CREATE TABLE IF NOT EXISTS datascience.modelmutation(hcpk text, filename text, time timeuuid, script text, result text, user text, forced boolean, PRIMARY KEY (hcpk, filename, time));
CREATE TABLE IF NOT EXISTS datascience.transactions (
    user_id                             uuid,
    account_id                          uuid,
    transaction_id                      text,
    external_id                         text,
    date                                text,
    post_date                           text,
    transaction_date                    text,
    transaction_timestamp               timestamp,
    time_zone                           text,
    transaction_type                    text,
    amount                              decimal,
    currency                            text,
    description                         text,
    simple_description                  text,
    mapped_category                     text,
    external_category                   text,
    external_category_id                int,
    external_merchant                   text,
    internal_transaction                text,
    pending                             int,
    external_transaction_date           text,
    external_post_date                  text,
    note                                text,
    extended_transaction                blob,
    labels                              frozen<set<text>>,
    last_updated_time                   timestamp,
    bank_specific                       frozen<map<text, text>>,
    bank_counterparty_bban                     text,
    bank_counterparty_iban                     text,
    bank_counterparty_masked_pan               text,
    bank_counterparty_name                     text,
    bank_counterparty_pan                      text,
    bank_counterparty_sort_code_account_number text,
PRIMARY KEY (user_id, pending, account_id, date, transaction_id)
) WITH CLUSTERING ORDER BY (pending DESC, account_id DESC, date DESC, transaction_id DESC);


CREATE TABLE IF NOT EXISTS datascience.account_current (
    user_id                            uuid,
    account_id                         uuid,
    user_site_id                       uuid,
    site_id                            uuid,
    external_account_id                text,
    external_site_id                   text,
    name                               text,
    account_type                       text,
    currency_code                      text,
    current_balance                    decimal,
    available_balance                  decimal,
    last_updated_time                  timestamp,
    status                             text,
    status_detail                      text,
    provider                           text,
    hidden                             boolean,
    extended_account                   blob,
    linked_account                     text,
    bank_specific                      text,
    account_holder_name                text,
    account_scheme                     text,
    account_identification              text,
    account_secondary_identification    text,
    account_masked_identification       text,
    closed                             boolean,
PRIMARY KEY ((user_id), account_id)
);

CREATE TABLE IF NOT EXISTS datascience.creditcards_current (
    user_id                      uuid,
    account_id                   uuid,
    user_site_id                 uuid,
    site_id                      uuid,
    external_account_id          text,
    external_site_id             text,
    name                         text,
    currency_code                text,
    last_updated_time            timestamp,
    apr                          double,
    cash_apr                     double,
    as_of_date                   text,
    due_amount                   decimal,
    due_date                     text,
    available_credit_amount      decimal,
    running_balance_amount       decimal,
    min_payment_amount           decimal,
    new_charges_amount           decimal,
    last_payment_amount          decimal,
    last_payment_date            text,
    total_credit_line_amount     decimal,
    cash_limit_amount            decimal,
PRIMARY KEY ((user_id), account_id)
);

CREATE TABLE IF NOT EXISTS test (
    id uuid,
    primary key (id)
);
INSERT INTO modelmutation (hcpk, filename, time, forced, result, script, user) VALUES ('HCPK', '003-Initial-test.cql', c5d382b0-37a7-11e9-93e1-49398bb05621, false, 'OK', '-', 'cassandra');
