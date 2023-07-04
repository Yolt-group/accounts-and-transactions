create TYPE balance(
    balance_type text,
    last_change_date_time timestamp,
    currency text,
    amount decimal
);

alter table accounts add balances list<frozen<balance>>;
