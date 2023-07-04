create table batch_sync_progress_state (
    user_id uuid,
    trxs_pending_removed boolean,
    acct_synced_from_ds boolean,
    trxs_synced_from_ds boolean,
    primary key (user_id)
);