package com.yolt.accountsandtransactions.inputprocessing;

import com.yolt.accountsandtransactions.transactions.Transaction;
import lombok.NonNull;

import java.time.LocalDate;
import java.util.List;
import java.util.UUID;

@FunctionalInterface
public interface TransactionLoader {

    /**
     * Load transactions based on the user-id, account-id and earliest (from) date.
     *
     * @param userId                the user-id
     * @param accountId             the account-id from which to retrieve the transactions
     * @param earliestDateInclusive the date from which to start the transaction
     * @return A list of transactions
     */
    List<Transaction> load(final @NonNull UUID userId, final @NonNull UUID accountId, final @NonNull LocalDate earliestDateInclusive);
}
