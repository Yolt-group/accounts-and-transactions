package com.yolt.accountsandtransactions.inputprocessing;

import lombok.RequiredArgsConstructor;
import nl.ing.lovebird.clienttokens.ClientUserToken;
import nl.ing.lovebird.extendeddata.common.CurrencyCode;
import nl.ing.lovebird.extendeddata.transaction.TransactionStatus;
import nl.ing.lovebird.providerdomain.ProviderTransactionDTO;

import java.util.List;
import java.util.UUID;

import static java.util.Collections.emptyList;
import static java.util.Optional.empty;
import static java.util.stream.Collectors.collectingAndThen;
import static java.util.stream.Collectors.toList;

/**
 * Currently used for BUDGET_INSIGHT but *not* recommended.
 * It's taking a set of transactions, and only 'adds' them to the database.
 * <p>
 * Flaw 1) There is no 'deletion' yet. So we probably have duplicated or old/to-be-deleted transactions for those providers.
 * Flaw 2) If we 'miss' a delta (assuming flaw 1 is solved by also providing a set of deleted transactions), the state is corrupt.
 * We should either:
 * 1) be completely sure we never miss a delta. (currently this is definitely not the case)
 * 2) have a reconciliation process to repair the data.
 */
@RequiredArgsConstructor
public class DeltaTransactionInsertionStrategy implements TransactionInsertionStrategy {

    @Override
    public Instruction determineTransactionPersistenceInstruction(List<ProviderTransactionDTO> upstreamTransactions, ClientUserToken clientUserToken, UUID yoltAccountId, String provider, CurrencyCode currencyCode) {
        // We insert every transaction, delete nothing (see flaw 2).

        return upstreamTransactions.stream()
                // We only store booked transactions, not pending transactions.  The reason is that scraping providers only send us
                // deltas that we insert into the database every time we receive an update.  If a delta contains a pending transaction
                // that transaction would never be deleted from our database.  In the past (prior to nov 2020) we deleted all pending
                // transactions from the database before inserting new ones.  This is no longer possible because we now insert transactions
                // into two keyspaces: datascience.transactions and accounts_and_transactions.transactions, the former table has the
                // status (pending, booked) of a transactions as part of its primary key, the latter table does not.  Deleting all
                // pending transactions from our own table is therefore not possible (efficiently) and we therefore choose to never
                // insert them in the first place.
                .filter(t -> t.getStatus() == TransactionStatus.BOOKED)
                // The transactionId for a transaction coming from one of our scraping parties is its externalId, we accept
                // whatever id the scraping party assigns to a transactions.
                .map(t -> new ProviderTransactionWithId(t, t.getExternalId()))
                .collect(collectingAndThen(toList(), transactions -> new Instruction(emptyList(), transactions, emptyList(), emptyList(), null, empty())));
    }

}
