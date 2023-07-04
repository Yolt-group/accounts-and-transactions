package com.yolt.accountsandtransactions.inputprocessing;

import com.yolt.accountsandtransactions.transactions.Transaction;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.NonNull;
import lombok.Value;
import nl.ing.lovebird.clienttokens.ClientUserToken;
import nl.ing.lovebird.extendeddata.common.CurrencyCode;
import nl.ing.lovebird.providerdomain.ProviderTransactionDTO;

import java.time.LocalDate;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static brave.internal.collect.Lists.concat;
import static java.util.Collections.emptyList;
import static java.util.Optional.empty;

/**
 * There are more strategies to reconcile the upstream transactions to which we have already persisted.
 * For example:
 * {@link DeltaTransactionInsertionStrategy} This just assumes we get only a 'delta' of transactions. Those will be upserted.
 * Nothing gets deleted.
 * {@link DefaultTransactionInsertionStrategy} The default assumes we get *all* the existing transactions in a given time window.
 * Due to quality of data there are some ways of matching performed so we end up with a proper set of instructions so we end up with a
 * proper set of persisted transactions.
 */
public interface TransactionInsertionStrategy {

    Instruction determineTransactionPersistenceInstruction(List<ProviderTransactionDTO> upstreamTransactions, ClientUserToken clientUserToken, UUID yoltAccountId, String provider, CurrencyCode currencyCode);

    Instruction EMPTY_INSTRUCTION = new Instruction(emptyList(), emptyList(), emptyList(), emptyList(), null, empty());

    enum Mode {
        ACTIVE,
        TEST
    }

    default @NonNull AttributeInsertionStrategy.Mode getMode() {
        return Mode.ACTIVE; // default MODE is active
    }

    @Value
    @AllArgsConstructor
    @Builder(toBuilder = true)
    class Instruction {

        public Instruction(final @NonNull List<ProviderTransactionWithId> transactionsToInsert, final @NonNull Optional<LocalDate> oldestTransactionChangeDate) {
            this.transactionsToInsert = transactionsToInsert;
            this.transactionsToDelete = emptyList();
            this.transactionsToIgnore = emptyList();
            this.transactionsToUpdate = emptyList();
            this.metrics = null;
            this.oldestTransactionChangeDate = oldestTransactionChangeDate;
        }

        public enum InstructionType {
            DELETE,
            INSERT,
            UPDATE;
        }

        List<Transaction> transactionsToDelete;
        List<ProviderTransactionWithId> transactionsToInsert; // upsert
        List<ProviderTransactionWithId> transactionsToUpdate; // upsert
        List<ProviderTransactionWithId> transactionsToIgnore; // ignored

        /**
         * Only used by {@link DefaultTransactionInsertionStrategy}
         */
        TransactionReconciliationResultMetrics metrics;

        /**
         * The oldest transaction change date in the set aka min(Transaction#date) in the above mentions transaction sets if any
         */
        Optional<LocalDate> oldestTransactionChangeDate;

        public Instruction appendToDelete(final @NonNull List<Transaction> append) {
            return this.toBuilder()
                    .transactionsToDelete(concat(transactionsToDelete, append))
                    .build();
        }
    }
}
