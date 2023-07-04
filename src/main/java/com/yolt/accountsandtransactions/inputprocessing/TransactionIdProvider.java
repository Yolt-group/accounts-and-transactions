package com.yolt.accountsandtransactions.inputprocessing;

import nl.ing.lovebird.providerdomain.ProviderTransactionDTO;

import java.util.UUID;

public interface TransactionIdProvider<T> {

    T generate(ProviderTransactionDTO providerTransaction);

    class RandomTransactionIdProvider implements TransactionIdProvider<UUID> {

        /**
         * Generate a random {@link UUID}.
         *
         * @param providerTransaction the transaction as it came from providers.  Not used in this implementation.
         * @return A random UUID as transactionId
         */
        @Override
        public UUID generate(final ProviderTransactionDTO providerTransaction) {
            return UUID.randomUUID();
        }
    }
}
