package com.yolt.accountsandtransactions.inputprocessing;

import java.util.UUID;

public interface AccountIdProvider<T> {

    T generate(AccountFromProviders accountFromProviders);

    class RandomAccountIdProvider implements AccountIdProvider<UUID> {

        /**
         * Generate a random {@link UUID}.
         *
         * @param accountFromProviders the account as it came from providers. Not used in this implementation
         * @return A random UUID as account-id
         */
        @Override
        public UUID generate(final AccountFromProviders accountFromProviders) {
            return UUID.randomUUID();
        }
    }
}
