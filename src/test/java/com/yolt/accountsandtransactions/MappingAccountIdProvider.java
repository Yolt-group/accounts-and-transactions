package com.yolt.accountsandtransactions;

import com.yolt.accountsandtransactions.inputprocessing.AccountFromProviders;
import com.yolt.accountsandtransactions.inputprocessing.AccountIdProvider;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

/**
 * Implementation of {@link AccountIdProvider} which maps account names to predetermined account ids.
 * This implementation is used, instead of the @{link {@link com.yolt.accountsandtransactions.inputprocessing.AccountIdProvider.RandomAccountIdProvider},
 * to create deterministic account-ids which can be asserted in integration tests.
 */
public class MappingAccountIdProvider implements AccountIdProvider<UUID> {
    private final Map<String, UUID> mapping = new HashMap<>();

    public void addMapping(final String name, final UUID accountId) {
        mapping.put(name, accountId);
    }

    @Override
    public UUID generate(final AccountFromProviders accountFromProviders) {
        return Optional.ofNullable(mapping.get(accountFromProviders.getName()))
                .orElseThrow(() -> new IllegalStateException("No mapping available between accounts from providers and accounts (from A&T)"));
    }

    public void clear() {
        mapping.clear();
    }
}
