package com.yolt.accountsandtransactions.inputprocessing;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import nl.ing.lovebird.providerdomain.ProviderAccountDTO;
import nl.ing.lovebird.providerdomain.ProviderAccountNumberDTO;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * This class addresses following issue:
 * - duplicated accounts arrive in a single upstream from data provider
 * - for new user-sites there would be nothing in DB yet, so we can't compare upstream with previous entries
 * - we store all accounts one by one with newly generated yoltAccountId which results in one (or more) duplicate(s)
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
@Slf4j
public final class AccountsDeduplicator {

    /**
     * According to previous discussions between C4PO and YCS teams we came to a conclusion that following set of
     * fields may guarantee that account is unique:
     * - accountId (external)
     * - accountNumber (if exists) - schema and identity
     * - currency code
     * This method checks all of the above and reflects the same approach is in {@link com.yolt.accountsandtransactions.legacyaccounts.AccountMatcher}
     *
     * @param upstreamAccounts - accounts from data provider
     * @return - de-duplicated (distinct) accounts
     */
    static List<AccountFromProviders> deduplicateAccounts(final List<AccountFromProviders> upstreamAccounts) {
        if (upstreamAccounts == null || upstreamAccounts.size() < 2) {
            return upstreamAccounts;
        }

        final Map<String, Integer> duplications = new HashMap<>();
        final Set<Integer> duplicateIndexes = new HashSet<>();
        for (int i = 0; i < upstreamAccounts.size(); i++) {
            for (int j = i + 1; j < upstreamAccounts.size(); j++) {
                final AccountFromProviders left = upstreamAccounts.get(i);
                final AccountFromProviders right = upstreamAccounts.get(j);
                if (isEqualAccountId(left, right) && isEqualAccountNumber(left, right) && isEqualCurrency(left, right)) {
                    duplications.compute(left.getAccountId(), (k, v) -> (v == null) ? 0 : v + 1);
                    duplicateIndexes.add(j);
                }
            }
        }

        duplications.forEach((key, value) -> log.warn("Found {} duplicates (compared id, number, currency) for " +
                "accountId {}, ignoring {} of those", value + 1, key, value));

        return IntStream.range(0, upstreamAccounts.size())
                .filter(index -> !duplicateIndexes.contains(index))
                .mapToObj(upstreamAccounts::get)
                .collect(Collectors.toList());
    }

    /**
     * accountId is mandatory according to {@link ProviderAccountDTO#validate()}
     * However, we will add a safe check to spot when 'validate()' method was not called upfront (should not happen).
     *
     * @param left  - one upstream account
     * @param right - another upstream account
     * @return - true when account ids are equal, otherwise false
     * @throws NullPointerException when account either or both account ids are null
     */
    private static boolean isEqualAccountId(final AccountFromProviders left, final AccountFromProviders right) {
        Objects.requireNonNull(left.getAccountId(), "accountId");
        Objects.requireNonNull(right.getAccountId(), "accountId");
        return left.getAccountId().equals(right.getAccountId());
    }

    /**
     * accountNumber is optional according to {@link ProviderAccountDTO#validate()}
     * However, when it exists, both 'schema' and 'identification' are mandatory.
     * But we should null-check those in case if something went wrong in a different service and 'validate()' method
     * was not called.
     *
     * @param left  - one upstream account
     * @param right - another upstream account
     * @return - true when both accountNumbers are null or equal, otherwise false
     * @throws NullPointerException when accountNumber is not null and schema or identification is null
     */
    private static boolean isEqualAccountNumber(final AccountFromProviders left, final AccountFromProviders right) {
        final ProviderAccountNumberDTO leftAccountNumber = left.getAccountNumber();
        final ProviderAccountNumberDTO rightAccountNumber = right.getAccountNumber();
        if (leftAccountNumber == null && rightAccountNumber == null) {
            return true;
        } else if (leftAccountNumber == null || rightAccountNumber == null) {
            return false;
        }

        Objects.requireNonNull(leftAccountNumber.getScheme(), "scheme");
        Objects.requireNonNull(leftAccountNumber.getIdentification(), "identification");
        Objects.requireNonNull(rightAccountNumber.getScheme(), "scheme");
        Objects.requireNonNull(rightAccountNumber.getIdentification(), "identification");

        return leftAccountNumber.getScheme() == rightAccountNumber.getScheme()
                && leftAccountNumber.getIdentification().equals(rightAccountNumber.getIdentification());
    }

    /**
     * currency is mandatory according to {@link ProviderAccountDTO#validate()}
     * However, we will add a safe check to spot when 'validate()' method was not called upfront (should not happen).
     *
     * @param left  - one upstream account
     * @param right - another upstream account
     * @return - true when currencies are equal
     * @throws NullPointerException when either or both currencies are null
     */
    private static boolean isEqualCurrency(final AccountFromProviders left, final AccountFromProviders right) {
        Objects.requireNonNull(left.getCurrency(), "currency");
        Objects.requireNonNull(right.getCurrency(), "currency");
        return left.getCurrency() == right.getCurrency();
    }
}
