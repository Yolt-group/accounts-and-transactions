package com.yolt.accountsandtransactions.legacyaccounts;

import com.yolt.accountsandtransactions.accounts.Account;
import com.yolt.accountsandtransactions.inputprocessing.AccountFromProviders;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.SneakyThrows;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import nl.ing.lovebird.providerdomain.AccountType;
import nl.ing.lovebird.providerdomain.ProviderAccountNumberDTO;
import org.springframework.lang.Nullable;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.regex.Pattern;

import static com.yolt.accountsandtransactions.Predef.maybe;
import static com.yolt.accountsandtransactions.Predef.some;
import static java.lang.String.format;
import static java.util.Comparator.comparing;
import static java.util.Optional.empty;
import static java.util.Optional.ofNullable;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;
import static nl.ing.lovebird.providerdomain.ProviderAccountNumberDTO.Scheme.IBAN;
import static nl.ing.lovebird.providerdomain.ProviderAccountNumberDTO.Scheme.SORTCODEACCOUNTNUMBER;
import static org.apache.commons.lang3.StringUtils.containsNone;
import static org.apache.commons.lang3.StringUtils.isAllUpperCase;
import static org.apache.commons.lang3.StringUtils.isBlank;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
@Slf4j
public final class AccountMatcher {
    // We expect a normalized sortcode accountnumber to contain hyphens to separate the sortcode numbers and a whitespace
    // to separate the sortcode from the accountnumber (e.g. 00-00-00 1234567)
    private static final Pattern normalizedSortCodeAccountNumberPattern = Pattern.compile("^\\d{2}-\\d{2}-\\d{2}\\s(\\d){7,8}$");

    // StringUtils.isAllUpperCase does not take numbers into consideration so we have to filter any numbers from the original account number
    private static final String REPLACE_NUMBERS_REGEX = "\\d+(?:[.,]\\d+)*\\s*";

    private static final String WHITESPACE_AND_NON_WORD_CHARACTER = "[\\s\\W]";

    public static @NonNull Optional<AccountMatchResult> findExisting(
            final @NonNull List<Account> existingAccounts,
            final @NonNull AccountFromProviders providerAccountDTO, boolean log) {

        return matchByExternalId(existingAccounts, providerAccountDTO, log)
                .or(() -> matchByAccountNumberAndCurrency(existingAccounts, providerAccountDTO, log))
                .or(() -> matchByAccountMaskedIdentificationAndCurrency(existingAccounts, providerAccountDTO, log));
    }

    /**
     * Most of the banks provide consistent accountIds, so we start comparison using those
     *
     * @param existingAccounts   - accounts from database
     * @param providerAccountDTO - account from upstream from providers
     * @param log
     * @return - optional matched account
     */
    private static @NonNull Optional<AccountMatchResult> matchByExternalId(
            final List<Account> existingAccounts,
            final AccountFromProviders providerAccountDTO, boolean log) {

        var externalAccountId = providerAccountDTO.getAccountId();
        var matchingExistingAccounts = existingAccounts.stream()
                .filter(existingAccount -> existingAccount.getExternalId().equals(externalAccountId))
                .map(existingAccount -> new AccountMatchResult(existingAccount, AccountMatchType.EXTERNAL_ID, false))
                .collect(toList());

        return selectLastRefreshed(matchingExistingAccounts, log);
    }

    /**
     * Some bank (for example, Tesco) change accountId with every user re-consent, so we try to match account numbers instead.
     * In case of multi-currency accounts which we process as a bunch of single-currency accounts, we need to match by currency as well.
     *
     * @param existingAccounts   - accounts from database
     * @param providerAccountDTO - account from upstream from providers
     * @param log
     * @return - optional matched account
     */
    private static @NonNull Optional<AccountMatchResult> matchByAccountNumberAndCurrency(
            final List<Account> existingAccounts,
            final AccountFromProviders providerAccountDTO, boolean log) {

        var accountNumber = providerAccountDTO.getAccountNumber();

        if (accountNumber == null
                || accountNumber.getScheme() == null
                || isBlank(accountNumber.getIdentification())
                || providerAccountDTO.getCurrency() == null) {
            return empty();
        }

        var matchingExistingAccounts = existingAccounts.stream()
                .filter(existingAccount -> existingAccount.getAccountNumber().isPresent())
                .flatMap(existingAccount -> areMatchingAccounts(existingAccount, providerAccountDTO).stream())
                .collect(toList());

        return selectLastRefreshed(matchingExistingAccounts, log);
    }

    /**
     * Some banks (for example, Sainsburys, Virgin Money and others supporting credit cards) change accountId with every user re-consent
     * and especially for CREDIT_CARD account type there is no so called account number - there is account masked identification (MASKED PAN) provided instead.
     * For this case we need to try to match against account masked identification number.
     * In case of multi-currency accounts which we process as a bunch of single-currency accounts, we need to match by currency as well.
     *
     * @param existingAccounts   - accounts from database
     * @param providerAccountDTO - account from upstream from providers
     * @param log
     * @return - optional matched account
     */
    private static @NonNull Optional<AccountMatchResult> matchByAccountMaskedIdentificationAndCurrency(
            final List<Account> existingAccounts,
            final AccountFromProviders providerAccountDTO, boolean log) {

        var accountMaskedIdentification = providerAccountDTO.getAccountMaskedIdentification();
        if (!AccountType.CREDIT_CARD.equals(providerAccountDTO.getYoltAccountType())
                || isBlank(accountMaskedIdentification)
                || providerAccountDTO.getCurrency() == null) {
            return empty();
        }

        var matchingExistingAccounts = existingAccounts.stream()
                .filter(existingAccount -> AccountType.CREDIT_CARD.equals(existingAccount.getType()))
                .filter(existingAccount -> matchingIdentifications(accountMaskedIdentification, existingAccount.getMaskedPan()))
                .filter(existingAccount -> providerAccountDTO.getCurrency() == existingAccount.getCurrency())
                .map(existingAccount -> new AccountMatchResult(existingAccount, AccountMatchType.MASKED_IDENTIFICATION_AND_CURRENCY, false))
                .collect(toList());

        return selectLastRefreshed(matchingExistingAccounts, log);
    }

    /**
     * Compare two accounts and determine if they are equal based on:
     * <ul>
     *     <li>account scheme equality</li> AND
     *     <li>account identification equality</li> AND
     *     <li>account currency equality</li>
     * </ul>
     * <p/>
     * Accounts are not matched by definition if scheme and/or identification is absent (null) in any of the two accounts provided
     *
     * @param existingAccount    the local/ existing account
     * @param providerAccountDTO the upstream/ provider account
     * @return an {@link Optional} filled with {@link AccountMatchResult} if matched, empty otherwise
     */
    private static @NonNull Optional<AccountMatchResult> areMatchingAccounts(Account existingAccount, AccountFromProviders providerAccountDTO) {
        var accountNumber = providerAccountDTO.getAccountNumber();
        var existingSchemeOrNull = existingAccount.getAccountNumber()
                .flatMap(n -> n.scheme)
                .flatMap(AccountMatcher::asProviderAccountNumberScheme)
                .orElse(null);

        var existingAccountIdentificationOrNull = existingAccount.getAccountNumber()
                .flatMap(number -> number.identification)
                .orElse(null);

        if (accountNumber.getScheme() == existingSchemeOrNull
                && matchingIdentifications(accountNumber.getIdentification(), existingAccountIdentificationOrNull)
                && providerAccountDTO.getCurrency() == existingAccount.getCurrency()) {
            return some(new AccountMatchResult(existingAccount,
                    SORTCODEACCOUNTNUMBER.equals(existingSchemeOrNull) ? AccountMatchType.SORTCODE_ACCOUNT_NUMBER_AND_CURRENCY : AccountMatchType.IBAN_ACCOUNT_NUMBER_AND_CURRENCY,
                    isAccountNumberNormalized(accountNumber)));
        }
        return empty();
    }


    /**
     * Select the last refreshed account from a set of duplicate account match results.
     * <p/>
     * Duplicate accounts are defined as accounts having identical invariants but different identifiers.
     * As we have a number of duplicate accounts currently in our database(s), selecting a consistent duplicate
     * is important to prevent any further processing from alternating between identical accounts.
     * The selection is made based on the max(lastDataFetchTime) property which indicates when the account was last refreshed.
     *
     * @param matchResults the match results to select from
     * @param log
     * @return the match result that contains the last refreshed account
     */
    private static Optional<AccountMatchResult> selectLastRefreshed(final @NonNull List<AccountMatchResult> matchResults, boolean log) {

        if (matchResults.size() > 1) {

            // determine max(Account#lastDataFetchTime). If LastDataFetchTime is null, assume EPOCH
            var max = matchResults.stream()
                    .max(comparing(matchResult -> maybe(matchResult.getAccount().getLastDataFetchTime()).orElse(Instant.EPOCH)));

            if (log) {
                try {
                    AccountMatcher.log.info("""
                                    Accounts match report:
                                                                    
                                    Found {} duplicate accounts in the database matched on {}
                                    Accounts:
                                    {}
                                                                    
                                    Using {} with last data fetch time {}.
                                    """,
                            matchResults.size(),
                            matchResults.stream()
                                    .map(accountMatchResult -> accountMatchResult.getAccountMatchType().name())
                                    .distinct()
                                    .collect(joining(",")),
                            outputInTabular(matchResults),
                            max.map(accountMatchResult -> accountMatchResult.getAccount().getId()),
                            max.map(accountMatchResult -> accountMatchResult.getAccount().getLastDataFetchTime()));
                } catch (Exception e) {
                    AccountMatcher.log.error("Failed to generate account matcher log output.");
                }
            }

            return max;
        } else {
            return matchResults.stream()
                    .limit(1)
                    .peek(accountMatchResult -> AccountMatcher.log.info("""
                                    Account match report:
                                                                        
                                    Found 1 account in the database matched on {}.
                                    Using {} with last data fetch time {}
                                    """,
                            accountMatchResult.getAccountMatchType(),
                            accountMatchResult.getAccount().getId(),
                            accountMatchResult.getAccount().getLastDataFetchTime()))
                    .findFirst();
        }
    }

    public static String outputInTabular(final List<AccountMatchResult> accounts) {
        var format = "|%1$-16s|%2$-16s|%3$-16s|%4$-20s|";

        return accounts.stream()
                .map(match -> {
                    var account = match.getAccount();

                    return format(format,
                            account.getId(),
                            hash(account.getExternalId()),
                            account.getUserSiteId(),
                            account.getLastDataFetchTime());
                })
                .collect(joining("\n"));
    }

    @SneakyThrows
    public static String hash(@NonNull String src) {
        return hex(MessageDigest.getInstance("SHA1").digest(src.getBytes(StandardCharsets.UTF_8)));
    }

    public static String hex(byte[] bytes) {
        StringBuilder result = new StringBuilder();
        for (byte b : bytes) {
            result.append(String.format("%02x", b));
        }
        return result.toString();
    }

    private static @NonNull Optional<ProviderAccountNumberDTO.Scheme> asProviderAccountNumberScheme(
            final @Nullable Account.AccountNumber.Scheme scheme) {
        return maybe(scheme)
                .flatMap(s -> switch (s) {
                    case IBAN -> some(IBAN);
                    case SORTCODEACCOUNTNUMBER -> some(SORTCODEACCOUNTNUMBER);
                });
    }

    private static boolean isAccountNumberNormalized(ProviderAccountNumberDTO accountNumber) {
        switch (accountNumber.getScheme()) {
            case IBAN:
                var accountNumberWithoutDigits = accountNumber.getIdentification().replaceAll(REPLACE_NUMBERS_REGEX, "");
                return containsNone(accountNumber.getIdentification(), " ") && isAllUpperCase(accountNumberWithoutDigits);
            case SORTCODEACCOUNTNUMBER:
                return normalizedSortCodeAccountNumberPattern.matcher(accountNumber.getIdentification()).matches();
            default:
                return false;
        }
    }

    static boolean matchingIdentifications(String firstIdentification, String secondIdentification) {
        return ofNullable(firstIdentification)
                .map(AccountMatcher::removeNonAlphaNum)
                .map(first -> first.equals(removeNonAlphaNum(secondIdentification)))
                .orElse(false);
    }

    private static String removeNonAlphaNum(String toBeNormalized) {
        return toBeNormalized != null ? toBeNormalized.replaceAll(WHITESPACE_AND_NON_WORD_CHARACTER, "").toUpperCase() : null;
    }

    @Value
    @AllArgsConstructor
    public static class AccountMatchResult {
        Account Account;
        AccountMatchType accountMatchType;
        boolean accountNumberIsNormalized;
    }

    public enum AccountMatchType {
        EXTERNAL_ID,
        SORTCODE_ACCOUNT_NUMBER_AND_CURRENCY,
        IBAN_ACCOUNT_NUMBER_AND_CURRENCY,
        MASKED_IDENTIFICATION_AND_CURRENCY,
        NO_MATCH
    }
}
