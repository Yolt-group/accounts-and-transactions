package com.yolt.accountsandtransactions.legacyaccounts;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.spi.ILoggingEvent;
import com.yolt.accountsandtransactions.TestAccountBuilder;
import com.yolt.accountsandtransactions.accounts.Account;
import com.yolt.accountsandtransactions.inputprocessing.AccountFromProviders;
import com.yolt.accountsandtransactions.legacyaccounts.AccountMatcher.AccountMatchResult;
import com.yolt.accountsandtransactions.legacyaccounts.AccountMatcher.AccountMatchType;
import nl.ing.lovebird.extendeddata.common.CurrencyCode;
import nl.ing.lovebird.logging.test.CaptureLogEvents;
import nl.ing.lovebird.logging.test.LogEvents;
import nl.ing.lovebird.providerdomain.AccountType;
import nl.ing.lovebird.providerdomain.ProviderAccountNumberDTO;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.UUID;

import static com.yolt.accountsandtransactions.Predef.none;
import static com.yolt.accountsandtransactions.Predef.some;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static java.util.UUID.randomUUID;
import static org.assertj.core.api.Assertions.assertThat;

@CaptureLogEvents
public class AccountMatcherTest {

    private static final String PROVIDER = "SOME_BANK";
    private static final UUID USER_ID = randomUUID();
    private static final UUID USER_SITE_ID = randomUUID();
    private static final String IDENTIFICATION = "properIban";
    private static final String IDENTIFICATION_WITH_EXTRA_CHARS = "prOper    && Iban *&!@";
    private static final CurrencyCode NEW_CURRENCY = CurrencyCode.EUR;
    private static final CurrencyCode OLD_CURRENCY = CurrencyCode.EUR;
    private static final String MASKED_PAN = "properMaskedPan";
    private static final String MASKED_PAN_WITH_EXTRA_CHARS = "prOper && Masked    Pan";

    @Test
    public void whenNoExistingAccounts_shouldNotMatch() {
        var newAcc = prepareNewAccount(NEW_CURRENCY)
                .accountId("acc1")
                .build();

        assertThat(AccountMatcher.findExisting(emptyList(), newAcc, false)).isNotPresent();
    }

    @Test
    public void whenExistingAccountWithSameExternalId_shouldMatch() {
        Account oldAcc = TestAccountBuilder.builder()
                .externalId("acc1")
                .currency(OLD_CURRENCY)
                .build();
        var newAcc = prepareNewAccount(NEW_CURRENCY)
                .accountId("acc1")
                .build();

        var result = AccountMatcher.findExisting(singletonList(oldAcc), newAcc, false);

        assertThat(result).isPresent();
        assertThat(result.get())
                .returns(oldAcc, AccountMatchResult::getAccount)
                .returns(AccountMatchType.EXTERNAL_ID, AccountMatchResult::getAccountMatchType);
    }

    @Test
    public void whenMultipleExistingAccountWithSameExternalId_shouldMatch(LogEvents events) {

        Clock pointInTimeClock = Clock.fixed(Instant.parse("2020-06-17T09:30:00.000Z"), ZoneId.of("UTC"));

        var userId = new UUID(1, 1);
        var userSiteId = new UUID(2, 1);

        Account base = TestAccountBuilder.builder()
                .userId(userId)
                .userSiteId(userSiteId)
                .externalId("acc1")
                .currency(OLD_CURRENCY)
                .build();

        Account existingAccountA = base.toBuilder()
                .id(new UUID(0, 1))
                .lastDataFetchTime(null) // no data fetch time
                .build();
        Account existingAccountB = base.toBuilder()
                .id(new UUID(0, 1))
                .lastDataFetchTime(Instant.now(pointInTimeClock).minus(10, ChronoUnit.DAYS)) // in the past
                .build();
        Account existingAccountC = base.toBuilder()
                .id(new UUID(0, 1))
                .lastDataFetchTime(Instant.now(pointInTimeClock)) // 2020-06-17T09:30:00.000Z
                .build();

        var newAcc = prepareNewAccount(NEW_CURRENCY)
                .accountId("acc1")
                .build();

        var result = AccountMatcher.findExisting(List.of(existingAccountA, existingAccountB, existingAccountC), newAcc, true);

        assertThat(result).isPresent();
        assertThat(result.get())
                .returns(existingAccountC, AccountMatchResult::getAccount)
                .returns(AccountMatchType.EXTERNAL_ID, AccountMatchResult::getAccountMatchType);

        String message = events.stream(AccountMatcher.class, Level.INFO)
                .map(ILoggingEvent::getFormattedMessage)
                .findFirst()
                .get();

        assertThat(message).isEqualTo("""
                Accounts match report:
                 
                Found 3 duplicate accounts in the database matched on EXTERNAL_ID
                Accounts:
                |00000000-0000-0000-0000-000000000001|c9f9f690b2e957fa3ed5c0b21c30241620822dd4|00000000-0000-0002-0000-000000000001|null                |
                |00000000-0000-0000-0000-000000000001|c9f9f690b2e957fa3ed5c0b21c30241620822dd4|00000000-0000-0002-0000-000000000001|2020-06-07T09:30:00Z|
                |00000000-0000-0000-0000-000000000001|c9f9f690b2e957fa3ed5c0b21c30241620822dd4|00000000-0000-0002-0000-000000000001|2020-06-17T09:30:00Z|
                 
                Using Optional[00000000-0000-0000-0000-000000000001] with last data fetch time Optional[2020-06-17T09:30:00Z].
                """);
    }

    @Test
    public void whenMultipleExistingAccountWithSameExternalIdNoLastDateFetch_shouldMatch(LogEvents events) {

        var userId = new UUID(1, 1);
        var userSiteId = new UUID(2, 1);

        Account base = TestAccountBuilder.builder()
                .userId(userId)
                .userSiteId(userSiteId)
                .externalId("acc1")
                .currency(OLD_CURRENCY)
                .build();

        Account existingAccountA = base.toBuilder()
                .id(new UUID(0, 1))
                .lastDataFetchTime(null) // no data fetch time
                .build();
        Account existingAccountB = base.toBuilder()
                .id(new UUID(0, 2))
                .lastDataFetchTime(null) // no data fetch time
                .build();

        var newAcc = prepareNewAccount(NEW_CURRENCY)
                .accountId("acc1")
                .build();

        var result = AccountMatcher.findExisting(List.of(existingAccountA, existingAccountB), newAcc, true);

        assertThat(result).isPresent();
        assertThat(result.get())
                .returns(existingAccountA, AccountMatchResult::getAccount)
                .returns(AccountMatchType.EXTERNAL_ID, AccountMatchResult::getAccountMatchType);

        String message = events.stream(AccountMatcher.class, Level.INFO)
                .map(ILoggingEvent::getFormattedMessage)
                .findFirst()
                .get();

        assertThat(message).isEqualTo("""
                Accounts match report:
                                                                                                                
                Found 2 duplicate accounts in the database matched on EXTERNAL_ID
                Accounts:
                |00000000-0000-0000-0000-000000000001|c9f9f690b2e957fa3ed5c0b21c30241620822dd4|00000000-0000-0002-0000-000000000001|null                |
                |00000000-0000-0000-0000-000000000002|c9f9f690b2e957fa3ed5c0b21c30241620822dd4|00000000-0000-0002-0000-000000000001|null                |
                              
                Using Optional[00000000-0000-0000-0000-000000000001] with last data fetch time Optional.empty.
                """);
    }


    @Test
    public void whenNoIdMatch_andNewAccountNumberNull_shouldNotMatch() {
        Account oldAcc = TestAccountBuilder.builder()
                .externalId("oldAcc")
                .currency(OLD_CURRENCY)
                .accountNumber(new Account.AccountNumber(some("holder"), some(Account.AccountNumber.Scheme.IBAN), some(IDENTIFICATION)))
                .build();
        var newAcc = prepareNewAccount(NEW_CURRENCY)
                .accountId("newAcc")
                .build();

        assertThat(AccountMatcher.findExisting(singletonList(oldAcc), newAcc, false)).isNotPresent();
    }

    @Test
    public void whenNoIdMatch_andNewAccountNumberSchemaNull_shouldNotMatch() {
        Account oldAcc = TestAccountBuilder.builder()
                .externalId("oldAcc")
                .currency(OLD_CURRENCY)
                .accountNumber(new Account.AccountNumber(some("holder"), some(Account.AccountNumber.Scheme.IBAN), some(IDENTIFICATION)))
                .build();
        var newAcc = prepareNewAccount(NEW_CURRENCY)
                .accountId("newAcc")
                .accountNumber(new ProviderAccountNumberDTO(null, IDENTIFICATION))
                .build();

        assertThat(AccountMatcher.findExisting(singletonList(oldAcc), newAcc, false)).isNotPresent();
    }

    @Test
    public void whenNoIdMatch_andNewAccountNumberIdentificationNull_shouldNotMatch() {
        Account oldAcc = TestAccountBuilder.builder()
                .externalId("oldAcc")
                .currency(OLD_CURRENCY)
                .accountNumber(new Account.AccountNumber(some("holder"), some(Account.AccountNumber.Scheme.IBAN), some(IDENTIFICATION)))
                .build();
        var newAcc = prepareNewAccount(NEW_CURRENCY)
                .accountId("newAcc")
                .accountNumber(new ProviderAccountNumberDTO(ProviderAccountNumberDTO.Scheme.IBAN, null))
                .build();

        assertThat(AccountMatcher.findExisting(singletonList(oldAcc), newAcc, false)).isNotPresent();
    }

    @Test
    public void whenNoIdMatch_andNewCurrencyNull_shouldNotMatch() {
        Account oldAcc = TestAccountBuilder.builder()
                .externalId("oldAcc")
                .currency(OLD_CURRENCY)
                .accountNumber(new Account.AccountNumber(some("holder"), some(Account.AccountNumber.Scheme.IBAN), some(IDENTIFICATION)))
                .build();
        var newAcc = prepareNewAccount(null)
                .accountId("newAcc")
                .accountNumber(new ProviderAccountNumberDTO(ProviderAccountNumberDTO.Scheme.IBAN, IDENTIFICATION))
                .build();

        assertThat(AccountMatcher.findExisting(singletonList(oldAcc), newAcc, false)).isNotPresent();
    }

    @Test
    public void whenNoIdMatch_andOldAccountNumberNull_shouldNotMatch() {
        Account oldAcc = TestAccountBuilder.builder()
                .externalId("oldAcc")
                .currency(OLD_CURRENCY)
                .build();
        var newAcc = prepareNewAccount(NEW_CURRENCY)
                .accountId("newAcc")
                .accountNumber(new ProviderAccountNumberDTO(ProviderAccountNumberDTO.Scheme.IBAN, IDENTIFICATION))
                .build();

        assertThat(AccountMatcher.findExisting(singletonList(oldAcc), newAcc, false)).isNotPresent();
    }

    @Test
    public void whenNoIdMatch_andOldAccountNumberSchemaNull_shouldNotMatch() {
        Account oldAcc = TestAccountBuilder.builder()
                .externalId("oldAcc")
                .currency(OLD_CURRENCY)
                .accountNumber(new Account.AccountNumber(some("holder"), none(), some(IDENTIFICATION)))
                .build();
        var newAcc = prepareNewAccount(NEW_CURRENCY)
                .accountId("newAcc")
                .accountNumber(new ProviderAccountNumberDTO(ProviderAccountNumberDTO.Scheme.IBAN, IDENTIFICATION))
                .build();

        assertThat(AccountMatcher.findExisting(singletonList(oldAcc), newAcc, false)).isNotPresent();
    }

    @Test
    public void whenNoIdMatch_andOldAccountNumberIdentificationNull_shouldNotMatch() {
        Account oldAcc = TestAccountBuilder.builder()
                .externalId("oldAcc")
                .currency(OLD_CURRENCY)
                .accountNumber(new Account.AccountNumber(some("holder"), some(Account.AccountNumber.Scheme.IBAN), none()))
                .build();
        var newAcc = prepareNewAccount(NEW_CURRENCY)
                .accountId("newAcc")
                .accountNumber(new ProviderAccountNumberDTO(ProviderAccountNumberDTO.Scheme.IBAN, IDENTIFICATION))
                .build();

        assertThat(AccountMatcher.findExisting(singletonList(oldAcc), newAcc, false)).isNotPresent();
    }

    @Test
    public void whenNoIdMatch_andOldCurrencyNull_shouldNotMatch() {
        Account oldAcc = TestAccountBuilder.builder()
                .externalId("oldAcc")
                .currency(null)
                .accountNumber(new Account.AccountNumber(some("holder"), some(Account.AccountNumber.Scheme.IBAN), some(IDENTIFICATION)))
                .build();
        var newAcc = prepareNewAccount(NEW_CURRENCY)
                .accountId("newAcc")
                .accountNumber(new ProviderAccountNumberDTO(ProviderAccountNumberDTO.Scheme.IBAN, IDENTIFICATION))
                .build();

        assertThat(AccountMatcher.findExisting(singletonList(oldAcc), newAcc, false)).isNotPresent();
    }

    @Test
    public void whenNoIdMatch_andNoSchemaMatch_shouldNotMatch() {
        Account oldAcc = TestAccountBuilder.builder()
                .externalId("oldAcc")
                .currency(OLD_CURRENCY)
                .accountNumber(new Account.AccountNumber(some("holder"), some(Account.AccountNumber.Scheme.SORTCODEACCOUNTNUMBER), some(IDENTIFICATION)))
                .build();
        var newAcc = prepareNewAccount(NEW_CURRENCY)
                .accountId("newAcc")
                .accountNumber(new ProviderAccountNumberDTO(ProviderAccountNumberDTO.Scheme.IBAN, IDENTIFICATION))
                .build();

        assertThat(AccountMatcher.findExisting(singletonList(oldAcc), newAcc, false)).isNotPresent();
    }

    @Test
    public void whenNoIdMatch_andNoIdentificationMatch_shouldNotMatch() {
        Account oldAcc = TestAccountBuilder.builder()
                .externalId("oldAcc")
                .currency(OLD_CURRENCY)
                .accountNumber(new Account.AccountNumber(some("holder"), some(Account.AccountNumber.Scheme.IBAN), some(IDENTIFICATION)))
                .build();
        var newAcc = prepareNewAccount(NEW_CURRENCY)
                .accountId("newAcc")
                .accountNumber(new ProviderAccountNumberDTO(ProviderAccountNumberDTO.Scheme.IBAN, "something went wrong here"))
                .build();

        assertThat(AccountMatcher.findExisting(singletonList(oldAcc), newAcc, false)).isNotPresent();
    }

    @Test
    public void whenNoIdMatch_andNoCurrencyMatch_shouldNotMatch() {
        Account oldAcc = TestAccountBuilder.builder()
                .externalId("oldAcc")
                .currency(OLD_CURRENCY)
                .accountNumber(new Account.AccountNumber(some("holder"), some(Account.AccountNumber.Scheme.IBAN), some(IDENTIFICATION)))
                .build();
        var newAcc = prepareNewAccount(CurrencyCode.GBP)
                .accountId("newAcc")
                .accountNumber(new ProviderAccountNumberDTO(ProviderAccountNumberDTO.Scheme.IBAN, IDENTIFICATION))
                .build();

        assertThat(AccountMatcher.findExisting(singletonList(oldAcc), newAcc, false)).isNotPresent();
    }

    @Test
    public void whenNoIdMatch_andAccountNumberMatch_shouldMatch() {
        Account oldAcc = TestAccountBuilder.builder()
                .externalId("oldAcc")
                .currency(OLD_CURRENCY)
                .accountNumber(new Account.AccountNumber(some("holder"), some(Account.AccountNumber.Scheme.IBAN), some(IDENTIFICATION)))
                .build();
        var newAcc = prepareNewAccount(NEW_CURRENCY)
                .accountId("newAcc")
                .accountNumber(new ProviderAccountNumberDTO(ProviderAccountNumberDTO.Scheme.IBAN, IDENTIFICATION_WITH_EXTRA_CHARS))
                .build();

        var result = AccountMatcher.findExisting(singletonList(oldAcc), newAcc, false);

        assertThat(result).isPresent();
        assertThat(result.get())
                .returns(oldAcc, AccountMatchResult::getAccount)
                .returns(AccountMatchType.IBAN_ACCOUNT_NUMBER_AND_CURRENCY, AccountMatchResult::getAccountMatchType)
                .returns(false, AccountMatchResult::isAccountNumberIsNormalized);
    }

    @Test
    public void whenNoIdMatch_andAccountNumberNull_andAccountTypeCreditCard_andAccountMaskedIdentificationMatch_shouldMatch() {
        Account oldAcc1 = TestAccountBuilder.builder()
                .externalId("oldAcc1")
                .type(AccountType.CURRENT_ACCOUNT)
                .currency(OLD_CURRENCY)
                .maskedPan(MASKED_PAN)
                .build();
        Account oldAcc2 = TestAccountBuilder.builder()
                .externalId("oldAcc2")
                .type(AccountType.CREDIT_CARD)
                .currency(OLD_CURRENCY)
                .maskedPan(MASKED_PAN)
                .build();
        var newAcc = prepareNewAccount(NEW_CURRENCY)
                .accountId("newAcc")
                .yoltAccountType(nl.ing.lovebird.providerdomain.AccountType.CREDIT_CARD)
                .accountMaskedIdentification(MASKED_PAN_WITH_EXTRA_CHARS)
                .build();

        var result = AccountMatcher.findExisting(List.of(oldAcc1, oldAcc2), newAcc, false);

        assertThat(result).isPresent();
        assertThat(result.get())
                .returns(oldAcc2, AccountMatchResult::getAccount)
                .returns(AccountMatchType.MASKED_IDENTIFICATION_AND_CURRENCY, AccountMatchResult::getAccountMatchType);
    }

    private static AccountFromProviders.AccountFromProvidersBuilder prepareNewAccount(CurrencyCode currencyCode) {
        return AccountFromProviders.accountsFromProvidersBuilder()
                .provider(PROVIDER)
                .yoltUserId(USER_ID)
                .yoltUserSiteId(USER_SITE_ID)
                .currency(currencyCode);
    }

    @ParameterizedTest
    @CsvSource({
            "IBAN, GB33BUKB20201555555555, true",
            "IBAN, gb33bukb20201555555555, false",
            "IBAN, GB33 BUKB20201555555555, false",
            "IBAN, GB33BUKB 20201555555555, false",
            "SORTCODEACCOUNTNUMBER, 30-80-87 25337846, true",
            "SORTCODEACCOUNTNUMBER, 30-80-87-25337846, false",
            "SORTCODEACCOUNTNUMBER, 30-80-8725337846, false"
    })
    public void checkAccountPatternNormalization(String scheme, String accountNumber, String isNormalized) {
        Account oldAcc = TestAccountBuilder.builder()
                .externalId("oldAcc")
                .currency(OLD_CURRENCY)
                .iban(null) // reset TestAccountBuilder default; will be overridden by accountNumber
                .accountNumber(new Account.AccountNumber(some("holder"), some(Account.AccountNumber.Scheme.valueOf(scheme)), some(accountNumber)))
                .build();
        var newAcc = prepareNewAccount(NEW_CURRENCY)
                .accountId("newAcc")
                .accountNumber(new ProviderAccountNumberDTO(ProviderAccountNumberDTO.Scheme.valueOf(scheme), accountNumber))
                .build();

        var result = AccountMatcher.findExisting(singletonList(oldAcc), newAcc, false);

        assertThat(result).isPresent();
        assertThat(result.get())
                .returns(oldAcc, AccountMatchResult::getAccount)
                .returns(Boolean.valueOf(isNormalized), AccountMatchResult::isAccountNumberIsNormalized);
    }

    @ParameterizedTest
    @CsvSource({
            "a b 3 - / 9 C #$@!_&*(), AB39C_, true",
            ",, false",
            ",AB39C_, false",
            "a b 3 - / 9 C #$@!_&*(),,false"})
    public void checkSameAccountNumbersIgnoringNonAlphaNum(String first, String second, boolean result) {
        assertThat(AccountMatcher.matchingIdentifications(first, second)).isEqualTo(result);
    }

}
