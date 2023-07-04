package com.yolt.accountsandtransactions.legacyaccounts;

import lombok.extern.slf4j.Slf4j;
import nl.ing.lovebird.providerdomain.AccountType;
import org.apache.commons.lang3.math.NumberUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Collection;
import java.util.Currency;
import java.util.Date;
import java.util.List;
import java.util.UUID;

import static com.yolt.accountsandtransactions.legacyaccounts.LegacyAccountType.CREDIT_CARD;
import static com.yolt.accountsandtransactions.legacyaccounts.LegacyAccountType.CURRENT_ACCOUNT;
import static com.yolt.accountsandtransactions.legacyaccounts.LegacyAccountType.PREPAID_ACCOUNT;
import static com.yolt.accountsandtransactions.legacyaccounts.LegacyAccountType.SAVINGS_ACCOUNT;
import static org.assertj.core.api.Assertions.assertThat;

@Slf4j
class LegacyAccountGroupsServiceTest {
    public static final Currency CURRENCY_EUR = Currency.getInstance("EUR");
    public static final Currency CURRENCY_GBP = Currency.getInstance("GBP");

    private UUID accountId1;
    private UUID accountId2;
    private UUID accountId3;
    private UUID accountId4;
    private UUID accountId5;

    private LegacyAccountGroupsService accountGroupsService;

    @BeforeEach
    void beforeEach() {
        accountId1 = UUID.randomUUID();
        accountId2 = UUID.randomUUID();
        accountId3 = UUID.randomUUID();
        accountId4 = UUID.randomUUID();
        accountId5 = UUID.randomUUID();

        accountGroupsService = new LegacyAccountGroupsService();
    }

    @Test
    void testAllAccountTypes() {
        LegacyAccountDTO account1 = createAccount(accountId1, CURRENT_ACCOUNT, CURRENCY_EUR, new BigDecimal("100.10"), new BigDecimal("101.10"));
        LegacyAccountDTO account2 = createAccount(accountId2, CURRENT_ACCOUNT, CURRENCY_EUR, new BigDecimal("200.20"), new BigDecimal("202.20"));
        LegacyAccountDTO account3 = createAccount(accountId3, CURRENT_ACCOUNT, CURRENCY_GBP, new BigDecimal("300.30"), new BigDecimal("303.30"));
        LegacyAccountDTO account4 = createAccount(accountId4, SAVINGS_ACCOUNT, CURRENCY_EUR, new BigDecimal("400.40"), new BigDecimal("404.40"));
        LegacyAccountDTO account5 = createAccount(accountId5, CREDIT_CARD, CURRENCY_EUR, new BigDecimal("500.50"), new BigDecimal("505.50"));
        LegacyAccountDTO account6 = createAccount(accountId5, PREPAID_ACCOUNT, CURRENCY_EUR, new BigDecimal("700.50"), new BigDecimal("505.50"));
        List<LegacyAccountDTO> accounts = Arrays.asList(account1, account2, account3, account4, account5, account6);

        List<LegacyAccountGroupDTO> accountGroups = accountGroupsService.buildAccountGroups(accounts);

        assertThat(accountGroups).hasSize(5)
                .extracting(LegacyAccountGroupDTO::getTotals)
                .extracting(Collection::size)
                .allMatch(NumberUtils.INTEGER_ONE::equals);

        assertThat(accountGroups.get(0).getType()).isEqualTo(CURRENT_ACCOUNT);
        assertThat(accountGroups.get(0).getAccounts()).containsExactlyInAnyOrder(account1, account2);
        assertThat(accountGroups.get(0).getTotals().get(0).getCurrencyCode()).isEqualTo(CURRENCY_EUR);
        assertThat(accountGroups.get(0).getTotals().get(0).getTotal()).isEqualTo(new BigDecimal("300.30"));

        assertThat(accountGroups.get(1).getType()).isEqualTo(CREDIT_CARD);
        assertThat(accountGroups.get(1).getAccounts()).containsExactlyInAnyOrder(account5);
        assertThat(accountGroups.get(1).getTotals().get(0).getCurrencyCode()).isEqualTo(CURRENCY_EUR);
        assertThat(accountGroups.get(1).getTotals().get(0).getTotal()).isEqualTo(new BigDecimal("500.50"));

        assertThat(accountGroups.get(2).getType()).isEqualTo(SAVINGS_ACCOUNT);
        assertThat(accountGroups.get(2).getAccounts()).containsExactlyInAnyOrder(account4);
        assertThat(accountGroups.get(2).getTotals().get(0).getCurrencyCode()).isEqualTo(CURRENCY_EUR);
        assertThat(accountGroups.get(2).getTotals().get(0).getTotal()).isEqualTo(new BigDecimal("400.40"));

        assertThat(accountGroups.get(3).getType()).isEqualTo(PREPAID_ACCOUNT);
        assertThat(accountGroups.get(3).getAccounts()).containsExactlyInAnyOrder(account6);
        assertThat(accountGroups.get(3).getTotals().get(0).getCurrencyCode()).isEqualTo(CURRENCY_EUR);
        assertThat(accountGroups.get(3).getTotals().get(0).getTotal()).isEqualTo(new BigDecimal("700.50"));

        assertThat(accountGroups.get(4).getType()).isEqualTo(LegacyAccountType.FOREIGN_CURRENCY);
        assertThat(accountGroups.get(4).getAccounts()).containsExactlyInAnyOrder(account3);
        assertThat(accountGroups.get(4).getTotals().get(0).getCurrencyCode()).isEqualTo(CURRENCY_GBP);
        assertThat(accountGroups.get(4).getTotals().get(0).getTotal()).isEqualTo(new BigDecimal("300.30"));
    }

    @Test
    void test2Accounts1BalanceMissing() {
        LegacyAccountDTO account1 = createAccount(accountId1, CURRENT_ACCOUNT, CURRENCY_EUR, new BigDecimal("100.10"), new BigDecimal("101.10"));
        LegacyAccountDTO account2 = createAccount(accountId2, CURRENT_ACCOUNT, CURRENCY_EUR, null, new BigDecimal("202.20"));
        List<LegacyAccountDTO> accounts = Arrays.asList(account1, account2);

        List<LegacyAccountGroupDTO> accountGroups = accountGroupsService.buildAccountGroups(accounts);

        assertThat(accountGroups).hasSize(1);

        assertThat(accountGroups.get(0).getType()).isEqualTo(CURRENT_ACCOUNT);
        assertThat(accountGroups.get(0).getAccounts()).containsExactlyInAnyOrder(account1, account2);
        assertThat(accountGroups.get(0).getTotals()).isEmpty();
    }

    @Test
    void test2Accounts1CurrencyMissing() {
        LegacyAccountDTO account1 = createAccount(accountId1, CURRENT_ACCOUNT, CURRENCY_EUR, new BigDecimal("100.10"), new BigDecimal("101.10"));
        LegacyAccountDTO account2 = createAccount(accountId2, CURRENT_ACCOUNT, null, new BigDecimal("200.20"), new BigDecimal("202.20"));
        List<LegacyAccountDTO> accounts = Arrays.asList(account1, account2);

        List<LegacyAccountGroupDTO> accountGroups = accountGroupsService.buildAccountGroups(accounts);

        assertThat(accountGroups).hasSize(2);

        assertThat(accountGroups.get(0).getType()).isEqualTo(CURRENT_ACCOUNT);
        assertThat(accountGroups.get(0).getAccounts()).containsExactlyInAnyOrder(account1);
        assertThat(accountGroups.get(0).getTotals()).hasSize(1);
        assertThat(accountGroups.get(0).getTotals().get(0).getCurrencyCode()).isEqualTo(CURRENCY_EUR);
        assertThat(accountGroups.get(0).getTotals().get(0).getTotal()).isEqualTo(new BigDecimal("100.10"));

        assertThat(accountGroups.get(1).getType()).isEqualTo(LegacyAccountType.FOREIGN_CURRENCY);
        assertThat(accountGroups.get(1).getAccounts()).containsExactlyInAnyOrder(account2);
        assertThat(accountGroups.get(1).getTotals()).isEmpty();
    }

    @Test
    void test2Accounts1AccountTypeMissing() {
        final LegacyAccountDTO account1 = createAccount(accountId1, SAVINGS_ACCOUNT, CURRENCY_EUR, new BigDecimal("100.10"), new BigDecimal("101.10"));
        final LegacyAccountDTO account2 = createAccount(accountId2, null, CURRENCY_EUR, new BigDecimal("200.20"), new BigDecimal("202.20"));
        final List<LegacyAccountDTO> accounts = Arrays.asList(account1, account2);

        List<LegacyAccountGroupDTO> accountGroups = accountGroupsService.buildAccountGroups(accounts);

        assertThat(accountGroups).hasSize(2);

        assertThat(accountGroups.get(0).getType()).isEqualTo(SAVINGS_ACCOUNT);
        assertThat(accountGroups.get(0).getAccounts()).containsExactlyInAnyOrder(account1);
        assertThat(accountGroups.get(0).getTotals()).hasSize(1);
        assertThat(accountGroups.get(0).getTotals().get(0).getCurrencyCode()).isEqualTo(CURRENCY_EUR);
        assertThat(accountGroups.get(0).getTotals().get(0).getTotal()).isEqualTo(new BigDecimal("100.10"));

        assertThat(accountGroups.get(1).getType()).isEqualTo(LegacyAccountType.UNKNOWN);
        assertThat(accountGroups.get(1).getAccounts()).containsExactlyInAnyOrder(account2);
        assertThat(accountGroups.get(1).getTotals()).hasSize(1);
        assertThat(accountGroups.get(1).getTotals().get(0).getCurrencyCode()).isEqualTo(CURRENCY_EUR);
        assertThat(accountGroups.get(1).getTotals().get(0).getTotal()).isEqualTo(new BigDecimal("200.20"));
    }

    @Test
    void testNotIncludeInTotals() {
        final LegacyAccountDTO account1 = createAccount(accountId1, CREDIT_CARD, CURRENCY_EUR, new BigDecimal("100.10"), new BigDecimal("101.10"))
                .toBuilder().status(LegacyAccountStatusCode.DATASCIENCE_FINISHED).build();
        final LegacyAccountDTO account2 = createAccount(accountId2, CREDIT_CARD, CURRENCY_EUR, new BigDecimal("200.20"), new BigDecimal("202.20"))
                .toBuilder().status(LegacyAccountStatusCode.DELETED).build();
        final LegacyAccountDTO account3 = createAccount(accountId4, CREDIT_CARD, CURRENCY_EUR, new BigDecimal("400.40"), new BigDecimal("404.40"))
                .toBuilder().hidden(Boolean.TRUE).build();
        final List<LegacyAccountDTO> accounts = List.of(account1, account2, account3);

        List<LegacyAccountGroupDTO> accountGroups = accountGroupsService.buildAccountGroups(accounts);

        assertThat(accountGroups).hasSize(1);

        assertThat(accountGroups.get(0).getType()).isEqualTo(LegacyAccountType.CREDIT_CARD);
        assertThat(accountGroups.get(0).getAccounts()).hasSize(3);
        assertThat(accountGroups.get(0).getTotals()).hasSize(1);
        assertThat(accountGroups.get(0).getTotals().get(0).getCurrencyCode()).isEqualTo(CURRENCY_EUR);
        assertThat(accountGroups.get(0).getTotals().get(0).getTotal()).isEqualTo(new BigDecimal("100.10"));
    }

    @Test
    void testDoIncludeExplicitlySetToNotHidden() {
        final LegacyAccountDTO account = createAccount(accountId4, CREDIT_CARD, CURRENCY_EUR, new BigDecimal("400.40"), new BigDecimal("404.40"))
                .toBuilder().hidden(Boolean.FALSE).build();

        List<LegacyAccountGroupDTO> accountGroups = accountGroupsService.buildAccountGroups(List.of(account));

        assertThat(accountGroups).hasSize(1);
        assertThat(accountGroups.get(0).getType()).isEqualTo(LegacyAccountType.CREDIT_CARD);
        assertThat(accountGroups.get(0).getAccounts()).hasSize(1);
        assertThat(accountGroups.get(0).getTotals()).hasSize(1);
        assertThat(accountGroups.get(0).getTotals().get(0).getCurrencyCode()).isEqualTo(CURRENCY_EUR);
        assertThat(accountGroups.get(0).getTotals().get(0).getTotal()).isEqualTo(new BigDecimal("400.40"));
    }

    @ParameterizedTest
    @EnumSource(value = AccountType.class)
    void testAllAccountTypesProcessed(AccountType accountType) {
        LegacyAccountType legacyAccountType = LegacyAccountType.valueOf(accountType.name());
        final LegacyAccountDTO account = createAccount(accountId4, legacyAccountType, CURRENCY_EUR, new BigDecimal("400.40"), new BigDecimal("404.40"));

        List<LegacyAccountGroupDTO> accountGroups = accountGroupsService.buildAccountGroups(List.of(account));

        assertThat(accountGroups).hasSize(1);
        assertThat(accountGroups.get(0).getType()).isEqualTo(legacyAccountType);
        assertThat(accountGroups.get(0).getAccounts()).hasSize(1);
        assertThat(accountGroups.get(0).getTotals()).hasSize(1);
    }

    public static LegacyAccountDTO createAccount(final UUID accountId, final LegacyAccountType type,
                                        final Currency currencyCode, final BigDecimal balance, final BigDecimal availableBalance) {
        return createAccount(accountId, type, currencyCode, balance, availableBalance, null);
    }

    public static LegacyAccountDTO createAccount(final UUID accountId, final LegacyAccountType type,
                                        final Currency currencyCode, final BigDecimal balance, final BigDecimal availableBalance, final LegacyAccountStatusCode statusCode) {
        return LegacyAccountDTO.builder()
                .id(accountId)
                .name("name_" + accountId)
                .type(type)
                .currencyCode(currencyCode)
                .balance(balance)
                .availableBalance(availableBalance)
                .status(statusCode)
                .lastRefreshed(new Date())
                .hidden(false)
                .updated(new Date())
                .build();
    }
}
