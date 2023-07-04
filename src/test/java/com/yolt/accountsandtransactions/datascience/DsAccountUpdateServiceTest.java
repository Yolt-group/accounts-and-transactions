package com.yolt.accountsandtransactions.datascience;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.mapping.Mapper;
import com.yolt.accountsandtransactions.BaseIntegrationTest;
import com.yolt.accountsandtransactions.accounts.Account;
import com.yolt.accountsandtransactions.inputprocessing.AccountFromProviders;
import lombok.Setter;
import lombok.experimental.Accessors;
import nl.ing.lovebird.extendeddata.account.ExtendedAccountDTO;
import nl.ing.lovebird.extendeddata.account.ExternalCashAccountType;
import nl.ing.lovebird.extendeddata.account.Status;
import nl.ing.lovebird.extendeddata.account.UsageType;
import nl.ing.lovebird.extendeddata.common.CurrencyCode;
import nl.ing.lovebird.providerdomain.*;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.math.BigDecimal;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Map;
import java.util.UUID;

import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;
import static com.google.common.collect.Lists.newArrayList;
import static com.yolt.accountsandtransactions.TestAccountBuilder.builder;
import static com.yolt.accountsandtransactions.datascience.DsAccountUpdateServiceTest.TestAccountsAndTransactionsDTOBuilder.testAccountsAndTransactionsDTOBuilder;
import static com.yolt.accountsandtransactions.datascience.DsAccountUpdateServiceTest.TestProviderCreditCardDTOBuilder.testProviderCreditCardDTOBuilder;
import static nl.ing.lovebird.extendeddata.transaction.TransactionStatus.BOOKED;
import static nl.ing.lovebird.providerdomain.AccountType.CURRENT_ACCOUNT;
import static org.assertj.core.api.Assertions.assertThat;

public class DsAccountUpdateServiceTest extends BaseIntegrationTest {
    @Autowired
    private DsAccountsCurrentService dsAccountsCurrentService;
    @Autowired
    private DsCreditCardsCurrentService dsCreditCardsCurrentService;

    private Mapper<DsAccountCurrent> dsAccountCurrentMapper;
    private Mapper<DSCreditCardCurrent> creditCardMapperCurrent;

    @Override
    protected void setup() {
        dsAccountCurrentMapper = mappingManager.mapper(DsAccountCurrent.class, dsKeyspace);
        creditCardMapperCurrent = mappingManager.mapper(DSCreditCardCurrent.class, dsKeyspace);
    }

    @Test
    public void shouldOverwriteAccountTypeAndNameWithDataFromNextUpdate() {
        // Given
        // Simulating the case when name and type are the same
        final Account account = builder().build();
        final AccountFromProviders accountsAndTransactionsAccountDTO = testAccountsAndTransactionsDTOBuilder()
                .build();

        assertThat(account.getType().name()).isEqualTo(accountsAndTransactionsAccountDTO.getYoltAccountType().name());
        assertThat(account.getName()).isEqualTo(accountsAndTransactionsAccountDTO.getName());

        // When
        dsAccountsCurrentService.saveDsAccountCurrent(account, accountsAndTransactionsAccountDTO);

        // Then
        DsAccountCurrent actual = dsAccountCurrentMapper.get(accountsAndTransactionsAccountDTO.getYoltUserId(), account.getId());
        assertThat(actual.getAccountType()).isEqualTo(accountsAndTransactionsAccountDTO.getYoltAccountType().name());
        assertThat(actual.getName()).isEqualTo(accountsAndTransactionsAccountDTO.getName());
        assertThat(actual.getExtendedAccount()).isNotNull();

        // Given
        // Simulating the case when name and type are different
        final AccountFromProviders nextAccountsAndTransactionsAccountDTO = testAccountsAndTransactionsDTOBuilder()
                .yoltAccountType(AccountType.SAVINGS_ACCOUNT)
                .accountName("Account Name 2")
                .build();

        assertThat(account.getType().name()).isNotEqualTo(nextAccountsAndTransactionsAccountDTO.getYoltAccountType().name());
        assertThat(account.getName()).isNotEqualTo(nextAccountsAndTransactionsAccountDTO.getName());

        // When
        dsAccountsCurrentService.saveDsAccountCurrent(account, nextAccountsAndTransactionsAccountDTO);

        // Then
        actual = dsAccountCurrentMapper.get(nextAccountsAndTransactionsAccountDTO.getYoltUserId(), account.getId());
        assertThat(actual.getAccountType()).isEqualTo(nextAccountsAndTransactionsAccountDTO.getYoltAccountType().name());
        assertThat(actual.getName()).isEqualTo(nextAccountsAndTransactionsAccountDTO.getName());
    }

    @Test
    public void shouldNotErrorWhenDTODoesNotContainCreditCardData() {
        // Given
        final Account account = builder().build();
        final AccountFromProviders accountsAndTransactionsAccountDTO = testAccountsAndTransactionsDTOBuilder()
                .yoltAccountType(CURRENT_ACCOUNT)
                .creditCardData(null)
                .build();

        // When
        dsAccountsCurrentService.saveDsAccountCurrent(account, accountsAndTransactionsAccountDTO);

        // Then
        final DSCreditCardCurrent dsCreditCardCurrent = creditCardMapperCurrent.map(session.execute(QueryBuilder
                .select()
                .from(dsKeyspace, DSCreditCardCurrent.TABLE_NAME)
                .where(eq(DSCreditCardCurrent.USER_ID_COLUMN, accountsAndTransactionsAccountDTO.getYoltUserId())))).one();

        assertThat(dsCreditCardCurrent).isNull();
    }

    @Test
    public void shouldNotErrorWhenCreditCardDatesAreNull() {
        // Given
        final Account account = builder().build();
        final AccountFromProviders accountsAndTransactionsAccountDTO = testAccountsAndTransactionsDTOBuilder()
                .yoltAccountType(CURRENT_ACCOUNT)
                .creditCardData(testProviderCreditCardDTOBuilder()
                        .dueDate(null)
                        .lastPaymentDate(null)
                        .build())
                .build();

        // When
        dsCreditCardsCurrentService.saveDsCreditCardCurrent(account, accountsAndTransactionsAccountDTO);

        // Then
        ResultSet execute = session.execute(QueryBuilder.select()
                .from(dsKeyspace, DSCreditCardCurrent.TABLE_NAME)
                .where(eq(DSCreditCardCurrent.USER_ID_COLUMN, accountsAndTransactionsAccountDTO.getYoltUserId())));
        final DSCreditCardCurrent dsCreditCardCurrent = creditCardMapperCurrent.map(execute).one();

        assertThat(dsCreditCardCurrent.getDueDate()).isNull();
        assertThat(dsCreditCardCurrent.getLastPaymentDate()).isNull();
    }


    @Setter
    @Accessors(fluent = true, chain = true)
    static class TestAccountsAndTransactionsDTOBuilder {

        private ArrayList<ProviderTransactionDTO> transactions = newArrayList(
                ProviderTransactionDTO.builder()
                        .amount(new BigDecimal("1"))
                        .category(YoltCategory.CASH)
                        .dateTime(ZonedDateTime.of(2011, 1, 1, 1, 1, 0, 1, ZoneId.of("UTC")))
                        .description("description")
                        .merchant("merchant")
                        .status(BOOKED)
                        .type(ProviderTransactionType.DEBIT)
                        .build()
        );
        private UUID yoltUserId = new UUID(0, 1);
        private UUID yoltUserSiteId = new UUID(0, 1);
        private String accountId = new UUID(1, 1).toString();
        private BigDecimal availableBalance = new BigDecimal("1");
        private String accountName = "Account Name 1";
        private CurrencyCode gbp = CurrencyCode.GBP;
        private BigDecimal currentBalance = new BigDecimal(2);
        private ZonedDateTime lastRefreshed = ZonedDateTime.of(2011, 1, 1, 1, 1, 0, 1, ZoneId.of("UTC"));
        private String provider = "provider";
        private AccountType yoltAccountType = AccountType.CURRENT_ACCOUNT;
        private UUID yoltSiteId = new UUID(1, 3);
        private ProviderCreditCardDTO creditCardData = testProviderCreditCardDTOBuilder().build();

        private TestAccountsAndTransactionsDTOBuilder() {
        }

        static TestAccountsAndTransactionsDTOBuilder testAccountsAndTransactionsDTOBuilder() {
            return new TestAccountsAndTransactionsDTOBuilder();
        }

        public AccountFromProviders build() {
            ExtendedAccountDTO extendedAccountDTO = ExtendedAccountDTO.builder()
                    .currency(CurrencyCode.GBP)
                    .name("My Account")
                    .bic("MYBICCODE")
                    .cashAccountType(ExternalCashAccountType.CLEARING)
                    .product("Woo Hoo Product")
                    .status(Status.ENABLED)
                    .usage(UsageType.PRIVATE)
                    .build();
            return AccountFromProviders.accountsFromProvidersBuilder()
                    .yoltUserId(new UUID(0, 1))
                    .transactions(transactions)
                    .yoltUserSiteId(yoltUserSiteId)
                    .creditCardData(creditCardData)
                    .accountId(accountId)
                    .availableBalance(availableBalance)
                    .name(accountName)
                    .currency(gbp)
                    .currentBalance(currentBalance)
                    .lastRefreshed(lastRefreshed)
                    .provider(provider)
                    .yoltAccountType(yoltAccountType)
                    .yoltSiteId(yoltSiteId)
                    .yoltUserId(yoltUserId)
                    .extendedAccount(extendedAccountDTO)
                    .bankSpecific(Map.of("key", "value"))
                    .build();
        }
    }


    @Setter
    @Accessors(fluent = true, chain = true)
    static class TestProviderCreditCardDTOBuilder {

        private ZonedDateTime lastPaymentDate = ZonedDateTime.of(2011, 1, 1, 1, 1, 0, 1, ZoneId.of("UTC"));
        private ZonedDateTime dueDate = ZonedDateTime.of(2012, 2, 2, 2, 2, 0, 1, ZoneId.of("UTC"));
        private BigDecimal apr = BigDecimal.valueOf(1.1);
        private BigDecimal availableCreditAmount = new BigDecimal("2.2");
        private BigDecimal cashApr = BigDecimal.valueOf(3.3);
        private BigDecimal cashLimitAmount = new BigDecimal("3.3");
        private BigDecimal dueAmount = new BigDecimal("4.4");
        private BigDecimal lastPaymentAmount = new BigDecimal("5.5");
        private BigDecimal minPaymentAmount = new BigDecimal("6.6");
        private BigDecimal newChargesAmount = new BigDecimal("7.7");
        private BigDecimal runningBalanceAmount = new BigDecimal("8.8");
        private BigDecimal totalCreditLineAmount = new BigDecimal("9.9");

        public static TestProviderCreditCardDTOBuilder testProviderCreditCardDTOBuilder() {
            return new TestProviderCreditCardDTOBuilder();
        }

        public ProviderCreditCardDTO build() {
            return ProviderCreditCardDTO.builder()
                    .lastPaymentDate(lastPaymentDate)
                    .dueDate(dueDate)
                    .apr(apr)
                    .availableCreditAmount(availableCreditAmount)
                    .cashApr(cashApr)
                    .cashLimitAmount(cashLimitAmount)
                    .dueAmount(dueAmount)
                    .lastPaymentAmount(lastPaymentAmount)
                    .minPaymentAmount(minPaymentAmount)
                    .newChargesAmount(newChargesAmount)
                    .runningBalanceAmount(runningBalanceAmount)
                    .totalCreditLineAmount(totalCreditLineAmount)
                    .build();
        }
    }


}
