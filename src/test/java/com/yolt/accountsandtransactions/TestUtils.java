package com.yolt.accountsandtransactions;

import com.datastax.driver.mapping.Mapper;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.util.StdDateFormat;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.fasterxml.jackson.module.paramnames.ParameterNamesModule;
import com.yolt.accountsandtransactions.accounts.Account;
import com.yolt.accountsandtransactions.datascience.DsTransaction;
import com.yolt.accountsandtransactions.datascience.ServiceUtil;
import com.yolt.accountsandtransactions.inputprocessing.AccountFromProviders;
import com.yolt.accountsandtransactions.inputprocessing.AccountsAndTransactionsRequestDTO;
import nl.ing.lovebird.extendeddata.common.BalanceAmountDTO;
import nl.ing.lovebird.extendeddata.common.CurrencyCode;
import nl.ing.lovebird.extendeddata.transaction.ExtendedTransactionDTO;
import nl.ing.lovebird.extendeddata.transaction.TransactionStatus;
import nl.ing.lovebird.providerdomain.*;
import org.springframework.boot.jackson.JsonComponentModule;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.stream.Collectors;

public class TestUtils {
    private static final ObjectMapper objectMapper = new ObjectMapper()
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
            .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
            .disable(DeserializationFeature.ADJUST_DATES_TO_CONTEXT_TIME_ZONE)
            .setDateFormat(new StdDateFormat().withColonInTimeZone(false))
            .registerModule(new JavaTimeModule())
            .registerModule(new Jdk8Module())
            .registerModule(new ParameterNamesModule())
            .registerModule(new JsonComponentModule());

    public static final String EXTENDED_TRANSACTION_JSON = "{\"transactionId\":\"TX1234\", \"entryReference\":\"ENT1234\", " +
            "\"creditorName\":\"JCVD\", \"ultimateCreditor\":\"STVSGL\", \"debtorName\":\"JHNRMBO\", \"purposeCode\":\"WHOKNOWS\", \"bankTransactionCode\":\"YACHA\"}";

    public static List<String> readFileLines(final String path) {
        final InputStream cardsJsonStream = TestUtils.class.getResourceAsStream(path);
        return new BufferedReader(new InputStreamReader(cardsJsonStream, StandardCharsets.UTF_8))
                .lines()
                .collect(Collectors.toList());
    }

    public static AccountsAndTransactionsRequestDTO ingestionRequestSuccessMessageWithTransactions(UUID userId,
                                                                                                   UUID activityId,
                                                                                                   UUID userSiteId,
                                                                                                   String externalAccountId,
                                                                                                   String provider,
                                                                                                   UUID siteId,
                                                                                                   List<ProviderTransactionDTO> transactions) {
        return new AccountsAndTransactionsRequestDTO(
                activityId,
                Collections.singletonList(
                        AccountFromProviders.accountsFromProvidersBuilder()
                                .yoltUserId(userId)
                                .yoltUserSiteId(userSiteId)
                                .yoltSiteId(siteId)
                                .yoltAccountType(AccountType.CREDIT_CARD)
                                .lastRefreshed(ZonedDateTime.now())
                                .availableBalance(new BigDecimal("100.0"))
                                .currentBalance(new BigDecimal("1"))
                                .currency(CurrencyCode.EUR)
                                .accountId(externalAccountId)
                                .name("account X")
                                .provider(provider)
                                .bankSpecific(Map.of("key", "value"))
                                .transactions(transactions)
                                .creditCardData(ProviderCreditCardDTO.builder()
                                        .cashApr(new BigDecimal("1.1"))
                                        .dueAmount(new BigDecimal("2.2"))
                                        .dueDate(ZonedDateTime.of(2017, 11, 1, 0, 0, 0, 0, ZoneId.of("UTC")))
                                        .availableCreditAmount(new BigDecimal("9000.0"))
                                        .runningBalanceAmount(new BigDecimal("4356.34"))
                                        .minPaymentAmount(new BigDecimal("3.33"))
                                        .newChargesAmount(new BigDecimal("4.44"))
                                        .lastPaymentAmount(new BigDecimal("55.5"))
                                        .lastPaymentDate(ZonedDateTime.of(2017, 11, 1, 0, 0, 0, 0, ZoneId.systemDefault()))
                                        .totalCreditLineAmount(new BigDecimal("6.66"))
                                        .cashLimitAmount(new BigDecimal("7.77"))
                                        .apr(new BigDecimal("8.88"))
                                        .build())
                                .build()),
                userSiteId,
                siteId
        );
    }

    public static AccountsAndTransactionsRequestDTO ingestionRequestSuccessMessage(UUID userId, UUID activityId, UUID userSiteId, String externalAccountId, String provider, UUID siteId) throws IOException {
        List<ProviderTransactionDTO> transactions = new ArrayList<>();
        transactions.add(ProviderTransactionDTO.builder()
                .externalId("12345x")
                .dateTime(ZonedDateTime.of(2017, 10, 4, 0, 0, 0, 0, ZoneId.systemDefault()))
                .amount(new BigDecimal("20.0"))
                .status(TransactionStatus.BOOKED)
                .type(ProviderTransactionType.CREDIT)
                .description("Transaction XYZ")
                .category(YoltCategory.INCOME)
                .merchant("merchantX")
                .bankSpecific(Map.of("key", "value"))
                .extendedTransaction(
                        ExtendedTransactionDTO.builder()
                                .transactionAmount(BalanceAmountDTO.builder().amount(new BigDecimal("20")).build())
                                .entryReference("ENT1234")
                                .creditorName("JCVD")
                                .ultimateCreditor("STVSGL")
                                .debtorName("JHNRMBO")
                                .purposeCode("WHOKNOWS")
                                .bankTransactionCode("YACHA")
                                .build()
                ).build());
        transactions.add(ProviderTransactionDTO.builder()
                .externalId("12345y")
                .dateTime(ZonedDateTime.of(2017, 10, 3, 0, 0, 0, 0, ZoneId.systemDefault()))
                .amount(new BigDecimal("10.0"))
                .status(TransactionStatus.PENDING)
                .type(ProviderTransactionType.DEBIT)
                .description("DIRECT DEBIT Energies")
                .category(YoltCategory.LEISURE)
                .merchant("merchantY")
                .bankSpecific(Map.of("key", "value"))
                .extendedTransaction(
                        ExtendedTransactionDTO.builder()
                                .transactionAmount(BalanceAmountDTO.builder().amount(new BigDecimal("-10")).build())
                                .entryReference("ENT1234")
                                .creditorName("JCVD")
                                .ultimateCreditor("STVSGL")
                                .debtorName("JHNRMBO")
                                .purposeCode("WHOKNOWS")
                                .bankTransactionCode("YACHA")
                                .build()
                )
                .build());
        transactions.add(ProviderTransactionDTO.builder()
                .externalId("12345z")
                .dateTime(ZonedDateTime.of(2017, 10, 3, 0, 0, 0, 0, ZoneId.systemDefault()))
                .amount(new BigDecimal("10.0"))
                .status(TransactionStatus.PENDING)
                .type(ProviderTransactionType.DEBIT)
                .description("DIRECT DEBIT Energies")
                .category(YoltCategory.LEISURE)
                .merchant("merchantY")
                .extendedTransaction(
                        ExtendedTransactionDTO.builder()
                                .transactionAmount(BalanceAmountDTO.builder().amount(new BigDecimal("-10")).build())
                                .entryReference("ENT1234")
                                .creditorName("JCVD")
                                .ultimateCreditor("STVSGL")
                                .debtorName("JHNRMBO")
                                .purposeCode("WHOKNOWS")
                                .bankTransactionCode("YACHA")
                                .build()
                )
                .build());
        return ingestionRequestSuccessMessageWithTransactions(userId, activityId, userSiteId, externalAccountId, provider, siteId, transactions);
    }

    public static DsTransaction createTransaction(
            final UUID userId,
            final int pending,
            final String date,
            final UUID accountId,
            final String transactionId,
            final BigDecimal amount,
            final String currency,
            final String category,
            final String merchant) {
        return DsTransaction.builder()
                .userId(userId)
                .pending(pending)
                .date(date)
                .accountId(accountId)
                .transactionId(transactionId)
                .currency(currency)
                .amount(amount)
                .mappedCategory(category)
                .description("Descr_" + date + transactionId)
                .extendedTransaction(StandardCharsets.UTF_8.encode(EXTENDED_TRANSACTION_JSON))
                .build();
    }

    public static DsTransaction saveTransaction(
            final UUID userId,
            final int pending,
            final String date,
            final UUID accountId,
            final String transactionId,
            final BigDecimal amount,
            final String currency,
            final String category,
            final String merchant,
            final Mapper<DsTransaction> mapper) {

        final DsTransaction transaction = createTransaction(userId, pending, date, accountId, transactionId, amount, currency, category, merchant);

        mapper.save(transaction);

        return transaction;
    }

    public static Account toAccount(final AccountFromProviders ingestionAccount) {

        return TestAccountBuilder.builder()
                .name(ingestionAccount.getName())
                .type(ingestionAccount.getYoltAccountType())
                .externalId(ingestionAccount.getAccountId())
                .lastDataFetchTime(ingestionAccount.getLastRefreshed().toInstant())
                .currency(ingestionAccount.getCurrency())
                .balance(ingestionAccount.getCurrentBalance())
                .userSiteId(ingestionAccount.getYoltUserSiteId())
                .siteId(ingestionAccount.getYoltSiteId())
                .hidden(false)
                .status(Account.Status.ENABLED)
                .build();
    }

    public static DsTransaction toTransaction(final UUID userId, final UUID accountId, final String currencyCode, final ProviderTransactionDTO pt) {
        final DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ISO_OFFSET_DATE_TIME;
        final String transactionDate = pt.getDateTime().format(dateTimeFormatter).substring(0, 10);

        return DsTransaction.builder()
                .userId(userId)
                .pending(mapPendingType(pt.getStatus()))
                .accountId(accountId)
                .date(transactionDate)
                .transactionType(mapTransactionType(pt.getType()))
                .amount(pt.getAmount())
                .currency(currencyCode)
                .mappedCategory(pt.getCategory().getValue())
                .description(pt.getDescription())
                .extendedTransaction(ServiceUtil.asByteBuffer(objectMapper, pt.getExtendedTransaction()))
                .build();
    }

    private static int mapPendingType(final TransactionStatus transactionStatus) {
        switch (transactionStatus) {
            case BOOKED:
                return 1;
            case PENDING:
                return 2;
        }
        throw new IllegalArgumentException("Unknown ProviderTransactionStatus: " + transactionStatus);
    }

    private static String mapTransactionType(final ProviderTransactionType transactionType) {
        switch (transactionType) {
            case DEBIT:
                return "debit";
            case CREDIT:
                return "credit";
        }
        throw new IllegalArgumentException("Unknown ProviderTransactionType: " + transactionType);
    }
}
