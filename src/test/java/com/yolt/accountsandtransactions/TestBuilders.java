package com.yolt.accountsandtransactions;

import com.yolt.accountsandtransactions.accounts.Account;
import com.yolt.accountsandtransactions.accounts.Balance;
import com.yolt.accountsandtransactions.inputprocessing.ProviderTransactionWithId;
import com.yolt.accountsandtransactions.transactions.Transaction;
import com.yolt.accountsandtransactions.transactions.TransactionService;
import com.yolt.accountsandtransactions.transactions.TransactionService.TransactionPrimaryKey;
import com.yolt.accountsandtransactions.transactions.cycles.CycleType;
import com.yolt.accountsandtransactions.transactions.cycles.TransactionCycle;
import com.yolt.accountsandtransactions.transactions.cycles.TransactionCycle.TransactionCycleBuilder;
import nl.ing.lovebird.extendeddata.account.BalanceType;
import nl.ing.lovebird.extendeddata.account.UsageType;
import nl.ing.lovebird.extendeddata.common.CurrencyCode;
import nl.ing.lovebird.extendeddata.transaction.ExtendedTransactionDTO;
import nl.ing.lovebird.extendeddata.transaction.TransactionStatus;
import nl.ing.lovebird.providerdomain.AccountType;
import nl.ing.lovebird.providerdomain.ProviderTransactionDTO;
import nl.ing.lovebird.providerdomain.ProviderTransactionType;
import nl.ing.lovebird.providerdomain.YoltCategory;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static java.math.BigDecimal.ONE;
import static java.math.BigDecimal.TEN;
import static java.time.Clock.systemUTC;
import static java.util.UUID.randomUUID;

public class TestBuilders {

    /**
     * Note: only use this method for testing mappings between domain objects
     * <p/>
     * The transactions which is returned by this method has all the fields filled. Technically this is not possible
     * e.a. a transaction cannot have a debtor and creditor, but its convenient when testing mapping from 1 object to another.
     *
     * @return a transaction which has a value for *all* fields.
     */
    public static Transaction createTransactionTemplate(TransactionPrimaryKey primaryKey) {
        return Transaction.builder()
                .id(primaryKey.getId())
                .accountId(primaryKey.getAccountId())
                .userId(primaryKey.getUserId())
                .date(primaryKey.getDate())
                .amount(BigDecimal.TEN)
                .bankTransactionCode("bank-transaction-code")
                .bookingDate(LocalDate.EPOCH)
                .bankSpecific(Map.of("key", "value"))
                .createdAt(Instant.EPOCH)
                .creditorBban("creditor-bban")
                .creditorIban("creditor-iban")
                .creditorMaskedPan("creditor-masked-pan")
                .creditorName("creditor-name")
                .creditorPan("creditor-pan")
                .creditorSortCodeAccountNumber("creditor-sortcode")
                .currency(CurrencyCode.EUR)
                .debtorBban("debtor-bban")
                .debtorIban("debtor-iban")
                .debtorMaskedPan("debtor-masked-pan")
                .debtorName("debtor-name")
                .debtorPan("debtor-pan")
                .debtorSortCodeAccountNumber("debtor-sort")
                .endToEndId("end-to-end")
                .exchangeRateCurrencyFrom(CurrencyCode.EUR)
                .exchangeRateCurrencyTo(CurrencyCode.GBP)
                .exchangeRateRate(BigDecimal.ONE)
                .externalId("external-id")
                .lastUpdatedTime(Instant.EPOCH)
                .originalAmountAmount(BigDecimal.TEN)
                .originalAmountCurrency(CurrencyCode.EUR)
                .originalCategory(YoltCategory.BILLS)
                .originalMerchantName("merchant")
                .purposeCode("purpose-code")
                .remittanceInformationStructured("structured")
                .remittanceInformationUnstructured("unstructured")
                .status(TransactionStatus.BOOKED)
                .timestamp(Instant.EPOCH)
                .timeZone("UTC")
                .valueDate(LocalDate.EPOCH)
                .build();
    }

    public static Transaction createTransactionTemplate() {
        return createTransactionTemplate(new TransactionPrimaryKey(randomUUID(), randomUUID(), LocalDate.EPOCH, "T" + randomUUID(), TransactionStatus.BOOKED));
    }

    public static Account createAllFieldsRandomAccount(final UUID userId, final UUID accountId) {
        return Account.builder()
                .userId(userId)
                .id(accountId)
                .externalId("external-id")
                .type(AccountType.CURRENT_ACCOUNT)
                .currency(CurrencyCode.EUR)
                .balance(BigDecimal.TEN)
                .status(Account.Status.ENABLED)
                .name("account-1")
                .product("product")
                .accountHolder("account-holder")
                .iban("iban")
                .maskedPan("masped-pan")
                .pan("pan")
                .bban("bban")
                .sortCodeAccountNumber("sort-code")
                .interestRate(BigDecimal.ONE)
                .userSiteId(randomUUID())
                .siteId(randomUUID())
                .creditLimit(BigDecimal.TEN)
                .availableCredit(BigDecimal.TEN)
                .linkedAccount("linked-account")
                .bic("bic")
                .isMoneyPotOf("money-pot")
                .bankSpecific(Map.of("Key", "value"))
                .hidden(false)
                .usage(UsageType.CORPORATE)
                .lastDataFetchTime(Instant.EPOCH)
                .balances(List.of(new Balance(
                        BalanceType.INTERIM_BOOKED,
                        CurrencyCode.EUR,
                        BigDecimal.TEN,
                        Instant.EPOCH
                )))
                .createdAt(Instant.parse("2018-11-30T18:35:24.00Z"))
                .build();
    }

    public static Transaction createTransactionWithId(String id, ProviderTransactionDTO pt) {
        return TransactionService.map(
                new ProviderTransactionWithId(pt, id),
                new TransactionService.AccountIdentifiable(
                        randomUUID(),
                        randomUUID(),
                        CurrencyCode.EUR),
                true,
                systemUTC(),
                Instant.now(systemUTC()));
    }

    public static Transaction createTransaction(ProviderTransactionDTO pt) {
        return createTransactionWithId(randomUUID().toString(), pt);
    }

    public static List<Transaction> bulkCreateTransactions(final int max,
                                                                     final Supplier<TransactionPrimaryKey> transactionIdSupplier,
                                                                     final BiFunction<Transaction.TransactionBuilder, Integer, Transaction.TransactionBuilder> customizer,
                                                                     final Function<Transaction, Transaction> saveFunction) {
        return IntStream.range(0, max)
                .mapToObj(i -> customizer.apply(createTransactionTemplate(transactionIdSupplier.get()).toBuilder(), i).build())
                .map(saveFunction)
                .collect(Collectors.toList());
    }

    public static TransactionCycle createTransactionCycle(final Supplier<UUID> userIdSupplier,
                                                          final BiFunction<TransactionCycleBuilder, Integer, TransactionCycleBuilder> customizer,
                                                          final Function<TransactionCycle, TransactionCycle> saveFunction) {

        return bulkCreateTransactionCycles(1, userIdSupplier, customizer, saveFunction).stream().findFirst().orElseThrow(IllegalStateException::new);
    }

    public static List<TransactionCycle> bulkCreateTransactionCycles(final int max,
                                                                     final Supplier<UUID> userIdSupplier,
                                                                     final BiFunction<TransactionCycleBuilder, Integer, TransactionCycleBuilder> customizer,
                                                                     final Function<TransactionCycle, TransactionCycle> saveFunction) {
        return IntStream.range(0, max)
                .mapToObj(i -> customizer.apply(createRandomTransactionCycle(userIdSupplier.get()).toBuilder(), i).build())
                .map(saveFunction)
                .collect(Collectors.toList());
    }

    public static TransactionCycle createRandomTransactionCycle(final UUID userId) {
        return TransactionCycle.builder()
                .userId(userId)
                .cycleId(randomUUID())
                .amount(TEN)
                .counterparty("CounterParty")
                .currency("EUR")
                .cycleType(CycleType.CREDITS)
                .label("Avery")
                .period("DOT")
                .predictedOccurrences(null)
                .subscription(true)
                .modelAmount(ONE)
                .modelCurrency("EUR")
                .modelPeriod("DOT")
                .expired(false)
                .build();
    }

    public static ProviderTransactionDTO transactionWithExternalIdAmountAndDate(
            final String externalId,
            final BigDecimal amount,
            final ZonedDateTime dateTime) {

        return ProviderTransactionDTO.builder()
                .externalId(externalId)
                .dateTime(dateTime)
                .amount(amount.abs())
                .status(TransactionStatus.BOOKED)
                .type(amount.signum() == 1 ? ProviderTransactionType.CREDIT : ProviderTransactionType.DEBIT)
                .category(YoltCategory.GENERAL)
                .description("description")
                .extendedTransaction(ExtendedTransactionDTO.builder()
                        .creditorName("creditor-name")
                        .debtorName("debtor-name")
                        .bookingDate(dateTime)
                        .build())
                .build();
    }
}
