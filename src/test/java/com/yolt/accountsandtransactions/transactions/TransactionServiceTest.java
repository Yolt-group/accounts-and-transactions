package com.yolt.accountsandtransactions.transactions;

import com.yolt.accountsandtransactions.TestBuilders;
import com.yolt.accountsandtransactions.inputprocessing.ProviderTransactionWithId;
import com.yolt.accountsandtransactions.metrics.AccountsAndTransactionMetrics;
import com.yolt.accountsandtransactions.offloading.OffloadService;
import com.yolt.accountsandtransactions.transactions.TransactionService.AccountIdentifiable;
import com.yolt.accountsandtransactions.transactions.cycles.TransactionCyclesService;
import com.yolt.accountsandtransactions.transactions.enrichments.TransactionEnrichments;
import com.yolt.accountsandtransactions.transactions.enrichments.TransactionEnrichmentsService;
import com.yolt.accountsandtransactions.datetime.DateInterval;
import nl.ing.lovebird.extendeddata.common.BalanceAmountDTO;
import nl.ing.lovebird.extendeddata.common.CurrencyCode;
import nl.ing.lovebird.extendeddata.transaction.ExtendedTransactionDTO;
import nl.ing.lovebird.providerdomain.ProviderTransactionDTO;
import org.junit.jupiter.api.Test;

import java.time.*;
import java.util.Collections;
import java.util.List;

import static java.math.BigDecimal.ONE;
import static java.math.BigDecimal.TEN;
import static java.time.Clock.systemUTC;
import static java.util.UUID.randomUUID;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TransactionServiceTest {

    @Test
    public void testMappingToTransaction() {
        var transaction = TransactionService.map(new ProviderTransactionWithId(ProviderTransactionDTO.builder()
                .dateTime(ZonedDateTime.parse("2020-06-17T09:30:00+02:00"))
                .amount(TEN)
                .build(), "1"), new AccountIdentifiable(randomUUID(), randomUUID(), CurrencyCode.EUR), true, systemUTC(), null);

        assertThat(transaction.getTimestamp()).isEqualTo(Instant.parse("2020-06-17T07:30:00Z"));
        assertThat(transaction.getTimeZone()).isEqualTo("+02:00");
    }

    @Test
    public void testMappingToTransactionWithFallbackTimeInNonInsertMode() {
        Clock pointInTimeClock = Clock.fixed(Instant.parse("2021-07-26T00:10:56.733Z"), ZoneId.of("UTC"));
        var transaction = TransactionService.map(new ProviderTransactionWithId(ProviderTransactionDTO.builder()
                .dateTime(ZonedDateTime.parse("2020-06-17T09:30:00+02:00"))
                .amount(TEN)
                .build(), "1"), new AccountIdentifiable(randomUUID(), randomUUID(), CurrencyCode.EUR), false, systemUTC(), pointInTimeClock.instant());

        assertThat(transaction.getCreatedAtOrEPOCH()).isEqualTo(Instant.parse("2021-07-26T00:10:56.733Z"));
        assertThat(transaction.getCreatedAt()).isEqualTo(Instant.parse("2021-07-26T00:10:56.733Z"));
    }

    @Test
    public void testMappingJavaTimeTypesTruncatedToMilliseconds() {
        Clock pointInTimeClock = Clock.fixed(Instant.parse("2021-07-26T00:10:56.733600Z"), ZoneId.of("UTC"));

        var transaction = TransactionService.map(new ProviderTransactionWithId(ProviderTransactionDTO.builder()
                .dateTime(ZonedDateTime.parse("2021-07-26T00:10:56.733600Z"))
                .amount(TEN)
                .build(), "1"), new AccountIdentifiable(randomUUID(), randomUUID(), CurrencyCode.EUR), true, pointInTimeClock, null);

        assertThat(transaction.getTimestamp()).isEqualTo(Instant.parse("2021-07-26T00:10:56.733Z"));
        assertThat(transaction.getTimeZone()).isEqualTo("Z");
        assertThat(transaction.getLastUpdatedTime()).isEqualTo(Instant.parse("2021-07-26T00:10:56.733Z"));
        assertThat(transaction.getCreatedAtOrEPOCH()).isEqualTo(Instant.parse("2021-07-26T00:10:56.733Z"));
    }

    @Test
    public void testMappingToTransaction_withExtendedTimezone() {
        var transaction = TransactionService.map(new ProviderTransactionWithId(ProviderTransactionDTO.builder()
                .dateTime(ZonedDateTime.parse("2020-06-17T09:30:00+02:00[Europe/Amsterdam]"))
                .amount(TEN)
                .build(), "1"), new AccountIdentifiable(randomUUID(), randomUUID(), CurrencyCode.EUR), true, systemUTC(), null);

        assertThat(transaction.getTimestamp()).isEqualTo(Instant.parse("2020-06-17T07:30:00Z"));
        assertThat(transaction.getTimeZone()).isEqualTo("+02:00");
    }

    @Test
    public void testMappingToTransactionDto() {
        var transactionDTO = TransactionService.map(Transaction.builder()
                .timestamp(Instant.parse("2020-06-17T11:30:00Z"))
                .timeZone("+02:00")
                .amount(TEN)
                .build(), new TransactionEnrichments(
                randomUUID(),
                randomUUID(),
                LocalDate.now(),
                "id",
                "categoryPersonal",
                "categorySME",
                "merchantName",
                "counterpartyName",
                true,
                randomUUID(),
                Collections.emptySet()
        ));

        assertThat(transactionDTO.getTimestamp()).isEqualTo(ZonedDateTime.parse("2020-06-17T13:30:00+02:00"));
        assertThat(transactionDTO.getEnrichment().getCategory()).isEqualTo("categoryPersonal");
        assertThat(transactionDTO.getEnrichment().getCategorySME()).isEqualTo("categorySME");
        assertThat(transactionDTO.getEnrichment().getCounterparty().getName()).isEqualTo("counterpartyName");
        assertThat(transactionDTO.getEnrichment().getCounterparty().isKnownMerchant()).isTrue();
        // Backward compat
        assertThat(transactionDTO.getEnrichment().getCounterparty().getName())
                .isEqualTo(transactionDTO.getEnrichment().getMerchant().getName());
    }

    @Test
    public void testMappingToTransactionDtoRemittanceInformation() {
        var transactionDTO = TransactionService.map(Transaction.builder()
                .timestamp(Instant.parse("2020-06-17T11:30:00Z"))
                .timeZone("+02:00")
                .amount(TEN)
                .remittanceInformationStructured("structured information")
                .remittanceInformationUnstructured("unstructured information")
                .build(), null);

        assertThat(transactionDTO.getRemittanceInformationStructured()).isEqualTo("structured information");
        assertThat(transactionDTO.getRemittanceInformationUnstructured()).isEqualTo("unstructured information");
    }

    @Test
    public void testMappingToTransactionDto_NoTimeZone() {
        var transactionDTO = TransactionService.map(Transaction.builder()
                .timestamp(Instant.parse("2020-06-17T11:30:00Z"))
                .amount(TEN)
                .build(), null);

        assertThat(transactionDTO.getTimestamp()).isEqualTo(ZonedDateTime.parse("2020-06-17T11:30:00-00:00"));
    }

    @Test
    public void testMappingToTransactionDtoCreditorAndDebtorInformation() {
        var transactionDTO = TransactionService.map(Transaction.builder()
                .timestamp(Instant.parse("2020-06-17T11:30:00Z"))
                .timeZone("+02:00")
                .amount(TEN)
                .creditorName("Creditor")
                .creditorBban("c-bban")
                .creditorIban("c-iban")
                .creditorPan("c-pan")
                .creditorSortCodeAccountNumber("c-sortcode")
                .creditorMaskedPan("c-maskedpan")
                .debtorName("Debtor")
                .debtorBban("d-bban")
                .debtorIban("d-iban")
                .debtorPan("d-pan")
                .debtorSortCodeAccountNumber("d-sortcode")
                .debtorMaskedPan("d-maskedpan")
                .build(), null);

        assertThat(transactionDTO.getCreditor().getName()).isEqualTo("Creditor");
        assertThat(transactionDTO.getCreditor().getAccountReferences().getBban()).isEqualTo("c-bban");
        assertThat(transactionDTO.getCreditor().getAccountReferences().getIban()).isEqualTo("c-iban");
        assertThat(transactionDTO.getCreditor().getAccountReferences().getPan()).isEqualTo("c-pan");
        assertThat(transactionDTO.getCreditor().getAccountReferences().getSortCodeAccountNumber()).isEqualTo("c-sortcode");
        assertThat(transactionDTO.getCreditor().getAccountReferences().getMaskedPan()).isEqualTo("c-maskedpan");
        assertThat(transactionDTO.getDebtor().getName()).isEqualTo("Debtor");
        assertThat(transactionDTO.getDebtor().getAccountReferences().getBban()).isEqualTo("d-bban");
        assertThat(transactionDTO.getDebtor().getAccountReferences().getIban()).isEqualTo("d-iban");
        assertThat(transactionDTO.getDebtor().getAccountReferences().getPan()).isEqualTo("d-pan");
        assertThat(transactionDTO.getDebtor().getAccountReferences().getSortCodeAccountNumber()).isEqualTo("d-sortcode");
        assertThat(transactionDTO.getDebtor().getAccountReferences().getMaskedPan()).isEqualTo("d-maskedpan");
    }

    @Test
    public void testMappingToTransactionOriginalAmount() {
        var transaction = TransactionService.map(new ProviderTransactionWithId(ProviderTransactionDTO.builder()
                .dateTime(ZonedDateTime.parse("2020-06-17T09:30:00+02:00[Europe/Amsterdam]"))
                .amount(ONE)
                .extendedTransaction(ExtendedTransactionDTO.builder()
                        .originalAmount(new BalanceAmountDTO(CurrencyCode.USD, TEN))
                        .build())
                .build(), "1"), new AccountIdentifiable(randomUUID(), randomUUID(), CurrencyCode.EUR), true, systemUTC(), null);

        assertThat(transaction.getOriginalAmountAmount()).isEqualTo(TEN);
        assertThat(transaction.getOriginalAmountCurrency()).isEqualTo(CurrencyCode.USD);
    }

    @Test
    public void testBookingAndValueDate() {
        Clock pointInTimeClock = Clock.fixed(Instant.now(), ZoneId.of("UTC"));

        var transaction = TransactionService.map(new ProviderTransactionWithId(ProviderTransactionDTO.builder()
                .dateTime(ZonedDateTime.parse("2020-06-17T09:30:00+02:00[Europe/Amsterdam]"))
                .amount(ONE)
                .extendedTransaction(ExtendedTransactionDTO.builder()
                        .bookingDate(ZonedDateTime.now(pointInTimeClock).plusDays(5))
                        .valueDate(ZonedDateTime.now(pointInTimeClock).plusDays(10))
                        .build())
                .build(), "1"), new AccountIdentifiable(randomUUID(), randomUUID(), CurrencyCode.EUR), true, pointInTimeClock, null);

        assertThat(transaction.getBookingDate()).isEqualTo(LocalDate.now(pointInTimeClock).plusDays(5));
        assertThat(transaction.getValueDate()).isEqualTo(LocalDate.now(pointInTimeClock).plusDays(10));
    }

    @Test
    public void testBookingAndValueDateAbsent() {
        var transaction = TransactionService.map(new ProviderTransactionWithId(ProviderTransactionDTO.builder()
                .dateTime(ZonedDateTime.parse("2020-06-17T09:30:00+02:00[Europe/Amsterdam]"))
                .amount(ONE)
                .extendedTransaction(ExtendedTransactionDTO.builder().build())
                .build(), "1"), new AccountIdentifiable(randomUUID(), randomUUID(), CurrencyCode.EUR), true, systemUTC(), null);

        assertThat(transaction.getBookingDate()).isNull();
        assertThat(transaction.getValueDate()).isNull();
    }

    @Test
    public void testRemittanceInformation() {
        var transaction = TransactionService.map(new ProviderTransactionWithId(ProviderTransactionDTO.builder()
                .dateTime(ZonedDateTime.parse("2020-06-17T09:30:00+02:00[Europe/Amsterdam]"))
                .amount(ONE)
                .extendedTransaction(ExtendedTransactionDTO.builder()
                        .remittanceInformationStructured("remittance structured")
                        .remittanceInformationUnstructured("remittance unstructured")
                        .build())
                .build(), "1"), new AccountIdentifiable(randomUUID(), randomUUID(), CurrencyCode.EUR), true, systemUTC(), null);

        assertThat(transaction.getRemittanceInformationStructured()).isEqualTo("remittance structured");
        assertThat(transaction.getRemittanceInformationUnstructured()).isEqualTo("remittance unstructured");
    }

    @Test
    public void testMappingToTransactionWithoutAmount() {
        assertThatThrownBy(() -> {
            TransactionService.map(new ProviderTransactionWithId(ProviderTransactionDTO.builder()
                    .dateTime(ZonedDateTime.parse("2020-06-17T09:30:00+02:00"))
                    .build(), "1"), new AccountIdentifiable(randomUUID(), randomUUID(), CurrencyCode.EUR), true, systemUTC(), null);
        }).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void shouldFilterBackfilledTransactionsByDefault() {

        var repository = mock(TransactionRepository.class);
        when(repository.get(any(), any(), any(DateInterval.class), isNull(), eq(100))).thenReturn(
                new TransactionsPage(List.of(TestBuilders.createTransactionTemplate().toBuilder()
                                .id("regular")
                                .fillType(Transaction.FillType.REGULAR)
                                .build(),
                        TestBuilders.createTransactionTemplate().toBuilder()
                                .id("backfilled")
                                .fillType(Transaction.FillType.BACKFILLED)
                                .build()), null));


        var transactionService = new TransactionService(
                repository,
                mock(TransactionEnrichmentsService.class),
                mock(TransactionCyclesService.class),
                mock(OffloadService.class),
                Clock.systemUTC(),
                mock(AccountsAndTransactionMetrics.class)
        );

        var transactions = transactionService.getTransactions(
                randomUUID(),
                List.of(randomUUID()),
                DateInterval.of(LocalDate.now(), Period.ofDays(7)), null, 100);

        assertThat(transactions.getTransactions()).hasSize(1);
        assertThat(transactions.getTransactions())
                .allSatisfy(transactionDTO -> assertThat(transactionDTO.getId()).isEqualTo("regular"));
    }

}
