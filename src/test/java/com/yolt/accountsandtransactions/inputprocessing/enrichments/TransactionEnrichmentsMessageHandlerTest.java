package com.yolt.accountsandtransactions.inputprocessing.enrichments;

import com.yolt.accountsandtransactions.inputprocessing.enrichments.api.base.EnrichedTransactionKey;
import com.yolt.accountsandtransactions.inputprocessing.enrichments.api.base.EnrichmentMessageKey;
import com.yolt.accountsandtransactions.inputprocessing.enrichments.api.categories.CategoriesEnrichedTransaction;
import com.yolt.accountsandtransactions.inputprocessing.enrichments.api.categories.CategoriesEnrichmentMessage;
import com.yolt.accountsandtransactions.inputprocessing.enrichments.api.counterparties.CounterpartiesEnrichedTransaction;
import com.yolt.accountsandtransactions.inputprocessing.enrichments.api.counterparties.CounterpartiesEnrichmentMessage;
import com.yolt.accountsandtransactions.inputprocessing.enrichments.api.cycles.CyclesEnrichedTransaction;
import com.yolt.accountsandtransactions.inputprocessing.enrichments.api.cycles.CyclesEnrichmentMessage;
import com.yolt.accountsandtransactions.inputprocessing.enrichments.api.cycles.DsTransactionCycle;
import com.yolt.accountsandtransactions.inputprocessing.enrichments.api.cycles.DsTransactionCycles;
import com.yolt.accountsandtransactions.inputprocessing.enrichments.api.labels.LabelsEnrichedTransaction;
import com.yolt.accountsandtransactions.inputprocessing.enrichments.api.labels.LabelsEnrichmentMessage;
import com.yolt.accountsandtransactions.transactions.cycles.TransactionCyclesService;
import com.yolt.accountsandtransactions.transactions.enrichments.TransactionEnrichmentsService;
import com.yolt.accountsandtransactions.transactions.enrichments.api.CategoryTransactionEnrichment;
import com.yolt.accountsandtransactions.transactions.enrichments.api.CounterpartyTransactionEnrichment;
import com.yolt.accountsandtransactions.transactions.enrichments.api.CycleTransactionEnrichment;
import com.yolt.accountsandtransactions.transactions.enrichments.api.LabelsTransactionEnrichment;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.Period;
import java.time.ZonedDateTime;
import java.util.Collections;
import java.util.Currency;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

import static java.util.UUID.randomUUID;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)

public class TransactionEnrichmentsMessageHandlerTest {
    static final UUID USER_ID = randomUUID();
    static final String TRANSACTION_ID = randomUUID().toString();
    static final UUID ACCOUNT_ID = randomUUID();
    static final UUID CYCLE_ID = randomUUID();

    private TransactionEnrichmentsMessageHandler transactionEnrichmentsMessageHandler;

    @Mock
    private TransactionEnrichmentsService transactionEnrichmentsService;

    @Mock
    private TransactionCyclesService transactionCyclesService;

    @Captor
    private ArgumentCaptor<List<CategoryTransactionEnrichment>> categoryEnrichmentsCaptor;

    @Captor
    private ArgumentCaptor<List<CounterpartyTransactionEnrichment>> counterpartiesEnrichmentsCaptor;

    @Captor
    private ArgumentCaptor<List<CycleTransactionEnrichment>> cycleEnrichmentsCaptor;

    @Captor
    private ArgumentCaptor<List<LabelsTransactionEnrichment>> labelsEnrichmentsCaptor;

    @BeforeEach
    public void init() {
        transactionEnrichmentsMessageHandler = new TransactionEnrichmentsMessageHandler(transactionEnrichmentsService, transactionCyclesService);
    }

    @Test
    public void testHandleCategoriesEnrichmentMessage() {
        var enrichedTransaction = new CategoriesEnrichedTransaction(new EnrichedTransactionKey(ACCOUNT_ID, USER_ID, TRANSACTION_ID, LocalDate.now()), Collections.emptyMap(), "DRINKS", "Cafe Hesp");
        var enrichmentMessage = new CategoriesEnrichmentMessage(1, randomUUID(), ZonedDateTime.now(), new EnrichmentMessageKey(randomUUID(), randomUUID()), List.of(enrichedTransaction), 1, 1);

        transactionEnrichmentsMessageHandler.process(enrichmentMessage);

        verify(transactionEnrichmentsService).updateCategories(categoryEnrichmentsCaptor.capture());

        var enrichments = categoryEnrichmentsCaptor.getValue();
        assertThat(enrichments.size()).isEqualTo(1);
        assertThat(enrichments.get(0).getUserId()).isEqualTo(USER_ID);
        assertThat(enrichments.get(0).getAccountId()).isEqualTo(ACCOUNT_ID);
        assertThat(enrichments.get(0).getDate()).isEqualTo(LocalDate.now());
        assertThat(enrichments.get(0).getTransactionId()).isEqualTo(TRANSACTION_ID);
        assertThat(enrichments.get(0).getCategory()).isEqualTo("DRINKS");
    }

    @Test
    public void testHandleCounterPartiesEnrichmentMessage() {
        var enrichedTransaction = new CounterpartiesEnrichedTransaction(new EnrichedTransactionKey(ACCOUNT_ID, USER_ID, TRANSACTION_ID, LocalDate.now()), "Ajax", true, "Amsterdam");
        var enrichmentMessage = new CounterpartiesEnrichmentMessage(1, randomUUID(), ZonedDateTime.now(), new EnrichmentMessageKey(randomUUID(), randomUUID()), List.of(enrichedTransaction), 1, 1);

        transactionEnrichmentsMessageHandler.process(enrichmentMessage);

        verify(transactionEnrichmentsService).updateCounterParties(counterpartiesEnrichmentsCaptor.capture());

        var enrichments = counterpartiesEnrichmentsCaptor.getValue();
        assertThat(enrichments.size()).isEqualTo(1);
        assertThat(enrichments.get(0).getUserId()).isEqualTo(USER_ID);
        assertThat(enrichments.get(0).getAccountId()).isEqualTo(ACCOUNT_ID);
        assertThat(enrichments.get(0).getDate()).isEqualTo(LocalDate.now());
        assertThat(enrichments.get(0).getTransactionId()).isEqualTo(TRANSACTION_ID);
        assertThat(enrichments.get(0).getCounterparty()).isEqualTo("Ajax");
        assertThat(enrichments.get(0).isKnownMerchant()).isTrue();
    }

    @Test
    public void testHandleCyclesEnrichmentMessageCredit() {
        var enrichedTransaction = new CyclesEnrichedTransaction(new EnrichedTransactionKey(ACCOUNT_ID, USER_ID, TRANSACTION_ID, LocalDate.now()), CYCLE_ID);
        var credit = new DsTransactionCycle(CYCLE_ID, BigDecimal.TEN, Currency.getInstance("EUR"), Period.parse("P0Y0M1D"), Optional.of(new DsTransactionCycle.ModelParameters(BigDecimal.ONE, Currency.getInstance("USD"), Period.parse("P1M"))), Set.of(LocalDate.parse("2020-08-04"), LocalDate.parse("2020-06-17")), Optional.of("sticker"), true, "Ajax");
        var enrichmentMessage = new CyclesEnrichmentMessage(1, randomUUID(), ZonedDateTime.now(), new EnrichmentMessageKey(randomUUID(), randomUUID()), List.of(enrichedTransaction), new DsTransactionCycles(List.of(credit), List.of()), 1, 1);

        transactionEnrichmentsMessageHandler.process(enrichmentMessage);

        verify(transactionCyclesService).reconsile(any());
        verify(transactionEnrichmentsService).updateCycles(cycleEnrichmentsCaptor.capture());

        var enrichments = cycleEnrichmentsCaptor.getValue();
        assertThat(enrichments.size()).isEqualTo(1);
        assertThat(enrichments.get(0).getUserId()).isEqualTo(USER_ID);
        assertThat(enrichments.get(0).getAccountId()).isEqualTo(ACCOUNT_ID);
        assertThat(enrichments.get(0).getDate()).isEqualTo(LocalDate.now());
        assertThat(enrichments.get(0).getTransactionId()).isEqualTo(TRANSACTION_ID);
        assertThat(enrichments.get(0).getCycleId()).isEqualTo(CYCLE_ID);
    }

    @Test
    public void testHandleCyclesEnrichmentMessageDebit() {
        var enrichedTransaction = new CyclesEnrichedTransaction(new EnrichedTransactionKey(ACCOUNT_ID, USER_ID, TRANSACTION_ID, LocalDate.now()), CYCLE_ID);
        var debit = new DsTransactionCycle(CYCLE_ID, BigDecimal.TEN, Currency.getInstance("EUR"), Period.parse("P0Y0M1D"), Optional.of(new DsTransactionCycle.ModelParameters(BigDecimal.ONE, Currency.getInstance("USD"), Period.parse("P1M"))), Set.of(LocalDate.parse("2020-08-04"), LocalDate.parse("2020-06-17")), Optional.of("sticker"), true, "Ajax");
        var enrichmentMessage = new CyclesEnrichmentMessage(1, randomUUID(), ZonedDateTime.now(), new EnrichmentMessageKey(randomUUID(), randomUUID()), List.of(enrichedTransaction), new DsTransactionCycles(List.of(), List.of(debit)), 1, 1);

        transactionEnrichmentsMessageHandler.process(enrichmentMessage);

        verify(transactionCyclesService).reconsile(any());
        verify(transactionEnrichmentsService).updateCycles(cycleEnrichmentsCaptor.capture());

        var enrichments = cycleEnrichmentsCaptor.getValue();
        assertThat(enrichments.size()).isEqualTo(1);
        assertThat(enrichments.get(0).getUserId()).isEqualTo(USER_ID);
        assertThat(enrichments.get(0).getAccountId()).isEqualTo(ACCOUNT_ID);
        assertThat(enrichments.get(0).getDate()).isEqualTo(LocalDate.now());
        assertThat(enrichments.get(0).getTransactionId()).isEqualTo(TRANSACTION_ID);
        assertThat(enrichments.get(0).getCycleId()).isEqualTo(CYCLE_ID);
    }

    @Test
    public void testHandleCyclesEnrichmentMessageNotProvided() {
        var enrichedTransaction = new CyclesEnrichedTransaction(new EnrichedTransactionKey(ACCOUNT_ID, USER_ID, TRANSACTION_ID, LocalDate.now()), CYCLE_ID);
        var enrichmentMessage = new CyclesEnrichmentMessage(1, randomUUID(), ZonedDateTime.now(), new EnrichmentMessageKey(randomUUID(), randomUUID()), List.of(enrichedTransaction), new DsTransactionCycles(List.of(), List.of()), 1, 1);

        transactionEnrichmentsMessageHandler.process(enrichmentMessage);

        verify(transactionCyclesService, never()).saveBatch(any());
        verify(transactionEnrichmentsService).updateCycles(cycleEnrichmentsCaptor.capture());
        assertThat(cycleEnrichmentsCaptor.getValue()).isEmpty();
    }

    @Test
    public void testHandleLabelsEnrichmentMessage() {
        var enrichedTransaction = new LabelsEnrichedTransaction(new EnrichedTransactionKey(ACCOUNT_ID, USER_ID, TRANSACTION_ID, LocalDate.now()), Set.of("Avery"));
        var enrichmentMessage = new LabelsEnrichmentMessage(1, randomUUID(), ZonedDateTime.now(), new EnrichmentMessageKey(randomUUID(), randomUUID()), List.of(enrichedTransaction), 1, 1);

        transactionEnrichmentsMessageHandler.process(enrichmentMessage);

        verify(transactionEnrichmentsService).updateLabels(labelsEnrichmentsCaptor.capture());

        var enrichments = labelsEnrichmentsCaptor.getValue();
        assertThat(enrichments.size()).isEqualTo(1);
        assertThat(enrichments.get(0).getUserId()).isEqualTo(USER_ID);
        assertThat(enrichments.get(0).getAccountId()).isEqualTo(ACCOUNT_ID);
        assertThat(enrichments.get(0).getDate()).isEqualTo(LocalDate.now());
        assertThat(enrichments.get(0).getTransactionId()).isEqualTo(TRANSACTION_ID);
        assertThat(enrichments.get(0).getLabels()).containsAll(Set.of("Avery"));
    }
}