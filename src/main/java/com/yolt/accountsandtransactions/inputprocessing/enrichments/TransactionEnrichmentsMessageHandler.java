package com.yolt.accountsandtransactions.inputprocessing.enrichments;

import com.yolt.accountsandtransactions.inputprocessing.enrichments.api.categories.CategoriesEnrichmentMessage;
import com.yolt.accountsandtransactions.inputprocessing.enrichments.api.counterparties.CounterpartiesEnrichmentMessage;
import com.yolt.accountsandtransactions.inputprocessing.enrichments.api.cycles.CyclesEnrichedTransaction;
import com.yolt.accountsandtransactions.inputprocessing.enrichments.api.cycles.CyclesEnrichmentMessage;
import com.yolt.accountsandtransactions.inputprocessing.enrichments.api.cycles.DsTransactionCycles;
import com.yolt.accountsandtransactions.inputprocessing.enrichments.api.labels.LabelsEnrichmentMessage;
import com.yolt.accountsandtransactions.transactions.cycles.TransactionCycle;
import com.yolt.accountsandtransactions.transactions.cycles.TransactionCyclesService;
import com.yolt.accountsandtransactions.transactions.enrichments.TransactionEnrichmentsService;
import com.yolt.accountsandtransactions.transactions.enrichments.api.*;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.time.LocalDate;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.yolt.accountsandtransactions.transactions.cycles.TransactionCyclesService.calculateChangeSet;
import static java.util.Collections.emptyList;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.toList;

/**
 * Process EnrichmentMessages. Each specific EnrichmentMessage is mapped to update the relevant attributes of the transaction involved.
 */
@Service
@RequiredArgsConstructor
@Slf4j
public class TransactionEnrichmentsMessageHandler {
    private final TransactionEnrichmentsService transactionEnrichmentsService;
    private final TransactionCyclesService transactionCyclesService;

    Set<TransactionEnrichment> process(CategoriesEnrichmentMessage enrichmentMessage) {
        var updates = enrichmentMessage.getTransactions().stream()
                .map(tx -> new CategoryTransactionEnrichment(
                        tx.getKey().getUserId(),
                        tx.getKey().getAccountId(),
                        tx.getKey().getDate(),
                        tx.getKey().getTransactionId(),
                        tx.getCategory(),
                        tx.getSMECategory(),
                        tx.getPersonalCategory()))
                .collect(toList());
        log.info("Writing categories for {} transactions, min(date) = {}", updates.size(), updates.stream().map(TransactionEnrichment::getDate).min(LocalDate::compareTo));
        transactionEnrichmentsService.updateCategories(updates);
        return new HashSet<>(updates);
    }

    Set<TransactionEnrichment> process(LabelsEnrichmentMessage enrichmentMessage) {
        var updates = enrichmentMessage.getTransactions().stream()
                .map(tx -> new LabelsTransactionEnrichment(
                        tx.getKey().getUserId(),
                        tx.getKey().getAccountId(),
                        tx.getKey().getDate(),
                        tx.getKey().getTransactionId(),
                        tx.getLabels()))
                .collect(toList());
        log.info("Writing labels for {} transactions, min(date) = {}", updates.size(), updates.stream().map(TransactionEnrichment::getDate).min(LocalDate::compareTo));
        transactionEnrichmentsService.updateLabels(updates);
        return new HashSet<>(updates);
    }

    Set<TransactionEnrichment> process(CounterpartiesEnrichmentMessage enrichmentMessage) {
        var updates = enrichmentMessage.getTransactions().stream()
                // only include transaction enrichment which actually have a counterparty.
                .filter(counterpartiesEnrichedTransaction -> counterpartiesEnrichedTransaction.getCounterparty() != null)
                .map(tx -> new CounterpartyTransactionEnrichment(
                        tx.getKey().getUserId(),
                        tx.getKey().getAccountId(),
                        tx.getKey().getDate(),
                        tx.getKey().getTransactionId(),
                        tx.getCounterparty(),
                        tx.isMerchant()))
                .collect(toList());
        transactionEnrichmentsService.updateCounterParties(updates);
        log.info("Writing counterparties for {} transactions, min(date) = {}", updates.size(), updates.stream().map(TransactionEnrichment::getDate).min(LocalDate::compareTo));
        return new HashSet<>(updates);
    }

    Set<TransactionEnrichment> process(final @NonNull CyclesEnrichmentMessage enrichmentMessage) {
        var userId = enrichmentMessage.getMessageKey().getUserId();

        // all transaction-cycles as received from datascience
        var datascienceTransactionCycles = createTransactionCycles(userId, enrichmentMessage.getCycles());

        // all transaction-cycles we have in our own database
        var localTransactionCycles = transactionCyclesService.getAll(userId);

        // calculate the change-set and reconsile the changes in the database
        transactionCyclesService.reconsile(calculateChangeSet(localTransactionCycles, datascienceTransactionCycles));

        var transactionsByCycleId = groupTransactionsByCycleId(enrichmentMessage.getTransactions());

        var updates = datascienceTransactionCycles
                .stream()
                .map(transactionCycle -> transactionsByCycleId.getOrDefault(transactionCycle.getCycleId(), emptyList()))
                .flatMap(Collection::stream)
                .map(transaction -> new CycleTransactionEnrichment(
                        transaction.getKey().getUserId(),
                        transaction.getKey().getAccountId(),
                        transaction.getKey().getDate(),
                        transaction.getKey().getTransactionId(),
                        transaction.getCycleId()))
                .collect(toList());
        log.info("Writing cycles for {} transactions, min(date) = {}", updates.size(), updates.stream().map(TransactionEnrichment::getDate).min(LocalDate::compareTo));
        transactionEnrichmentsService.updateCycles(updates);
        return new HashSet<>(updates);
    }

    private Map<UUID, List<CyclesEnrichedTransaction>> groupTransactionsByCycleId(List<CyclesEnrichedTransaction> transactions) {
        return transactions.stream()
                .collect(groupingBy(CyclesEnrichedTransaction::getCycleId, toList()));
    }

    private static List<TransactionCycle> createTransactionCycles(final @NonNull UUID userId, final @NonNull DsTransactionCycles dsTransactionCycles) {
        return Stream.concat(dsTransactionCycles.getCredits().stream(), dsTransactionCycles.getDebits().stream())
                .map(cycle -> TransactionCycle.fromDatascienceTransactionCycle(userId, cycle))
                .collect(Collectors.toList());
    }
}
