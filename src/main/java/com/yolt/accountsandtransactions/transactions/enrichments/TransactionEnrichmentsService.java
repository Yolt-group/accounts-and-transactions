package com.yolt.accountsandtransactions.transactions.enrichments;

import com.yolt.accountsandtransactions.transactions.TransactionService.TransactionPrimaryKey;
import com.yolt.accountsandtransactions.transactions.enrichments.api.CategoryTransactionEnrichment;
import com.yolt.accountsandtransactions.transactions.enrichments.api.CounterpartyTransactionEnrichment;
import com.yolt.accountsandtransactions.transactions.enrichments.api.CycleTransactionEnrichment;
import com.yolt.accountsandtransactions.transactions.enrichments.api.LabelsTransactionEnrichment;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import com.yolt.accountsandtransactions.datetime.DateInterval;
import org.springframework.stereotype.Service;

import java.time.LocalDate;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

/**
 * Enrichment information for {@link com.yolt.accountsandtransactions.transactions.Transaction} are not stored inside that class/table but instead are
 * stored in {@link TransactionEnrichments}. Both tables have the same keys. This separation is made to be able to handle enrichment-messages from DS
 * as fast and efficient as possible. The downside is that upon retrieval of transactions the associated (if present) {@link TransactionEnrichments} need to be
 * determined and joined with the {@link com.yolt.accountsandtransactions.transactions.Transaction} programmatically.
 */
@RequiredArgsConstructor
@Service
@Slf4j
public class TransactionEnrichmentsService {
    private final TransactionEnrichmentsRepository transactionEnrichmentsRepository;

    public void updateCategories(List<CategoryTransactionEnrichment> categoryTransactionEnrichments) {
        transactionEnrichmentsRepository.updateCategories(categoryTransactionEnrichments);
    }

    public void updateLabels(List<LabelsTransactionEnrichment> labelsTransactionEnrichments) {
        transactionEnrichmentsRepository.updateLabels(labelsTransactionEnrichments);
    }

    public void updateCounterParties(List<CounterpartyTransactionEnrichment> counterpartyTransactionEnrichments) {
        transactionEnrichmentsRepository.updateCounterparties(counterpartyTransactionEnrichments);
    }

    public void updateCycles(List<CycleTransactionEnrichment> cycleTransactionEnrichments) {
        transactionEnrichmentsRepository.updateEnrichmentCycleIds(cycleTransactionEnrichments);
    }

    public void deleteAllEnrichmentsForAccounts(UUID userId, List<UUID> accountIds) {
        transactionEnrichmentsRepository.deleteTransactionEnrichmentsForAccount(userId, accountIds);
    }

    public void deleteSpecificEnrichments(final @NonNull List<TransactionPrimaryKey> enrichmentsToDelete) {
        transactionEnrichmentsRepository.deleteSpecificEnrichments(enrichmentsToDelete);
    }

    public Optional<TransactionEnrichments> getTransactionEnrichments(UUID userId, UUID accountId, LocalDate date, String transactionId) {
        return transactionEnrichmentsRepository.get(userId, accountId, date, transactionId);
    }

    public List<TransactionEnrichments> getTransactionEnrichments(UUID userId, List<UUID> accountIds, DateInterval interval) {
        return transactionEnrichmentsRepository.get(userId, accountIds, interval);
    }
}
