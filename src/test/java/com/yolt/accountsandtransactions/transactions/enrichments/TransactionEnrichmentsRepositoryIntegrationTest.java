package com.yolt.accountsandtransactions.transactions.enrichments;

import com.yolt.accountsandtransactions.BaseIntegrationTest;
import com.yolt.accountsandtransactions.transactions.TransactionService.TransactionPrimaryKey;
import com.yolt.accountsandtransactions.transactions.enrichments.TransactionEnrichments.TransactionEnrichmentsBuilder;
import com.yolt.accountsandtransactions.transactions.enrichments.api.CategoryTransactionEnrichment;
import com.yolt.accountsandtransactions.transactions.enrichments.api.CounterpartyTransactionEnrichment;
import com.yolt.accountsandtransactions.transactions.enrichments.api.CycleTransactionEnrichment;
import com.yolt.accountsandtransactions.transactions.enrichments.api.LabelsTransactionEnrichment;
import com.yolt.accountsandtransactions.datetime.DateInterval;
import nl.ing.lovebird.extendeddata.transaction.TransactionStatus;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.time.LocalDate;
import java.time.Period;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

import static java.util.Collections.singletonList;
import static java.util.UUID.randomUUID;
import static org.assertj.core.api.Assertions.assertThat;

public class TransactionEnrichmentsRepositoryIntegrationTest extends BaseIntegrationTest {

    @Autowired
    private TransactionEnrichmentsRepository transactionEnrichmentsRepository;

    @Test
    public void shouldUpdateSMEAndPersonalCategories() {

        var userId = randomUUID();
        var accountId = randomUUID();
        var date = LocalDate.now();
        var transactionId = randomUUID();
        transactionEnrichmentsRepository.updateCategories(List.of(
                new CategoryTransactionEnrichment(userId, accountId, date, transactionId.toString(), "Transport", Optional.of("sme"), Optional.of("Transport"))));

        List<TransactionEnrichments> allEnrichments = transactionEnrichmentsRepository.getAllEnrichments(userId);
        assertThat(allEnrichments).hasSize(1);

        assertThat(allEnrichments.get(0).getEnrichmentCategoryPersonal()).isEqualTo("Transport");
        assertThat(allEnrichments.get(0).getEnrichmentCategorySME()).isEqualTo("sme");

        // assert that *absent* SME and Personal categories do *not* overwrite existing SME and Personal categories.
        transactionEnrichmentsRepository.updateCategories(List.of(
                new CategoryTransactionEnrichment(userId, accountId, date, transactionId.toString(), "Transport", Optional.empty(), Optional.empty())));

        List<TransactionEnrichments> allEnrichmentsAfterUpdateWithAbsentCategories = transactionEnrichmentsRepository.getAllEnrichments(userId);
        assertThat(allEnrichments).hasSize(1);

        assertThat(allEnrichmentsAfterUpdateWithAbsentCategories.get(0).getEnrichmentCategoryPersonal()).isEqualTo("Transport");
        assertThat(allEnrichmentsAfterUpdateWithAbsentCategories.get(0).getEnrichmentCategorySME()).isEqualTo("sme");
    }


    @Test
    public void shouldUseCounterpartyOverMerchantIfAvailable() {
        var userId = randomUUID();
        var accountId = randomUUID();
        var transactionId = "id";
        var date = LocalDate.now();

        TransactionEnrichmentsBuilder enrichmentsBuilder = TransactionEnrichments.builder()
                .userId(userId)
                .accountId(accountId)
                .date(date)
                .id(transactionId);

        transactionEnrichmentsRepository.batchUpsertOmitNullValues(
                List.of(enrichmentsBuilder.enrichmentMerchantName("fallback").build()));

        transactionEnrichmentsRepository.batchUpsertOmitNullValues(
                List.of(enrichmentsBuilder
                        .enrichmentCounterpartyName("counterparty")
                        .enrichmentCounterpartyIsKnownMerchant(true)
                        .build()
                ));
        List<TransactionEnrichments> allEnrichments = transactionEnrichmentsRepository.getAllEnrichments(userId);

        assertThat(allEnrichments).hasSize(1);
        assertThat(allEnrichments.get(0).getMerchantName()).contains("counterparty");
    }

    @Test
    public void shouldNotUseMerchantFallbackIfCounterpartyIsNotAKnownMerchant() {
        var userId = randomUUID();
        var accountId = randomUUID();
        var transactionId = "id";
        var date = LocalDate.now();

        TransactionEnrichmentsBuilder enrichmentsBuilder = TransactionEnrichments.builder()
                .userId(userId)
                .accountId(accountId)
                .date(date)
                .id(transactionId);

        transactionEnrichmentsRepository.batchUpsertOmitNullValues(
                List.of(enrichmentsBuilder.enrichmentMerchantName("fallback").build()));

        transactionEnrichmentsRepository.batchUpsertOmitNullValues(
                List.of(enrichmentsBuilder
                        .enrichmentCounterpartyName("counterparty")
                        .enrichmentCounterpartyIsKnownMerchant(false)
                        .build()
                ));
        List<TransactionEnrichments> allEnrichments = transactionEnrichmentsRepository.getAllEnrichments(userId);

        assertThat(allEnrichments).hasSize(1);
        assertThat(allEnrichments.get(0).getMerchantName()).isEmpty();
    }

    @Test
    public void shouldUseMerchantAsFallbackIfCounterpartyDoesNotExist() {
        var userId = randomUUID();
        var accountId = randomUUID();
        var transactionId = "id";
        var date = LocalDate.now();

        TransactionEnrichmentsBuilder enrichmentsBuilder = TransactionEnrichments.builder()
                .userId(userId)
                .accountId(accountId)
                .date(date)
                .id(transactionId);

        transactionEnrichmentsRepository.batchUpsertOmitNullValues(
                List.of(enrichmentsBuilder.enrichmentMerchantName("fallback").build()));

        transactionEnrichmentsRepository.batchUpsertOmitNullValues(
                List.of(enrichmentsBuilder.build()));

        List<TransactionEnrichments> allEnrichments = transactionEnrichmentsRepository.getAllEnrichments(userId);

        assertThat(allEnrichments).hasSize(1);
        assertThat(allEnrichments.get(0).getMerchantName()).contains("fallback");
    }

    @Test
    public void willNotThrowExceptionsWhenAttemptingToDeleteEnrichmentsThatDoNotExist() {
        try {
            transactionEnrichmentsRepository.deleteTransactionEnrichmentsForAccount(randomUUID(), singletonList(randomUUID()));
        } catch (Exception e) {
            // Should never happen.
            assertThat(e).isNull();
        }
    }

    @Test
    public void willNotThrowExceptionsWhenAttemptingToDeleteSpecificEnrichmentsThatDoNotExist() {
        var accountId = randomUUID();
        var userId = randomUUID();
        var transactionPrimaryKey = new TransactionPrimaryKey(userId, accountId, LocalDate.now(), "id", TransactionStatus.BOOKED);
        try {
            transactionEnrichmentsRepository.deleteSpecificEnrichments(singletonList(transactionPrimaryKey));
        } catch (Exception e) {
            // Should never happen.
            assertThat(e).isNull();
        }
    }

    @Test
    public void willDeleteAllTransactionEnrichmentsForAccount() {
        var userId = randomUUID();
        var accountId = randomUUID();
        var transactionId = "id";
        var date = LocalDate.now();
        // Persist the enrichments that should be deleted
        persistEnrichments(userId, accountId, transactionId, date);

        // Persist enrichments for another user
        var otherUserId = randomUUID();
        var otherUserAccountId = randomUUID();
        persistEnrichments(otherUserId, otherUserAccountId, "other-id", date);

        // Delete the enrichments
        transactionEnrichmentsRepository.deleteTransactionEnrichmentsForAccount(userId, List.of(accountId));

        assertThat(transactionEnrichmentsRepository.get(userId, singletonList(accountId), getDateInterval()))
                .isEmpty();

        assertEnrichmentsExistForAccount(otherUserId, otherUserAccountId, singletonList(otherUserAccountId));
    }

    @Test
    public void willDeleteAllTransactionEnrichmentsForSpecificAcc() {
        var userId = randomUUID();
        var deletedAccountId = randomUUID();
        var transactionId = "id";
        var date = LocalDate.now();
        // Persist enrichments for the user's account we want to delete
        persistEnrichments(userId, deletedAccountId, transactionId, date);

        // Persist enrichments for the user's account that should not be deleted
        var remainingAccountId = randomUUID();
        persistEnrichments(userId, remainingAccountId, transactionId, date);

        // Persist enrichments for another user
        var otherUserId = randomUUID();
        var otherUserAccountId = randomUUID();
        persistEnrichments(otherUserId, otherUserAccountId, "other-id", date);

        // Delete the enrichments
        transactionEnrichmentsRepository.deleteSpecificEnrichments(singletonList(new TransactionPrimaryKey(userId, deletedAccountId, date, transactionId, TransactionStatus.BOOKED)));

        assertEnrichmentsExistForAccount(userId, remainingAccountId, List.of(deletedAccountId, remainingAccountId));
        assertEnrichmentsExistForAccount(otherUserId, otherUserAccountId, List.of(otherUserAccountId));
    }

    private void assertEnrichmentsExistForAccount(UUID userId, UUID accountIdToMatch, List<UUID> accountsToFetch) {
        assertThat(transactionEnrichmentsRepository.get(userId, accountsToFetch, getDateInterval()))
                .hasSize(1)
                .allMatch(it -> accountIdToMatch.equals(it.getAccountId()));
    }

    private void persistEnrichments(UUID userid, UUID accountId, String transactionId, LocalDate date) {
        transactionEnrichmentsRepository.updateCounterparties(singletonList(new CounterpartyTransactionEnrichment(userid, accountId, date, transactionId, "merchant", true)));
        transactionEnrichmentsRepository.updateCategories(singletonList(new CategoryTransactionEnrichment(userid, accountId, date, transactionId, "category", Optional.of("sme"), Optional.of("personal"))));
        transactionEnrichmentsRepository.updateLabels(singletonList(new LabelsTransactionEnrichment(userid, accountId, date, transactionId, Set.of("label"))));
        transactionEnrichmentsRepository.updateEnrichmentCycleIds(singletonList(new CycleTransactionEnrichment(userid, accountId, date, transactionId, randomUUID())));
    }

    private DateInterval getDateInterval() {
        return DateInterval.of(LocalDate.now().minusDays(1L), Period.ofDays(2));
    }
}
