package com.yolt.accountsandtransactions.transactions.updates;

import com.yolt.accountsandtransactions.datascience.preprocessing.PreProcessingServiceClient;
import com.yolt.accountsandtransactions.inputprocessing.enrichments.activities.ActivityEnrichmentService;
import com.yolt.accountsandtransactions.metrics.AccountsAndTransactionMetrics;
import com.yolt.accountsandtransactions.transactions.TransactionDTO;
import com.yolt.accountsandtransactions.transactions.TransactionService;
import com.yolt.accountsandtransactions.transactions.updates.api.BulkTransactionCategoryUpdateRequestDTO;
import com.yolt.accountsandtransactions.transactions.updates.api.SeedTransactionKey;
import com.yolt.accountsandtransactions.transactions.updates.updatesession.BulkUpdateSession;
import com.yolt.accountsandtransactions.transactions.updates.updatesession.BulkUpdateSessionService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import nl.ing.lovebird.clienttokens.ClientUserToken;
import org.springframework.lang.NonNull;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

import static com.yolt.accountsandtransactions.inputprocessing.enrichments.activities.ActivityEnrichmentType.FEEDBACK_CATEGORIES;
import static java.time.temporal.ChronoUnit.SECONDS;
import static java.util.UUID.randomUUID;

@Slf4j
@Service
@RequiredArgsConstructor
public class RecategorizationService {
    private static final int DEFAULT_TIMEOUT_IN_SECONDS = 10;

    private final PreProcessingServiceClient preProcessingServiceClient;
    private final ActivityEnrichmentService activityEnrichmentService;
    private final TransactionService transactionService;
    private final BulkUpdateSessionService bulkUpdateSessionService;
    private final AccountsAndTransactionMetrics accountsAndTransactionMetrics;

    Optional<BulkUpdateSession> getUpdateSession(UUID userId, BulkTransactionCategoryUpdateRequestDTO bulkTransactionCategoryUpdateRequestDTO) {
        return bulkUpdateSessionService.find(userId, bulkTransactionCategoryUpdateRequestDTO.getUpdateSessionId());
    }

    Optional<UUID> applyFeedback(@NonNull final ClientUserToken clientUserToken,
                                 @NonNull final SeedTransactionKey tx,
                                 @NonNull final String category) {
        var activityId = startActivity(clientUserToken);

        return preProcessingServiceClient.applyCategoriesFeedbackAsync(clientUserToken, activityId, tx, category)
                .doOnSuccess(__ -> accountsAndTransactionMetrics.incrementSingleRecategorizationCount())
                .doOnError(accountsAndTransactionMetrics::incrementSingleRecategorizationFailureCount)
                .doOnError(throwable -> log.error("Re-categorization for single transaction failed", throwable))
                .then(Mono.just(activityId))
                .onErrorResume(__ -> Mono.empty())
                .blockOptional(Duration.of(DEFAULT_TIMEOUT_IN_SECONDS, SECONDS));
    }

    Optional<UUID> applyFeedbackGroups(@NonNull final ClientUserToken clientUserToken,
                                       @NonNull final BulkUpdateSession updateSession,
                                       @NonNull BulkTransactionCategoryUpdateRequestDTO bulkTransactionCategoryUpdateRequestDTO) {
        var newCategory = bulkTransactionCategoryUpdateRequestDTO.getCategory();

        return transactionService.getTransaction(updateSession.getUserId(), updateSession.getAccountId(), updateSession.getDate(), updateSession.getTransactionId())
                .flatMap(seedTx -> {
                    var activityId = startActivity(clientUserToken);
                    return applyBulkCategoryFeedback(clientUserToken, activityId, seedTx, newCategory, bulkTransactionCategoryUpdateRequestDTO.getGroupSelectors());
                });
    }

    private Optional<UUID> applyBulkCategoryFeedback(ClientUserToken clientUserToken, UUID activityId, TransactionDTO seedTx, String category, Set<String> groupSelectors) {
        return preProcessingServiceClient.applyCategoriesFeedbackGroupsAsync(clientUserToken, activityId, seedTx, groupSelectors, category)
                .doOnSuccess(__ -> accountsAndTransactionMetrics.incrementBulkRecategorizationCount())
                .doOnError(accountsAndTransactionMetrics::incrementBulkRecategorizationFailureCount)
                .doOnError(throwable -> log.error("Re-categorization for a group of transactions failed", throwable))
                .then(Mono.just(activityId))
                .onErrorResume(__ -> Mono.empty())
                .blockOptional(Duration.of(DEFAULT_TIMEOUT_IN_SECONDS, SECONDS));
    }

    private UUID startActivity(ClientUserToken clientUserToken) {
        var activityId = randomUUID();

        // Mark the start of the enrichment of this activity.
        activityEnrichmentService.startActivityEnrichment(clientUserToken, FEEDBACK_CATEGORIES, activityId);

        return activityId;
    }
}
