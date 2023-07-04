package com.yolt.accountsandtransactions.transactions.updates;

import com.yolt.accountsandtransactions.datascience.counterparties.client.dto.DsCounterpartiesFeedbackDTO;
import com.yolt.accountsandtransactions.datascience.counterparties.client.dto.DsCounterpartiesFeedbackGroupsDTO;
import com.yolt.accountsandtransactions.datascience.counterparties.client.dto.DsCounterpartiesTransactionKeyDTO;
import com.yolt.accountsandtransactions.datascience.preprocessing.PreProcessingServiceClient;
import com.yolt.accountsandtransactions.inputprocessing.enrichments.activities.ActivityEnrichmentService;
import com.yolt.accountsandtransactions.metrics.AccountsAndTransactionMetrics;
import com.yolt.accountsandtransactions.transactions.TransactionService;
import com.yolt.accountsandtransactions.transactions.updates.api.BulkTransactionCounterpartyUpdateRequestDTO;
import com.yolt.accountsandtransactions.transactions.updates.api.TransactionCounterpartyUpdateRequestDTO;
import com.yolt.accountsandtransactions.transactions.updates.updatesession.BulkUpdateSession;
import com.yolt.accountsandtransactions.transactions.updates.updatesession.BulkUpdateSessionService;
import lombok.EqualsAndHashCode;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import nl.ing.lovebird.clienttokens.ClientUserToken;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.Optional;
import java.util.UUID;

import static com.yolt.accountsandtransactions.inputprocessing.enrichments.activities.ActivityEnrichmentType.FEEDBACK_COUNTERPARTIES;
import static java.time.temporal.ChronoUnit.SECONDS;
import static java.util.UUID.randomUUID;

@Service
@RequiredArgsConstructor
@Slf4j
public class CounterpartyAdjustmentService {
    private static final int DEFAULT_TIMEOUT_IN_SECONDS = 10;

    private final PreProcessingServiceClient preprocessingServiceClient;
    private final ActivityEnrichmentService activityEnrichmentService;
    private final TransactionService transactionService;
    private final BulkUpdateSessionService bulkUpdateSessionService;
    private final AccountsAndTransactionMetrics accountsAndTransactionMetrics;

    Optional<BulkUpdateSession> getUpdateSession(UUID userId, BulkTransactionCounterpartyUpdateRequestDTO bulkTransactionCounterpartyUpdateRequestDTO) {
        return bulkUpdateSessionService.find(userId, bulkTransactionCounterpartyUpdateRequestDTO.getUpdateSessionId());
    }

    public Optional<CounterpartyFeedbackActivity> updateCounterpartyOnTransaction(final ClientUserToken clientUserToken, final TransactionCounterpartyUpdateRequestDTO transactionCounterpartyUpdateRequest) {
        var activityId = startActivity(clientUserToken);

        var counterpartyUpdate = DsCounterpartiesFeedbackDTO.builder()
                .transactionKey(DsCounterpartiesTransactionKeyDTO.builder()
                        .accountId(transactionCounterpartyUpdateRequest.getAccountId())
                        .transactionType("REGULAR")
                        .date(transactionCounterpartyUpdateRequest.getDate())
                        .transactionId(transactionCounterpartyUpdateRequest.getId())
                        .build())
                .counterpartyName(transactionCounterpartyUpdateRequest.getCounterpartyName())
                .build();

        return preprocessingServiceClient.updateCounterparty(
                clientUserToken,
                activityId,
                counterpartyUpdate)
                .doOnSuccess(__ -> accountsAndTransactionMetrics.incrementSingleCounterpartyCount())
                .doOnError(accountsAndTransactionMetrics::incrementSingleCounterpartyFailureCount)
                .map(response -> new CounterpartyFeedbackActivity(activityId, response.getCounterpartyName(), response.getKnownMerchant()))
                .onErrorResume(__ -> Mono.empty())
                .blockOptional(Duration.of(DEFAULT_TIMEOUT_IN_SECONDS, SECONDS));
    }

    public Optional<CounterpartyFeedbackActivity> updateSimilarTransactions(ClientUserToken clientUserToken,
                                                                            BulkUpdateSession bulkUpdateSession,
                                                                            BulkTransactionCounterpartyUpdateRequestDTO bulkTransactionCounterpartyUpdateRequestDTO) {
        var dsCounterpartiesFeedbackGroupsDTO = DsCounterpartiesFeedbackGroupsDTO.builder()
                .counterpartyName(bulkTransactionCounterpartyUpdateRequestDTO.getCounterpartyName())
                .groupSelectors(bulkTransactionCounterpartyUpdateRequestDTO.getGroupSelectors())
                .build();

        return transactionService.getTransaction(bulkUpdateSession.getUserId(), bulkUpdateSession.getAccountId(), bulkUpdateSession.getDate(), bulkUpdateSession.getTransactionId())
                .flatMap(seedTransaction -> {
                    var activityId = startActivity(clientUserToken);
                    return updateCounterpartyForTransactions(clientUserToken, activityId, dsCounterpartiesFeedbackGroupsDTO);
                });
    }

    private Optional<CounterpartyFeedbackActivity> updateCounterpartyForTransactions(ClientUserToken clientUserToken, UUID activityId, DsCounterpartiesFeedbackGroupsDTO dsCounterpartiesFeedbackGroupsDTO) {
        return preprocessingServiceClient.updateMultipleCounterparties(clientUserToken, activityId, dsCounterpartiesFeedbackGroupsDTO)
                .doOnSuccess(__ -> accountsAndTransactionMetrics.incrementBulkCounterpartyCount())
                .doOnError(accountsAndTransactionMetrics::incrementBulkCounterpartyFailureCount)
                .map(response -> new CounterpartyFeedbackActivity(activityId, response.getCounterpartyName(), response.isKnownMerchant()))
                .onErrorResume(__ -> Mono.empty())
                .blockOptional(Duration.of(DEFAULT_TIMEOUT_IN_SECONDS, SECONDS));
    }

    /**
     * Will start an activity enrichment event to associate with the updating of counterparties.
     */
    private UUID startActivity(ClientUserToken clientUserToken) {
        var activityId = randomUUID();

        // Mark the start of the enrichment of this activity.
        activityEnrichmentService.startActivityEnrichment(clientUserToken, FEEDBACK_COUNTERPARTIES, activityId);

        return activityId;
    }


    @ToString
    @EqualsAndHashCode
    @RequiredArgsConstructor
    public static class CounterpartyFeedbackActivity {
        @NonNull
        public final UUID activityId;
        @NonNull
        public final String counterPartyName;

        public final boolean knownMerchant;

    }
}
