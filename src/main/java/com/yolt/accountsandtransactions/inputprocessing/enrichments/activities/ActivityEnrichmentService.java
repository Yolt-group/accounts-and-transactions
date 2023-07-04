package com.yolt.accountsandtransactions.inputprocessing.enrichments.activities;

import brave.baggage.BaggageField;
import com.google.common.annotations.VisibleForTesting;
import com.yolt.accountsandtransactions.inputprocessing.enrichments.TransactionEnrichmentsFinishedActivityEventProducer;
import com.yolt.accountsandtransactions.inputprocessing.enrichments.UsersClient;
import com.yolt.accountsandtransactions.inputprocessing.enrichments.api.EnrichmentMessageType;
import com.yolt.accountsandtransactions.inputprocessing.enrichments.api.base.EnrichmentMessage;
import com.yolt.accountsandtransactions.metrics.AccountsAndTransactionMetrics;
import com.yolt.accountsandtransactions.transactions.enrichments.api.TransactionEnrichment;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import nl.ing.lovebird.clienttokens.ClientToken;
import nl.ing.lovebird.clienttokens.ClientUserToken;
import org.jose4j.jwt.MalformedClaimException;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Clock;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZonedDateTime;
import java.util.*;

import static java.lang.String.format;
import static java.time.temporal.ChronoUnit.MINUTES;
import static java.util.stream.Collectors.*;
import static java.util.stream.Stream.concat;
import static nl.ing.lovebird.clienttokens.constants.ClientTokenConstants.*;

/**
 * This service keeps track of the Enrichments (categories, counterparties, etc. etc.) that can be associated with an Activity.
 * The set of Enrichments that are applicable for the client are stored in the ClientToken (claims). When all applicable Enrichments
 * have arrived, the Activity can be marked as finished.
 * <p>
 * An activity is eligible to be marked as "timed-out" if the applicable Enrichments are not registered within a specified time-window.
 * The method {@link #markTimedOutActivities} can be used to find and mark these activities as timed-out and should be called periodically
 * (e.g. with the <code>batch-trigger</code>).
 */
@Service
@Slf4j
@RequiredArgsConstructor
public class ActivityEnrichmentService {
    private static final Set<EnrichmentMessageType> ENRICHMENT_TYPES = Set.of(EnrichmentMessageType.values());

    @Value("${yolt.accounts-and-transactions.enrichments.datascience.timeout:10}")
    private final long timedOutInMinutes;
    private final ActivityEnrichmentRepository activityEnrichmentRepository;
    private final TransactionEnrichmentsFinishedActivityEventProducer transactionEnrichmentsFinishedActivityEventProducer;
    private final AccountsAndTransactionMetrics accountsAndTransactionMetrics;
    private final Clock clock;
    private final BaggageField userIdField;
    private final BaggageField clientIdField;
    private final UsersClient usersClient;

    /*
     * Mark the start of the Enrichment of an Activity. Register the id's of the UserSites that are affected by this Activity. The INITIAL (only for internal use)
     * Enrichment is used as a marker and also contains the checksum that is used to determine if the ActivityEnrichment has finished.
     */
    public void startActivityEnrichment(ClientUserToken clientUserToken, ActivityEnrichmentType activityEnrichmentType, UUID activityId) {

        // We should store the start event if we expect to also consume *ANY* enrichment message type.
        if (hasClaim(clientUserToken, CLAIM_DATA_ENRICHMENT_CATEGORIZATION) ||
                hasClaim(clientUserToken, CLAIM_DATA_ENRICHMENT_MERCHANT_RECOGNITION) ||
                hasClaim(clientUserToken, CLAIM_DATA_ENRICHMENT_CYCLE_DETECTION) ||
                hasClaim(clientUserToken, CLAIM_DATA_ENRICHMENT_LABELS)) {
            var overallChecksum = computeCheckSumForFinishedEvent(clientUserToken, activityEnrichmentType);

            // Currently, this 'start' method is called multiple times: For each user site in a refresh.
            // Can be fixed once SM and A&T merge.  For now, we only 'start' a new enrichment if it's not there yet.
            // Otherwise, we rely on the initial start.
            Optional<ActivityEnrichment> byId = activityEnrichmentRepository.findById(activityId);
            if (byId.isEmpty()) {
                ActivityEnrichment activityEnrichment = new ActivityEnrichment(activityId, Instant.now(clock), activityEnrichmentType, clientUserToken.getUserIdClaim(), 0, Collections.emptySet());
                activityEnrichmentRepository.save(activityEnrichment);
                log.info("Saving activity-enrichment {} (activity {}, {} {})", activityEnrichmentType, activityId, clientTokenSettingsLogString(clientUserToken), ", checksum: " + overallChecksum);
            }
        } else {
            log.info("Not saving activity-enrichment {} (activity {}, {}). DS not enabled for this client.", activityEnrichmentType, activityId, clientTokenSettingsLogString(clientUserToken));
        }
    }

    @Transactional
    public void processActivityEnrichment(EnrichmentMessage enrichmentMessage, Set<TransactionEnrichment> affectedTransactions, ClientUserToken clientUserToken) {
        if (!isRelevant(enrichmentMessage, clientUserToken)) {
            return;
        }

        var oldestTransactionDatesForAccounts = determineOldestTransactionDatesForAccounts(affectedTransactions);

        Optional<ActivityEnrichment> optionalActivityEnrichment = activityEnrichmentRepository.findById(enrichmentMessage.getActivityId());
        if (optionalActivityEnrichment.isEmpty()) {
            log.warn("No INITIAL enrichment found for activity {}, client settings: ({})", enrichmentMessage.getActivityId(), clientTokenSettingsLogString(clientUserToken));
            return;
        }
        ActivityEnrichment activityEnrichment = optionalActivityEnrichment.get();

        // register this enrichment message
        processAccountsAndOldestTransactionsOnEnrichment(activityEnrichment, oldestTransactionDatesForAccounts);
        if (enrichmentMessage.isLastPage()) {
            activityEnrichment.setChecksum(activityEnrichment.getChecksum() + (int) enrichmentMessage.getDomain().checksumValue());
            if (activityEnrichment.getChecksum() == computeCheckSumForFinishedEvent(clientUserToken, activityEnrichment.getEnrichmentType())) {
                log.info("Activity enrichment completed (activity {})", activityEnrichment.getActivityId());
                Map<UUID, LocalDate> oldestTransactionChangeByAccountId = activityEnrichment.getActivityEnrichmentAccounts().stream()
                        .collect(toMap(ActivityEnrichmentAccount::getAccountId, ActivityEnrichmentAccount::getOldestTransactionTs));
                transactionEnrichmentsFinishedActivityEventProducer.markAsCompleted(activityEnrichment.getActivityId(), oldestTransactionChangeByAccountId, ZonedDateTime.now(clock), clientUserToken);
                activityEnrichmentRepository.delete(activityEnrichment);
                return;
            }
        }
        log.info("Saving activity-enrichment {} activity {}, while processing page {} of {} current checksum {}", activityEnrichment.getEnrichmentType(), activityEnrichment.getActivityId(),
                enrichmentMessage.getMessageIndex() != null ? enrichmentMessage.getMessageIndex() + 1 : "null",
                enrichmentMessage.getMessageTotal(),
                activityEnrichment.getChecksum());
        activityEnrichmentRepository.save(activityEnrichment);
    }

    public void processAccountsAndOldestTransactionsOnEnrichment(ActivityEnrichment activityEnrichment, Map<UUID, LocalDate> oldestTransactionDatesForAccounts) {
        oldestTransactionDatesForAccounts.forEach((accountId, oldestTrxDate) -> {
            Optional<ActivityEnrichmentAccount> existingEntry = activityEnrichment.getActivityEnrichmentAccounts().stream()
                    .filter(it -> it.getAccountId().equals(accountId))
                    .findFirst();

            if (existingEntry.isPresent()) {
                if (existingEntry.get().getOldestTransactionTs().isAfter(oldestTrxDate)) {
                    existingEntry.get().setOldestTransactionTs(oldestTrxDate);
                }
            } else {
                activityEnrichment.addActivityEnrichmentAccount(new ActivityEnrichmentAccount(activityEnrichment.getActivityId(), accountId, oldestTrxDate));
            }
        });
    }

    /*
     * The client-token contains claims for the enrichment types (labels, counterparties, etc. etc.) that are active (or not). Absence of a claim implicitly de-activates the
     * enrichment type. The set of active enrichment types map to relevant enrichment-message types. The incoming enrichment message is checked against the set of relevant
     * enrichment-message types to determine if it is relevant or not.
     */
    public static boolean isRelevant(EnrichmentMessage enrichmentMessage, ClientToken clientToken) {
        return relevantMessageTypes(clientToken).contains(enrichmentMessage.getDomain());
    }

    @VisibleForTesting
    public Optional<ActivityEnrichment> find(UUID activityId) {
        return activityEnrichmentRepository.findById(activityId);
    }

    /*
     * Mark an Activity as timed-out when not all expected Enrichments have arrived in time. For this get the ActivityEnrichments for which the
     * INITIAL was received before the threshold. For those determine the ones where the last received Enrichment was received before the threshold.
     * These can be marked as timed-out by sending an event to reflect this and cleaning up the repository afterwards.
     */
    public void markTimedOutActivities() {
        var threshold = Instant.now(clock).minus(timedOutInMinutes, MINUTES);

        activityEnrichmentRepository.findAllByStartedAtBefore(threshold)
                .forEach(activityEnrichment -> {
                    userIdField.updateValue(activityEnrichment.getUserId().toString());
                    log.info("Transaction enrichment for activity {} TIMED-OUT", activityEnrichment.getActivityId());
                    accountsAndTransactionMetrics.incrementActivityEnrichmentTimedOutCounter();
                    usersClient.getUserContext(activityEnrichment.getUserId())
                            .ifPresentOrElse(userContext -> {
                                        clientIdField.updateValue(userContext.getClientId().toString());
                                        transactionEnrichmentsFinishedActivityEventProducer.markAsTimedOut(activityEnrichment.getActivityId(), ZonedDateTime.now(clock), userContext);

                                    },
                                    () -> log.warn("Unable to find user-context for user {}. Cannot mark activity {} as timed-out", activityEnrichment.getUserId(), activityEnrichment.getActivityId()));
                    activityEnrichmentRepository.delete(activityEnrichment);
                });
    }

    private static Collection<EnrichmentMessageType> relevantMessageTypes(ClientToken clientToken) {
        return ENRICHMENT_TYPES.stream()
                .filter(enrichmentMessageType -> claimValue(clientToken, enrichmentMessageType))
                .collect(toSet());
    }

    /*
     * Check if the type of enrichment is "active" for the client. For this the client-token contains a set of specific
     * claims. The internal "INITIAL" enrichment is used to mark the start of the enrichment activity. It is only allowed
     * if at least one of the enrichments is active for this client.
     */
    private static boolean claimValue(ClientToken clientToken, EnrichmentMessageType enrichmentMessageType) {
        if (clientToken == null) {
            return false;
        }

        try {
            switch (enrichmentMessageType) {
                case CATEGORIES:
                    return hasClaim(clientToken, CLAIM_DATA_ENRICHMENT_CATEGORIZATION);
                case COUNTER_PARTIES:
                    return hasClaim(clientToken, CLAIM_DATA_ENRICHMENT_MERCHANT_RECOGNITION);
                case TRANSACTION_CYCLES:
                    return hasClaim(clientToken, CLAIM_DATA_ENRICHMENT_CYCLE_DETECTION);
                case LABELS:
                    return hasClaim(clientToken, CLAIM_DATA_ENRICHMENT_LABELS);
            }
        } catch (IllegalStateException e) {
            log.warn("Failed to determine claim-value for enrichment type {}", enrichmentMessageType);
        }

        return false;
    }

    static Map<UUID, LocalDate> determineOldestTransactionDatesForAccounts(Set<TransactionEnrichment> transactions) {
        return transactions.stream()
                .collect(groupingBy(TransactionEnrichment::getAccountId,
                        mapping(TransactionEnrichment::getDate,
                                collectingAndThen(toList(), ActivityEnrichmentService::findOldest))));
    }

    /*
     * Note this method should be called with a non-empty list of dates or else it will throw an exception. When its called
     * as part of the downstream collector on a stream of transactions then we can be sure that the list is non-empty since
     * in that case there is at least one transaction, and a transaction is required to have a date.
     */
    private static LocalDate findOldest(Collection<LocalDate> dates) {
        return dates.stream()
                .min(LocalDate::compareTo)
                .orElseThrow(() -> new RuntimeException("No dates provided, unable to find the oldest."));
    }

    public static String clientTokenSettingsLogString(ClientToken clientToken) {
        if (clientToken == null) {
            return "no-token";
        } else {
            return format("cat=%s,merch=%s,cycle=%s,label=%s",
                    hasClaim(clientToken, CLAIM_DATA_ENRICHMENT_CATEGORIZATION),
                    hasClaim(clientToken, CLAIM_DATA_ENRICHMENT_MERCHANT_RECOGNITION),
                    hasClaim(clientToken, CLAIM_DATA_ENRICHMENT_CYCLE_DETECTION),
                    hasClaim(clientToken, CLAIM_DATA_ENRICHMENT_LABELS));
        }
    }

    private static boolean hasClaim(ClientToken clientToken, String claimName) {
        try {
            return clientToken.hasClaim(claimName) && clientToken.getClaimValue(claimName, Boolean.class);
        } catch (MalformedClaimException e) {
            log.warn("Malformed claim {}, assuming false", claimName, e);
            return false;
        }
    }

    /*
     * Compute the checksum for the ActivityEnrichmentType. This value is computed by the sum of the checksums for the
     * enrichments that are active in the ClientToken. The sum of the additional enrichments for the ActivityEnrichmentType
     * are added to compute an overall checksum.
     */
    long computeCheckSumForFinishedEvent(ClientToken clientToken, ActivityEnrichmentType activityEnrichmentType) {
        var activeAdditionalEnrichments = activityEnrichmentType.additionalEnrichments.stream()
                .filter(enrichmentMessageType -> claimValue(clientToken, enrichmentMessageType)).toList();
        var activeRelevantMessages = relevantMessageTypes(clientToken);

        return concat(activeRelevantMessages.stream(), activeAdditionalEnrichments.stream())
                .mapToLong(EnrichmentMessageType::checksumValue)
                .sum();
    }
}
