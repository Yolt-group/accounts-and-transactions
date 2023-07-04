package com.yolt.accountsandtransactions.inputprocessing.enrichments;

import com.yolt.accountsandtransactions.inputprocessing.enrichments.activities.ActivityEnrichmentService;
import com.yolt.accountsandtransactions.inputprocessing.enrichments.api.base.EnrichmentMessage;
import com.yolt.accountsandtransactions.inputprocessing.enrichments.api.categories.CategoriesEnrichmentMessage;
import com.yolt.accountsandtransactions.inputprocessing.enrichments.api.counterparties.CounterpartiesEnrichmentMessage;
import com.yolt.accountsandtransactions.inputprocessing.enrichments.api.cycles.CyclesEnrichmentMessage;
import com.yolt.accountsandtransactions.inputprocessing.enrichments.api.labels.LabelsEnrichmentMessage;
import com.yolt.accountsandtransactions.metrics.AccountsAndTransactionMetrics;
import com.yolt.accountsandtransactions.transactions.enrichments.api.TransactionEnrichment;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import nl.ing.lovebird.clienttokens.ClientToken;
import nl.ing.lovebird.clienttokens.ClientUserToken;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

import java.time.Clock;
import java.util.Set;

import static com.yolt.accountsandtransactions.inputprocessing.enrichments.activities.ActivityEnrichmentService.clientTokenSettingsLogString;
import static com.yolt.accountsandtransactions.inputprocessing.enrichments.api.EnrichmentMessageType.*;
import static java.time.Duration.between;
import static java.time.Instant.now;
import static java.util.Collections.emptySet;
import static nl.ing.lovebird.clienttokens.constants.ClientTokenConstants.CLIENT_TOKEN_HEADER_NAME;
import static nl.ing.lovebird.logging.LogTypeMarker.getDataErrorMarker;

@ConditionalOnProperty("yolt.kafka.topics.transaction-enrichments.topic-name")
@Component
@Slf4j
@RequiredArgsConstructor
public class TransactionEnrichmentsEventConsumer {

    private final TransactionEnrichmentsMessageHandler transactionEnrichmentsMessageHandler;
    private final ActivityEnrichmentService activityEnrichmentService;
    private final AccountsAndTransactionMetrics metrics;
    private final Clock clock;


    /*
     * Consume incoming EnrichmentMessage's from Kafka. First check if the message can be handled and if so process the message.
     * Also register the message for the associated activity if necessary mark the activity as enrichment-completed.
     */
    @KafkaListener(topics = "${yolt.kafka.topics.transaction-enrichments.topic-name}",
            concurrency = "${yolt.kafka.topics.transaction-enrichments.listener-concurrency}")
    public void consume(@Payload final EnrichmentMessage enrichmentMessage,
                        @Header(value = CLIENT_TOKEN_HEADER_NAME) final ClientUserToken clientUserToken) {
        log.debug("Received {} ({}) with version {} (activity {}, {} client-token)", enrichmentMessage.getDomain(), enrichmentMessage.getClass().getSimpleName(), enrichmentMessage.getVersion(), enrichmentMessage.getActivityId(), clientUserToken != null ? "with" : "without");

        var isEnabled = switch (enrichmentMessage.getDomain()) {
            case CATEGORIES, COUNTER_PARTIES, TRANSACTION_CYCLES, LABELS -> true;
            default -> false;
        };

        if (!isEnabled) {
            log.debug("Processing of enrichment message {} is disabled. Skipping.", enrichmentMessage.getDomain());
            return;
        }

        try {
            if (canHandleMessage(enrichmentMessage, clientUserToken)) {
                var startOfHandlingEnrichment = now(clock);

                log.debug("Handling {} ({}) with version {} (activity {})", enrichmentMessage.getDomain(), enrichmentMessage.getClass().getSimpleName(), enrichmentMessage.getVersion(), enrichmentMessage.getActivityId());

                var affectedTransactions = processEnrichments(enrichmentMessage);
                log.info("Enriched {} transactions for {} on activity: {}", affectedTransactions.size(), enrichmentMessage.getDomain(), enrichmentMessage.getActivityId());

                activityEnrichmentService.processActivityEnrichment(enrichmentMessage, affectedTransactions, clientUserToken);
                log.debug("Handled {} ({}) with version {} for activity {} ({})", enrichmentMessage.getDomain(), enrichmentMessage.getClass().getSimpleName(), enrichmentMessage.getVersion(), enrichmentMessage.getActivityId(), clientTokenSettingsLogString(clientUserToken));

                metrics.updateClientEnrichment(clientUserToken.getClientIdClaim().toString(), enrichmentMessage.getDomain().value);
                metrics.updateClientEnrichmentDuration(clientUserToken.getClientIdClaim().toString(), enrichmentMessage.getDomain().value, between(startOfHandlingEnrichment, now(clock)));
            }
        } catch (RuntimeException e) {
            log.warn(getDataErrorMarker(), "Skipping {} ({}) with version {} for activity {} ({})", enrichmentMessage.getDomain(), enrichmentMessage.getClass().getSimpleName(), enrichmentMessage.getVersion(), enrichmentMessage.getActivityId(), clientTokenSettingsLogString(clientUserToken), e);
        }
    }

    private boolean canHandleMessage(@NonNull EnrichmentMessage enrichmentMessage, @NonNull ClientToken clientToken) {

        // If the client is not subscribed to this enrichment message we do not need to handle it.
        if (!ActivityEnrichmentService.isRelevant(enrichmentMessage, clientToken)) {
            log.debug("Data-Science not active for {}. Skipping.", enrichmentMessage.getDomain());
            return false;
        }

        // If there is a version mismatch we can skip this enrichment-message.
        // Note: For now this is a simple check. In the future we can extend this.
        if (enrichmentMessage.getVersion() != 1) {
            log.warn("Version {} for {} not supported yet.", enrichmentMessage.getVersion(), enrichmentMessage.getDomain());
            return false;
        }

        return true;
    }

    private Set<TransactionEnrichment> processEnrichments(EnrichmentMessage enrichmentMessage) {
        if (enrichmentMessage.getDomain() == CATEGORIES) {
            return transactionEnrichmentsMessageHandler.process((CategoriesEnrichmentMessage) enrichmentMessage);
        } else if (enrichmentMessage.getDomain() == COUNTER_PARTIES) {
            return transactionEnrichmentsMessageHandler.process((CounterpartiesEnrichmentMessage) enrichmentMessage);
        } else if (enrichmentMessage.getDomain() == TRANSACTION_CYCLES) {
            return transactionEnrichmentsMessageHandler.process((CyclesEnrichmentMessage) enrichmentMessage);
        } else if (enrichmentMessage.getDomain() == LABELS) {
            return transactionEnrichmentsMessageHandler.process((LabelsEnrichmentMessage) enrichmentMessage);
        } else {
            log.warn("Unexpected enrichment message {} will be skipped.", enrichmentMessage.getDomain());
            return emptySet();
        }
    }
}
