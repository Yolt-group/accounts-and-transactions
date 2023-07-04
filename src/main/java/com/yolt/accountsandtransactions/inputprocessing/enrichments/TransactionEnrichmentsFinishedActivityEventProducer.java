package com.yolt.accountsandtransactions.inputprocessing.enrichments;

import com.yolt.accountsandtransactions.accounts.AccountService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import nl.ing.lovebird.activityevents.events.ActivityEventKey;
import nl.ing.lovebird.activityevents.events.TransactionsEnrichmentFinishedEvent;
import nl.ing.lovebird.clienttokens.AbstractClientToken;
import nl.ing.lovebird.clienttokens.ClientUserToken;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Service;

import java.time.LocalDate;
import java.time.ZonedDateTime;
import java.util.*;

import static com.yolt.accountsandtransactions.inputprocessing.enrichments.UserContext.USER_CONTEXT_HEADER_KEY;
import static java.util.Collections.emptyMap;
import static java.util.Optional.ofNullable;
import static java.util.stream.Collectors.toList;
import static nl.ing.lovebird.activityevents.events.TransactionsEnrichmentFinishedEvent.Status;
import static nl.ing.lovebird.activityevents.events.TransactionsEnrichmentFinishedEvent.Status.SUCCESS;
import static nl.ing.lovebird.activityevents.events.TransactionsEnrichmentFinishedEvent.Status.TIMEOUT;
import static nl.ing.lovebird.activityevents.events.TransactionsEnrichmentFinishedEvent.UserSiteInfo;
import static nl.ing.lovebird.activityevents.events.serializer.ActivityEventSerializer.serialize;
import static nl.ing.lovebird.clienttokens.constants.ClientTokenConstants.CLIENT_TOKEN_HEADER_NAME;
import static org.springframework.kafka.support.KafkaHeaders.MESSAGE_KEY;
import static org.springframework.kafka.support.KafkaHeaders.TOPIC;

@RequiredArgsConstructor
@Service
@Slf4j
public class TransactionEnrichmentsFinishedActivityEventProducer {
    private final KafkaTemplate<String, TransactionsEnrichmentFinishedEvent> kafkaTemplate;
    private final AccountService accountService;
    @Value("${yolt.kafka.topics.activityEvents.topic-name}")
    private final String topic;


    public void markAsCompleted(UUID activityId, Map<UUID, LocalDate> accountToOldestChangedDate, ZonedDateTime completedAt, ClientUserToken clientUserToken) {
        send(null, activityId, accountToOldestChangedDate, completedAt, clientUserToken, SUCCESS);
    }

    /**
     * Note: this is the only place where a ClientUserToken is not required, there's a hack to accommodate this situation
     * in the corresponding Kafka consumer in site-management that converts the UserContext to a ClientUserToken.
     */
    public void markAsTimedOut(UUID activityId, ZonedDateTime timedOutAt, UserContext userContext) {
        send(userContext, activityId, emptyMap(), timedOutAt, null, TIMEOUT);
    }

    private void send(UserContext userContext, UUID activityId, Map<UUID, LocalDate> accountToOldestChangedDate, ZonedDateTime time, ClientUserToken clientUserToken, Status status) {
        if (userContext == null && clientUserToken == null) {
            throw new IllegalArgumentException("Must have either a UserContext or a ClientUserToken");
        }
        var userId = Optional.ofNullable(clientUserToken)
                .map(ClientUserToken::getUserIdClaim)
                .orElseGet(() -> Objects.requireNonNull(userContext).getUserId());

        // The request trace id is not used but still part of the kafka key.
        // We can't use a random UUID here. Then the message might not be partitioned correctly.
        // By setting it zero we ensure that the kafka key is effectively the userId and activityId
        var requestTraceId = new UUID(0, 0);

        var message = MessageBuilder
                .withPayload(new TransactionsEnrichmentFinishedEvent(userId, activityId, getUserSiteInfo(userId, accountToOldestChangedDate), time, status))
                .setHeader(CLIENT_TOKEN_HEADER_NAME, Optional.ofNullable(clientUserToken).map(AbstractClientToken::getSerialized).orElse(null))
                .setHeader(USER_CONTEXT_HEADER_KEY, Optional.ofNullable(userContext).map(UserContext::toJson).orElse(null))
                .setHeader(MESSAGE_KEY, serialize(new ActivityEventKey(userId, activityId, requestTraceId)))
                .setHeader(TOPIC, topic)
                .build();

        kafkaTemplate.send(message).completable()
                .handle((sendResult, throwable) -> {
                    ofNullable(throwable)
                            .ifPresentOrElse(
                                    t -> log.error("Failed to publish request to Kafka", t),
                                    () -> log.debug("Successfully published {} for activity {} to Kafka: {}", message.getPayload().getClass().getSimpleName(), message.getPayload().getActivityId(), sendResult.getRecordMetadata().topic()));
                    return null;
                });
    }

    List<UserSiteInfo> getUserSiteInfo(UUID userId, Map<UUID, LocalDate> accountToOldestChangedDate) {
        return accountService.getUserSiteIdsForAccountIds(userId, accountToOldestChangedDate.keySet())
                .entrySet().stream()
                .map(accountIdToUserSiteId -> UserSiteInfo.builder()
                        .accountId(accountIdToUserSiteId.getKey())
                        .userSiteId(accountIdToUserSiteId.getValue())
                        .oldestChangedTransaction(accountToOldestChangedDate.get(accountIdToUserSiteId.getKey()))
                        .build())
                .collect(toList());
    }
}
