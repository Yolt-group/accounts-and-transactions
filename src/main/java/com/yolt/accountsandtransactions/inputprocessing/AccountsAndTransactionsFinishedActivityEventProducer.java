package com.yolt.accountsandtransactions.inputprocessing;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import nl.ing.lovebird.activityevents.events.ActivityEventKey;
import nl.ing.lovebird.activityevents.events.IngestionFinishedEvent;
import nl.ing.lovebird.activityevents.events.serializer.ActivityEventSerializer;
import nl.ing.lovebird.clienttokens.ClientToken;
import nl.ing.lovebird.clienttokens.constants.ClientTokenConstants;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Component;

import java.util.UUID;

@Component
@Slf4j
public class AccountsAndTransactionsFinishedActivityEventProducer {

    private final KafkaTemplate<String, IngestionFinishedEvent> kafkaTemplate;
    private final String topic;

    public AccountsAndTransactionsFinishedActivityEventProducer(KafkaTemplate<String, IngestionFinishedEvent> kafkaTemplate,
                                                                @Value("${yolt.kafka.topics.activityEvents.topic-name}") String topic) {
        this.kafkaTemplate = kafkaTemplate;
        this.topic = topic;
    }

    public void sendMessage(final @NonNull IngestionFinishedEvent ingestionFinishedEvent,
                            final @NonNull ClientToken clientToken) {

        // The request trace id is not used but still part of the kafka key.
        // We can't use a random UUID here. Then the message might not be partitioned correctly.
        // By setting it zero we ensure that the kafka key is effectively the userId and activityId
        var requestTraceId = new UUID(0, 0);

        final ActivityEventKey activityEventKey = new ActivityEventKey(ingestionFinishedEvent.getUserId(), ingestionFinishedEvent.getActivityId(), requestTraceId);

        final String messageKey = ActivityEventSerializer.serialize(activityEventKey);
        Message<IngestionFinishedEvent> message = MessageBuilder
                .withPayload(ingestionFinishedEvent)
                .setHeader(ClientTokenConstants.CLIENT_TOKEN_HEADER_NAME, clientToken.getSerialized())
                .setHeader(KafkaHeaders.MESSAGE_KEY, messageKey)
                .setHeader(KafkaHeaders.TOPIC, topic)
                .build();

        kafkaTemplate.send(message)
                .addCallback(sendResult -> {
                    if (sendResult != null) {
                        log.debug("Successfully published to Kafka: {}", sendResult.getRecordMetadata().topic());
                    }
                }, throwable -> log.error("Failed to publish request to Kafka", throwable));
    }
}
