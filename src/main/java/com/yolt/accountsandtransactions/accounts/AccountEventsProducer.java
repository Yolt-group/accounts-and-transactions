package com.yolt.accountsandtransactions.accounts;

import com.yolt.accountsandtransactions.accounts.event.AccountEvent;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import nl.ing.lovebird.clienttokens.ClientToken;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Service;
import org.springframework.validation.annotation.Validated;

import javax.validation.Valid;

import static nl.ing.lovebird.clienttokens.constants.ClientTokenConstants.CLIENT_TOKEN_HEADER_NAME;

@Slf4j
@Service
@Validated
public class AccountEventsProducer {

    private static final String EVENT_TYPE_HEADER = "event-type";

    private final KafkaTemplate<String, AccountEvent> kafkaTemplate;
    private final String topic;

    public AccountEventsProducer(KafkaTemplate<String, AccountEvent> kafkaTemplate,
                                 @Value("${yolt.kafka.topics.account-events.topic-name}") String topic) {
        this.kafkaTemplate = kafkaTemplate;
        this.topic = topic;
    }

    public void sendMessage(@Valid AccountEvent payload, @NonNull ClientToken clientToken) {
        Message<AccountEvent> message = MessageBuilder
                .withPayload(payload)
                .setHeader(KafkaHeaders.TOPIC, topic)
                .setHeader(CLIENT_TOKEN_HEADER_NAME, clientToken.getSerialized())
                .setHeader(KafkaHeaders.MESSAGE_KEY, payload.getUserId().toString())
                .build();
        kafkaTemplate.send(message);
    }
}
