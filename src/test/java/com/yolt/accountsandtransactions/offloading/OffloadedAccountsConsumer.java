package com.yolt.accountsandtransactions.offloading;

import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

@Slf4j
@Component
public class OffloadedAccountsConsumer {

    private final List<Message> consumed = new ArrayList<>();

    @KafkaListener(topics = "${yolt.kafka.topics.offload-yts-accounts.topic-name}",
            concurrency = "${yolt.kafka.topics.offload-yts-accounts.listener-concurrency}")
    public void transactionsUpdate(@Payload final OffloadableEnvelope<?> envelope) {
        consumed.add(new Message(envelope));
    }

    public List<Message> getConsumed() {
        return consumed;
    }

    @Value
    public static class Message {
        OffloadableEnvelope<?> envelope;
    }

    public Optional<OffloadableEnvelope<?>> head() {
        return consumed.stream()
                .findFirst()
                .map(message -> message.envelope);
    }

    public void reset() {
        consumed.clear();
    }
}
