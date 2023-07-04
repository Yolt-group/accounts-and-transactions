package com.yolt.accountsandtransactions;

import com.yolt.accountsandtransactions.accounts.event.AccountEvent;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

@Slf4j
@Component
public class AccountEventsTestConsumer {

    private final List<Message> consumed = new ArrayList<>();

    @KafkaListener(topics = "${yolt.kafka.topics.account-events.topic-name}",
            concurrency = "${yolt.kafka.topics.account-events.listener-concurrency}")
    public void transactionsUpdate(@Payload final AccountEvent event) {
        log.info("got message: {} for event-type: {}", event, event.getType());
        consumed.add(new Message(event));
    }

    List<Message> getConsumed() {
        return consumed;
    }

    @Value
    public static class Message {
        AccountEvent accountEvent;
    }
}
