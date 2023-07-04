package com.yolt.accountsandtransactions;

import com.yolt.accountsandtransactions.inputprocessing.enrichments.UserContext;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import nl.ing.lovebird.activityevents.events.AbstractEvent;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

@Slf4j
@Component
public class ActivityEventsTestConsumer {

    private final List<Message> consumed = new ArrayList<>();


    @KafkaListener(topics = "${yolt.kafka.topics.activityEvents.topic-name}",
            concurrency = "${yolt.kafka.topics.activityEvents.listener-concurrency}")
    public void transactionsUpdate(@Payload final AbstractEvent event,
                                   @Header(value = UserContext.USER_CONTEXT_HEADER_KEY, required = false) final UserContext userContextHeader) {
        log.info("got message: {} for activity {}", event, event.getActivityId());
        consumed.add(new Message(userContextHeader, event));
    }

    public List<Message> getConsumed() {
        return consumed;
    }

    public void clearMessages() {
        consumed.clear();
    }

    @Value
    public static class Message {
        UserContext userContext;
        AbstractEvent abstractEvent;
    }
}
