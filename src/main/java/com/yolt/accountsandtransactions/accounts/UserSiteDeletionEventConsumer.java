package com.yolt.accountsandtransactions.accounts;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import nl.ing.lovebird.clienttokens.ClientToken;
import nl.ing.lovebird.clienttokens.ClientUserToken;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

import java.util.Optional;

import static nl.ing.lovebird.clienttokens.constants.ClientTokenConstants.CLIENT_TOKEN_HEADER_NAME;

@Slf4j
@Service
@RequiredArgsConstructor
public class UserSiteDeletionEventConsumer {

    private final AccountService accountService;

    @KafkaListener(topics = "${yolt.kafka.topics.user-site-events.topic-name}",
            concurrency = "${yolt.kafka.topics.user-site-events.listener-concurrency}")
    public void consume(@Payload UserSiteEventDTO userSiteEvent, @Header(value = CLIENT_TOKEN_HEADER_NAME, required = false) Optional<ClientToken> clientToken) {
        if (log.isDebugEnabled()) {
            log.debug("Received user site event of type {}", userSiteEvent.getType());
        }
        if (!UserSiteEventDTO.EventType.DELETE_USER_SITE.equals(userSiteEvent.getType())) {
            return;
        }

        clientToken.ifPresentOrElse(
                clientUserToken -> {
                    if (!(clientUserToken instanceof ClientUserToken)) {
                        throw new IllegalArgumentException("Received a DELETE_USER_SITE with a ClientToken that is not a ClientUserToken.");
                    }
                    var userId = ((ClientUserToken) clientUserToken).getUserIdClaim();

                    accountService.deleteAccountsAndTransactionsForUserSite(userId, userSiteEvent.getUserSiteId());
                    log.info("Deleted user site information for user site id {}.", userSiteEvent.getUserSiteId());
                },
                () -> {
                    throw new IllegalArgumentException("Received a DELETE_USER_SITE without a ClientToken.");
                }
        );
    }

}
