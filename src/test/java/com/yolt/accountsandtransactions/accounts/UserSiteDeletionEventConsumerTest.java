package com.yolt.accountsandtransactions.accounts;

import nl.ing.lovebird.clienttokens.ClientToken;
import nl.ing.lovebird.clienttokens.ClientUserToken;
import nl.ing.lovebird.clienttokens.test.TestJwtClaims;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.ZonedDateTime;
import java.util.Optional;
import java.util.UUID;

import static com.yolt.accountsandtransactions.accounts.UserSiteEventDTO.EventType.DELETE_USER_SITE;
import static com.yolt.accountsandtransactions.accounts.UserSiteEventDTO.EventType.UPDATE_USER_SITE;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

@ExtendWith(MockitoExtension.class)
public class UserSiteDeletionEventConsumerTest {

    @Mock
    private AccountService accountService;

    @InjectMocks
    private UserSiteDeletionEventConsumer userSiteDeletionEventConsumer;

    @Test
    public void willIgnoreUserSiteUpdateEvents() {
        var userId = UUID.randomUUID();
        var clientUserToken = new ClientUserToken("mocked-client-token", TestJwtClaims.createClientUserClaims("junit", UUID.randomUUID(), UUID.randomUUID(), userId));
        var userSiteEvent = getUserSiteEvent(userId, UPDATE_USER_SITE);
        userSiteDeletionEventConsumer.consume(userSiteEvent, Optional.of(clientUserToken));

        verifyNoInteractions(accountService);
    }

    @Test
    public void willDeleteForUserSiteDeletionEvents() {
        var userId = UUID.randomUUID();
        var clientToken = new ClientUserToken("mocked-client-token", TestJwtClaims.createClientUserClaims("junit", UUID.randomUUID(), UUID.randomUUID(), userId));
        var userSiteEvent = getUserSiteEvent(userId, DELETE_USER_SITE);
        userSiteDeletionEventConsumer.consume(userSiteEvent, Optional.of(clientToken));

        verify(accountService).deleteAccountsAndTransactionsForUserSite(userId, userId);
    }

    private UserSiteEventDTO getUserSiteEvent(UUID uuid, UserSiteEventDTO.EventType eventType) {
        return UserSiteEventDTO
                .builder()
                .siteId(uuid)
                .userId(uuid)
                .userSiteId(uuid)
                .type(eventType)
                .time(ZonedDateTime.now())
                .build();
    }
}