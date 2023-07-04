package com.yolt.accountsandtransactions.accounts;

import com.github.tomakehurst.wiremock.client.WireMock;
import com.yolt.accountsandtransactions.BaseIntegrationTest;
import nl.ing.lovebird.clienttokens.test.TestClientTokens;
import nl.ing.lovebird.extendeddata.common.CurrencyCode;
import nl.ing.lovebird.providerdomain.AccountType;
import org.awaitility.Duration;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.messaging.support.MessageBuilder;

import java.math.BigDecimal;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.UUID;

import static com.github.tomakehurst.wiremock.client.WireMock.noContent;
import static com.github.tomakehurst.wiremock.client.WireMock.urlPathMatching;
import static com.yolt.accountsandtransactions.accounts.UserSiteEventDTO.EventType.DELETE_USER_SITE;
import static nl.ing.lovebird.clienttokens.constants.ClientTokenConstants.CLIENT_TOKEN_HEADER_NAME;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.springframework.kafka.support.KafkaHeaders.MESSAGE_KEY;
import static org.springframework.kafka.support.KafkaHeaders.TOPIC;

public class UserSiteDeletionEventConsumerIntegrationTest extends BaseIntegrationTest {

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @Autowired
    private AccountRepository accountRepository;

    @Autowired
    private TestClientTokens testClientTokens;

    @Test
    public void shouldDeleteAccountsAndTransactionsWhenReceivingDeleteEvent() {
        // Given
        var clientGroupId = UUID.randomUUID();
        var clientId = UUID.randomUUID();
        var userId = UUID.randomUUID();
        var clientUserToken = testClientTokens.createClientUserToken(clientGroupId, clientId, userId);

        var userSiteId = UUID.randomUUID();
        var siteId = UUID.randomUUID();
        var id = UUID.randomUUID();
        var account = Account.builder()
                .userId(userId)
                .siteId(siteId)
                .userSiteId(userSiteId)
                .id(id)
                .name("current account")
                .externalId(id.toString())
                .type(AccountType.CURRENT_ACCOUNT)
                .currency(CurrencyCode.GBP)
                .balance(BigDecimal.ONE)
                .status(Account.Status.ENABLED)
                .build();

        accountRepository.saveBatch(List.of(account), 1);

        final String accountDeletePath = "/users/" + userId + "/accounts/" + id;
        WireMock.stubFor(
                WireMock.delete(urlPathMatching(accountDeletePath))
                        .willReturn(noContent()));

        var deleteEvent = UserSiteEventDTO.builder()
                .siteId(siteId)
                .userId(userId)
                .userSiteId(userSiteId)
                .type(DELETE_USER_SITE)
                .time(ZonedDateTime.now())
                .build();

        // When
        kafkaTemplate.send(MessageBuilder
                .withPayload(deleteEvent)
                .setHeader(TOPIC, "userSites")
                .setHeader(MESSAGE_KEY, deleteEvent.getUserId().toString())
                .setHeader(CLIENT_TOKEN_HEADER_NAME, clientUserToken.getSerialized())
                .setHeader("type", deleteEvent.getType().name())
                .build());

        // Then
        await().timeout(Duration.ONE_MINUTE)
                .untilAsserted(() -> assertThat(accountRepository.getAccounts(userId)).isEmpty());

        await().timeout(Duration.FIVE_SECONDS)
                .untilAsserted(() -> WireMock.verify(2, WireMock.deleteRequestedFor(WireMock.urlPathMatching(accountDeletePath))));
    }
}