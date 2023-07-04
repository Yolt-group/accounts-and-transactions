package com.yolt.accountsandtransactions.datascience;

import com.github.tomakehurst.wiremock.client.WireMock;
import com.yolt.accountsandtransactions.BaseIntegrationTest;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.List;
import java.util.UUID;

import static com.github.tomakehurst.wiremock.client.WireMock.badRequest;
import static com.github.tomakehurst.wiremock.client.WireMock.serverError;
import static com.github.tomakehurst.wiremock.client.WireMock.urlPathMatching;

class DsAccountDataDeletionServiceTest extends BaseIntegrationTest {
    @Autowired
    DsAccountDataDeletionService dsAccountDataDeletionService;

    @Test
    void testAllCallsAreMadeWhenPreviousCallsFail() {
        UUID userId = UUID.randomUUID();
        UUID accountId1 = UUID.randomUUID();
        UUID accountId2 = UUID.randomUUID();

        // mock datascience responses
        final String accountDeletePath1 = "/users/" + userId + "/accounts/" + accountId1;
        WireMock.stubFor(
                WireMock.delete(urlPathMatching(accountDeletePath1))
                        .willReturn(serverError()));
        final String accountDeletePath2 = "/users/" + userId + "/accounts/" + accountId2;
        WireMock.stubFor(
                WireMock.delete(urlPathMatching(accountDeletePath2))
                        .willReturn(badRequest()));

        // make calls to delete account data
        dsAccountDataDeletionService.deleteAccountData(userId, List.of(accountId1, accountId2));

        // verify datascience was called (the calls are retried 2 times if they fail)
        WireMock.verify(6, WireMock.deleteRequestedFor(WireMock.urlPathMatching(accountDeletePath1)));
        WireMock.verify(6, WireMock.deleteRequestedFor(WireMock.urlPathMatching(accountDeletePath2)));
    }
}