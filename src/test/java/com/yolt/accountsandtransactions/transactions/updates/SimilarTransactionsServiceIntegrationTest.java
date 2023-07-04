package com.yolt.accountsandtransactions.transactions.updates;

import com.github.tomakehurst.wiremock.client.WireMock;
import com.yolt.accountsandtransactions.BaseIntegrationTest;
import com.yolt.accountsandtransactions.datascience.DsShortTransactionKeyDTO;
import com.yolt.accountsandtransactions.datascience.preprocessing.dto.DsSimilarTransactionGroupsDTO;
import com.yolt.accountsandtransactions.datascience.preprocessing.dto.DsSimilarTransactionsDTO;
import com.yolt.accountsandtransactions.transactions.updates.api.SimilarTransactionGroupDTO;
import com.yolt.accountsandtransactions.transactions.updates.api.TransactionsGroupedByAccountId;
import com.yolt.accountsandtransactions.transactions.updates.updatesession.BulkUpdateSession;
import nl.ing.lovebird.clienttokens.test.TestClientTokens;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.time.LocalDate;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import static com.github.tomakehurst.wiremock.client.ResponseDefinitionBuilder.okForJson;
import static com.github.tomakehurst.wiremock.client.WireMock.equalTo;
import static com.github.tomakehurst.wiremock.client.WireMock.urlPathMatching;
import static java.util.Collections.emptyList;
import static org.assertj.core.api.Assertions.assertThat;

public class SimilarTransactionsServiceIntegrationTest extends BaseIntegrationTest {
    private static final UUID BULK_USER_SESSION_ID = UUID.randomUUID();

    private static final UUID USER_ID = UUID.randomUUID();
    private static final UUID ACCOUNT_1 = UUID.randomUUID();
    private static final UUID TRANSACTION_ID_1 = UUID.randomUUID();
    private static final LocalDate TRANSACTION_DATE_1 = LocalDate.now();
    private static final UUID ACCOUNT_2 = UUID.randomUUID();
    private static final UUID TRANSACTION_ID_2 = UUID.randomUUID();
    private static final UUID ACCOUNT_3 = UUID.randomUUID();
    private static final UUID TRANSACTION_ID_3 = UUID.randomUUID();

    @Autowired
    private SimilarTransactionsService similarTransactionsService;

    @Autowired
    private TestClientTokens testClientTokens;

    @Test
    public void shouldGetSimilarTransactions() {
        // Given
        var responseDTO = new DsSimilarTransactionsDTO(
                List.of(
                        DsSimilarTransactionGroupsDTO.builder()
                                .groupSelector("Group one description")
                                .transactions(List.of(
                                        DsShortTransactionKeyDTO.builder()
                                                .accountId(ACCOUNT_1)
                                                .transactionId(TRANSACTION_ID_1.toString())
                                                .build(),
                                        DsShortTransactionKeyDTO.builder()
                                                .accountId(ACCOUNT_2)
                                                .transactionId(TRANSACTION_ID_2.toString())
                                                .build()))
                                .build(),
                        DsSimilarTransactionGroupsDTO.builder()
                                .groupSelector("Group two description")
                                .transactions(List.of(
                                        DsShortTransactionKeyDTO.builder()
                                                .accountId(ACCOUNT_3)
                                                .transactionId(TRANSACTION_ID_3.toString())
                                                .build()))
                                .build()));

        WireMock.stubFor(
                WireMock.get(urlPathMatching("/v1/similar-transactions"))
                        .withQueryParam("accountId", equalTo(ACCOUNT_1.toString()))
                        .withQueryParam("transactionId", equalTo(TRANSACTION_ID_1.toString()))
                        .willReturn(okForJson(responseDTO)));

        // When
        var transactionsByMerchantView = similarTransactionsService.getSimilarTransactions(
                testClientTokens.createClientUserToken(UUID.randomUUID(), UUID.randomUUID(), USER_ID),
                BulkUpdateSession.builder()
                        .userId(USER_ID)
                        .updateSessionId(BULK_USER_SESSION_ID)
                        .accountId(ACCOUNT_1)
                        .transactionId(TRANSACTION_ID_1.toString())
                        .date(TRANSACTION_DATE_1)
                        .build());

        // Then
        assertThat(transactionsByMerchantView).isPresent();

        assertThat(transactionsByMerchantView.get())
                .returns(TRANSACTION_ID_1.toString(), v -> v.getBulkUpdateSession().getTransactionId())
                .returns(ACCOUNT_1, v -> v.getBulkUpdateSession().getAccountId());

        assertThat(transactionsByMerchantView.get().getGroups())
                .containsExactlyInAnyOrder(
                        SimilarTransactionGroupDTO.builder()
                                .groupSelector("Group one description")
                                .groupDescription("Group one description")
                                .count(2)
                                .transactions(Set.of(
                                        TransactionsGroupedByAccountId.builder()
                                                .accountId(ACCOUNT_1)
                                                .transactionIds(Set.of(TRANSACTION_ID_1.toString()))
                                                .build(),
                                        TransactionsGroupedByAccountId.builder()
                                                .accountId(ACCOUNT_2)
                                                .transactionIds(Set.of(TRANSACTION_ID_2.toString()))
                                                .build()))
                                .build(),
                        SimilarTransactionGroupDTO.builder()
                                .groupSelector("Group two description")
                                .groupDescription("Group two description")
                                .count(1)
                                .transactions(Set.of(
                                        TransactionsGroupedByAccountId.builder()
                                                .accountId(ACCOUNT_3)
                                                .transactionIds(Set.of(TRANSACTION_ID_3.toString()))
                                                .build()))
                                .build());
    }

    @Test
    public void shouldReturnAnEmptyGroupIfTheListOfTransactionsFromTheCounterpartiesServiceIsEmpty() {
        // Given
        var responseDTO = new DsSimilarTransactionsDTO(
                List.of(
                        DsSimilarTransactionGroupsDTO.builder()
                                .groupSelector("Group one description")
                                .transactions(emptyList())
                                .build()));

        WireMock.stubFor(
                WireMock.get(urlPathMatching("/v1/similar-transactions"))
                        .withQueryParam("accountId", equalTo(ACCOUNT_1.toString()))
                        .withQueryParam("transactionId", equalTo(TRANSACTION_ID_1.toString()))
                        .willReturn(okForJson(responseDTO)));

        // When
        var similarTransactionsForUpdatesView = similarTransactionsService.getSimilarTransactions(
                testClientTokens.createClientUserToken(UUID.randomUUID(), UUID.randomUUID(), USER_ID),
                BulkUpdateSession.builder()
                        .userId(USER_ID)
                        .updateSessionId(BULK_USER_SESSION_ID)
                        .accountId(ACCOUNT_1)
                        .transactionId(TRANSACTION_ID_1.toString())
                        .date(TRANSACTION_DATE_1)
                        .build());

        // Then
        assertThat(similarTransactionsForUpdatesView).isPresent();
        assertThat(similarTransactionsForUpdatesView.get().getBulkUpdateSession().getUpdateSessionId()).isEqualTo(BULK_USER_SESSION_ID);
        assertThat(similarTransactionsForUpdatesView.get().getGroups().isEmpty()).isTrue();
    }

    @Test
    public void shouldReturnNoGroupsIfTheListOfGroupsFromTheCounterpartiesClientIsEmpty() {
        // Given
        WireMock.stubFor(
                WireMock.get(urlPathMatching("/v1/similar-transactions"))
                        .withQueryParam("accountId", equalTo(ACCOUNT_1.toString()))
                        .withQueryParam("transactionId", equalTo(TRANSACTION_ID_1.toString()))
                        .willReturn(okForJson(new DsSimilarTransactionsDTO(emptyList()))));

        // When
        var similarTransactionsForUpdatesView = similarTransactionsService.getSimilarTransactions(
                testClientTokens.createClientUserToken(UUID.randomUUID(), UUID.randomUUID(), USER_ID),
                BulkUpdateSession.builder()
                        .userId(USER_ID)
                        .updateSessionId(BULK_USER_SESSION_ID)
                        .accountId(ACCOUNT_1)
                        .transactionId(TRANSACTION_ID_1.toString())
                        .date(TRANSACTION_DATE_1)
                        .build());

        // Then
        assertThat(similarTransactionsForUpdatesView).isPresent();
        assertThat(similarTransactionsForUpdatesView.get().getBulkUpdateSession().getUpdateSessionId()).isEqualTo(BULK_USER_SESSION_ID);
        assertThat(similarTransactionsForUpdatesView.get().getGroups().isEmpty()).isTrue();
    }
}