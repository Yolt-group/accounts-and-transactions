package com.yolt.accountsandtransactions.transactions.updates;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.github.tomakehurst.wiremock.client.WireMock;
import com.yolt.accountsandtransactions.BaseIntegrationTest;
import com.yolt.accountsandtransactions.accounts.Account;
import com.yolt.accountsandtransactions.accounts.AccountRepository;
import com.yolt.accountsandtransactions.datascience.DsShortTransactionKeyDTO;
import com.yolt.accountsandtransactions.datascience.counterparties.client.dto.DsCounterpartiesFeedbackGroupsDTO;
import com.yolt.accountsandtransactions.datascience.counterparties.client.dto.DsCounterpartiesFeedbackGroupsResponseDTO;
import com.yolt.accountsandtransactions.transactions.Transaction;
import com.yolt.accountsandtransactions.transactions.TransactionRepository;
import com.yolt.accountsandtransactions.transactions.updates.api.BulkTransactionCounterpartyUpdateRequestDTO;
import com.yolt.accountsandtransactions.transactions.updates.updatesession.BulkUpdateSession;
import nl.ing.lovebird.clienttokens.test.TestClientTokens;
import nl.ing.lovebird.providerdomain.AccountType;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.time.Instant;
import java.time.LocalDate;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import static com.github.tomakehurst.wiremock.client.ResponseDefinitionBuilder.okForJson;
import static com.github.tomakehurst.wiremock.client.WireMock.equalToJson;
import static com.github.tomakehurst.wiremock.client.WireMock.urlPathMatching;
import static java.math.BigDecimal.ONE;
import static java.math.BigDecimal.TEN;
import static nl.ing.lovebird.extendeddata.common.CurrencyCode.EUR;
import static nl.ing.lovebird.extendeddata.common.CurrencyCode.GBP;
import static nl.ing.lovebird.extendeddata.transaction.TransactionStatus.BOOKED;
import static org.assertj.core.api.Assertions.assertThat;

public class CounterpartyAdjustmentServiceIntegrationTest extends BaseIntegrationTest {
    private static final UUID BULK_USER_SESSION_ID = UUID.randomUUID();

    private static final UUID CLIENT_GROUP_ID = UUID.randomUUID();
    private static final UUID CLIENT_ID = UUID.randomUUID();
    private static final UUID USER_ID = UUID.randomUUID();
    private static final UUID ACCOUNT_1 = UUID.randomUUID();
    private static final UUID TRANSACTION_ID_1 = UUID.randomUUID();
    private static final LocalDate TRANSACTION_DATE_1 = LocalDate.now();
    private static final UUID ACCOUNT_2 = UUID.randomUUID();
    private static final UUID TRANSACTION_ID_2 = UUID.randomUUID();

    @Autowired
    private CounterpartyAdjustmentService counterpartyAdjustmentService;
    @Autowired
    private AccountRepository accountRepository;
    @Autowired
    private TransactionRepository transactionRepository;
    @Autowired
    private TestClientTokens testClientTokens;

    @Test
    public void shouldUpdateMerchantForSimilarTransactions() throws JsonProcessingException {
        // Given
        var requestDTO = DsCounterpartiesFeedbackGroupsDTO.builder()
                .counterpartyName("newMerchant")
                .groupSelectors(Set.of("one", "two"))
                .build();

        var responseDTO = new DsCounterpartiesFeedbackGroupsResponseDTO("actualNewMerchant",
                true,
                List.of(
                        new DsShortTransactionKeyDTO(ACCOUNT_1, TRANSACTION_ID_1.toString()),
                        new DsShortTransactionKeyDTO(ACCOUNT_2, TRANSACTION_ID_2.toString())
                )
        );

        WireMock.stubFor(
                WireMock.put(urlPathMatching("/counterparties/users/" + USER_ID + "/feedback/groups"))
                        .withRequestBody(equalToJson(objectMapper.writeValueAsString(requestDTO)))
                        .willReturn(okForJson(responseDTO)));

        accountRepository.saveBatch(List.of(createAccount(USER_ID, ACCOUNT_1), createAccount(USER_ID, ACCOUNT_2)), 2);
        transactionRepository.upsert(List.of(
                Transaction.builder()
                        .userId(USER_ID)
                        .accountId(ACCOUNT_1)
                        .date(TRANSACTION_DATE_1)
                        .id(TRANSACTION_ID_1.toString())
                        .status(BOOKED)
                        .amount(TEN)
                        .currency(EUR)
                        .description("")
                        .timestamp(Instant.now())
                        .build()
        ));

        // When
        var clientUserToken = testClientTokens.createClientUserToken(CLIENT_GROUP_ID, CLIENT_ID, USER_ID);
        var bulkMerchantAdjustmentRequestDTO = new BulkTransactionCounterpartyUpdateRequestDTO(UUID.randomUUID(), Set.of("one", "two"), "newMerchant");
        var feedbackActivity = counterpartyAdjustmentService.updateSimilarTransactions(
                clientUserToken,
                BulkUpdateSession.builder()
                        .userId(USER_ID)
                        .updateSessionId(BULK_USER_SESSION_ID)
                        .accountId(ACCOUNT_1)
                        .transactionId(TRANSACTION_ID_1.toString())
                        .date(TRANSACTION_DATE_1)
                        .build(),
                bulkMerchantAdjustmentRequestDTO);

        assertThat(feedbackActivity).isPresent();
        assertThat(feedbackActivity.get().activityId).isNotNull();
        assertThat(feedbackActivity.get().counterPartyName).isEqualTo("actualNewMerchant");
        assertThat(feedbackActivity.get().knownMerchant).isTrue();
    }

    private static Account createAccount(UUID userId, UUID accountId) {
        return Account.builder()
                .userId(userId)
                .siteId(UUID.randomUUID())
                .userSiteId(UUID.randomUUID())
                .id(accountId)
                .name("current account")
                .externalId(accountId.toString())
                .type(AccountType.CURRENT_ACCOUNT)
                .currency(GBP)
                .balance(ONE)
                .status(Account.Status.ENABLED)
                .build();
    }
}