package com.yolt.accountsandtransactions.transactions.updates;

import com.github.tomakehurst.wiremock.client.WireMock;
import com.yolt.accountsandtransactions.BaseIntegrationTest;
import com.yolt.accountsandtransactions.inputprocessing.enrichments.activities.ActivityEnrichmentService;
import com.yolt.accountsandtransactions.inputprocessing.enrichments.api.cycles.DsTransactionCycle;
import com.yolt.accountsandtransactions.transactions.TransactionRepository;
import com.yolt.accountsandtransactions.transactions.cycles.TransactionCycleDTO;
import com.yolt.accountsandtransactions.transactions.cycles.TransactionCyclesService;
import com.yolt.accountsandtransactions.transactions.updates.api.SeedTransactionKey;
import com.yolt.accountsandtransactions.transactions.updates.api.TransactionCyclesCreateRequest;
import com.yolt.accountsandtransactions.transactions.updates.api.TransactionCyclesUpdateRequest;
import nl.ing.lovebird.clienttokens.test.TestClientTokens;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.Period;
import java.util.*;

import static com.github.tomakehurst.wiremock.client.WireMock.ok;
import static com.github.tomakehurst.wiremock.client.WireMock.urlPathMatching;
import static com.yolt.accountsandtransactions.TestBuilders.createTransactionCycle;
import static com.yolt.accountsandtransactions.TestBuilders.createTransactionTemplate;
import static com.yolt.accountsandtransactions.transactions.updates.TransactionCyclesFeedbackController.TransactionCyclesFeedbackResponse;
import static nl.ing.lovebird.clienttokens.constants.ClientTokenConstants.CLIENT_TOKEN_HEADER_NAME;
import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.request;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

class TransactionCyclesFeedbackControllerTest extends BaseIntegrationTest {

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private TransactionRepository transactionRepository;

    @Autowired
    private TransactionCyclesService transactionCyclesService;

    @Autowired
    private ActivityEnrichmentService activityEnrichmentService;

    @Autowired
    private TestClientTokens testClientTokens;

    @Test
    void testCreateCycleReturnsOk() throws Exception {
        var clientGroupId = UUID.randomUUID();
        var clientId = UUID.randomUUID();
        var userId = UUID.randomUUID();
        var clientUserToken = testClientTokens.createClientUserToken(clientGroupId, clientId, userId);

        var cycleId = UUID.randomUUID();

        // create an existing reference transaction in the database
        var transaction = createTransactionTemplate().toBuilder()
                .userId(userId)
                .build();
        transactionRepository.saveBatch(List.of(transaction), 1);

        // create datascience response
        WireMock.stubFor(
                WireMock.post(urlPathMatching("/users/" + userId + "/transaction-cycles"))
                        .willReturn(ok()
                                .withHeader("Content-Type", "application/json")
                                .withBody(objectMapper.writeValueAsString(DsTransactionCycle.builder()
                                        .id(cycleId)
                                        .amount(BigDecimal.TEN)
                                        .period(Period.ofDays(7))
                                        .currency(Currency.getInstance(transaction.getCurrency().toString()))
                                        .subscription(true)
                                        .modelParameters(Optional.empty())
                                        .label(Optional.empty())
                                        .counterparty("Ajax")
                                        .predictedOccurrences(Set.of(LocalDate.now()))
                                        .build()
                                ))));

        var seed = new SeedTransactionKey(transaction.getAccountId(), transaction.getId(), transaction.getDate());

        // create cycle
        var mockRequest = post("/v1/users/{userId}/enrichment-tasks/transaction-cycles", userId)
                .header(CLIENT_TOKEN_HEADER_NAME, clientUserToken.getSerialized())
                .contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(new TransactionCyclesCreateRequest(seed, BigDecimal.TEN, Period.ofDays(7).toString(), Optional.empty())));

        var result = mockMvc.perform(mockRequest)
                .andExpect(request().asyncStarted())
                .andReturn();

        // wait for the completion
        var response = mockMvc.perform(asyncDispatch(result))
                .andExpect(status().isAccepted())
                .andReturn()
                .getResponse()
                .getContentAsString();

        // compare that all transaction cycles match (database, response) with set values and each other
        var feedback = objectMapper.readValue(response, TransactionCyclesFeedbackResponse.class);
        var activityId = feedback.activityId;
        var createdCycle = feedback.cycle.orElseThrow(AssertionError::new);
        var inDatabase = transactionCyclesService.find(userId, createdCycle.getCycleId())
                .orElseThrow(AssertionError::new);

        assertThat(inDatabase.getCycleId()).isEqualTo(cycleId);
        assertThat(inDatabase.getAmount()).isEqualTo(transaction.getAmount());
        assertThat(inDatabase.getPeriod()).isEqualTo(Period.ofDays(7).toString());
        assertThat(inDatabase.getCurrency()).isEqualTo(transaction.getCurrency().toString());
        assertThat(inDatabase.isSubscription()).isTrue();
        assertThat(inDatabase.getCounterparty()).isEqualTo("Ajax");
        assertThat(inDatabase.getPredictedOccurrences()).containsExactly(LocalDate.now());
        assertThat(inDatabase.getLabel()).isNull();
        assertThat(inDatabase.getModelAmount()).isNull();
        assertThat(inDatabase.getModelCurrency()).isNull();
        assertThat(inDatabase.getModelPeriod()).isNull();
        assertThat(inDatabase.isExpired()).isFalse();

        assertThat(TransactionCycleDTO.fromTransactionCycle(inDatabase))
                .isEqualTo(feedback.cycle.orElseThrow(AssertionError::new));

        assertThat(activityEnrichmentService.find(activityId)).isNotEmpty();
    }

    @Test
    void testCreateCycleAmountZeroReturnBadRequest() throws Exception {
        var clientGroupId = UUID.randomUUID();
        var clientId = UUID.randomUUID();
        var userId = UUID.randomUUID();
        var clientUserToken = testClientTokens.createClientUserToken(clientGroupId, clientId, userId);

        var seed = new SeedTransactionKey(UUID.randomUUID(), "id", LocalDate.now());

        // create cycle
        var mockRequest = post("/v1/users/{userId}/enrichment-tasks/transaction-cycles", userId)
                .header(CLIENT_TOKEN_HEADER_NAME, clientUserToken.getSerialized())
                .contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(new TransactionCyclesCreateRequest(seed, BigDecimal.TEN, Period.ofDays(7).toString(), Optional.empty())));

        var result = mockMvc.perform(mockRequest)
                .andExpect(request().asyncStarted())
                .andReturn();

        // wait for the completion
        mockMvc.perform(asyncDispatch(result))
                .andExpect(status().isBadRequest())
                .andReturn();
    }

    @Test
    void testUpdateCycleReturnsOk() throws Exception {
        var clientGroupId = UUID.randomUUID();
        var clientId = UUID.randomUUID();
        var userId = UUID.randomUUID();
        var clientUserToken = testClientTokens.createClientUserToken(clientGroupId, clientId, userId);

        var cycleId = UUID.randomUUID();

        createTransactionCycle(() -> userId,
                (builder, i) -> builder
                        .userId(userId)
                        .cycleId(cycleId)
                        .amount(BigDecimal.ONE)
                        .period(Period.ofMonths(1).toString()),
                cycle -> transactionCyclesService.upsert(cycle));

        // create datascience response
        WireMock.stubFor(
                WireMock.put(urlPathMatching("/users/" + userId + "/transaction-cycles/" + cycleId))
                        .willReturn(ok()
                                .withHeader("Content-Type", "application/json")
                                .withBody(objectMapper.writeValueAsString(DsTransactionCycle.builder()
                                        .id(cycleId)
                                        .amount(BigDecimal.TEN)
                                        .period(Period.ofDays(7))
                                        .currency(Currency.getInstance("EUR"))
                                        .subscription(true)
                                        .modelParameters(Optional.empty())
                                        .label(Optional.empty())
                                        .counterparty("Ajax")
                                        .predictedOccurrences(Set.of(LocalDate.now()))
                                        .build()
                                ))));

        // create cycle
        var mockRequest = put("/v1/users/{userId}/enrichment-tasks/transaction-cycles/{cycleId}", userId, cycleId)
                .header(CLIENT_TOKEN_HEADER_NAME, clientUserToken.getSerialized())
                .contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(new TransactionCyclesUpdateRequest(BigDecimal.TEN, Period.ofDays(7).toString(), Optional.empty())));

        var result = mockMvc.perform(mockRequest)
                .andExpect(request().asyncStarted())
                .andReturn();

        // wait for the completion
        var response = mockMvc.perform(asyncDispatch(result))
                .andExpect(status().isAccepted())
                .andReturn()
                .getResponse()
                .getContentAsString();

        // compare that all transaction cycles match (database, response) with set values and each other
        var feedback = objectMapper.readValue(response, TransactionCyclesFeedbackResponse.class);
        var activityId = feedback.activityId;
        var createdCycle = feedback.cycle.orElseThrow(AssertionError::new);
        var inDatabase = transactionCyclesService.find(userId, createdCycle.getCycleId())
                .orElseThrow(AssertionError::new);

        assertThat(inDatabase.getCycleId()).isEqualTo(cycleId);
        assertThat(inDatabase.getAmount()).isEqualTo(BigDecimal.TEN);
        assertThat(inDatabase.getPeriod()).isEqualTo(Period.ofDays(7).toString());
        assertThat(inDatabase.getCurrency()).isEqualTo("EUR");
        assertThat(inDatabase.isSubscription()).isTrue();
        assertThat(inDatabase.getCounterparty()).isEqualTo("Ajax");
        assertThat(inDatabase.getPredictedOccurrences()).containsExactly(LocalDate.now());
        assertThat(inDatabase.getLabel()).isNull();
        assertThat(inDatabase.getModelAmount()).isNull();
        assertThat(inDatabase.getModelCurrency()).isNull();
        assertThat(inDatabase.getModelPeriod()).isNull();
        assertThat(inDatabase.isExpired()).isFalse();

        assertThat(TransactionCycleDTO.fromTransactionCycle(inDatabase))
                .isEqualTo(feedback.cycle.orElseThrow(AssertionError::new));

        assertThat(activityEnrichmentService.find(activityId)).isNotEmpty();
    }


    @Test
    void testDeleteExpire() throws Exception {
        var clientGroupId = UUID.randomUUID();
        var clientId = UUID.randomUUID();
        var userId = UUID.randomUUID();
        var clientUserToken = testClientTokens.createClientUserToken(clientGroupId, clientId, userId);

        var cycleId = UUID.randomUUID();

        createTransactionCycle(() -> userId,
                (builder, i) -> builder
                        .userId(userId)
                        .cycleId(cycleId),
                cycle -> transactionCyclesService.upsert(cycle));

        // create datascience response
        WireMock.stubFor(
                WireMock.delete(urlPathMatching("/users/" + userId + "/transaction-cycles/" + cycleId))
                        .willReturn(ok()));

        // create cycle
        var mockRequest = delete("/v1/users/{userId}/enrichment-tasks/transaction-cycles/{cycleId}", userId, cycleId)
                .header(CLIENT_TOKEN_HEADER_NAME, clientUserToken.getSerialized())
                .contentType(MediaType.APPLICATION_JSON);

        var result = mockMvc.perform(mockRequest)
                .andExpect(request().asyncStarted())
                .andReturn();

        // wait for the completion
        var response = mockMvc.perform(asyncDispatch(result))
                .andExpect(status().isAccepted())
                .andReturn()
                .getResponse()
                .getContentAsString();

        // compare that all transaction cycles match (database, response) with set values
        var feedback = objectMapper.readValue(response, TransactionCyclesFeedbackResponse.class);
        var activityId = feedback.activityId;
        var inDatabase = transactionCyclesService.find(userId, cycleId)
                .orElseThrow(AssertionError::new);

        assertThat(inDatabase.isExpired()).isTrue();

        assertThat(activityEnrichmentService.find(activityId)).isNotEmpty();
    }
}