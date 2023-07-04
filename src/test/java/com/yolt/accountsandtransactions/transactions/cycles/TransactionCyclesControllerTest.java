package com.yolt.accountsandtransactions.transactions.cycles;

import com.yolt.accountsandtransactions.BaseIntegrationTest;
import nl.ing.lovebird.clienttokens.constants.ClientTokenConstants;
import nl.ing.lovebird.clienttokens.test.TestClientTokens;
import nl.ing.lovebird.logging.MDCContextCreator;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;

import java.util.List;
import java.util.UUID;

import static com.yolt.accountsandtransactions.TestBuilders.bulkCreateTransactionCycles;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

class TransactionCyclesControllerTest extends BaseIntegrationTest {

    @Autowired
    protected MockMvc mockMvc;

    @Autowired
    protected TransactionCyclesService transactionCyclesService;

    @Autowired
    private TestClientTokens testClientTokens;

    @BeforeEach
    public void onStart() {
        // Add some noise (random user, 10 random cycle-ids)
        bulkCreateTransactionCycles(10, UUID::randomUUID,
                (builder, i) -> builder, cycle -> {
                    transactionCyclesService.saveBatch(List.of(cycle));
                    return cycle;
                });
    }

    @Test
    public void testGetTransactionCycles() throws Exception {
        var clientGroupId = UUID.randomUUID();
        var clientId = UUID.randomUUID();

        var userId = UUID.randomUUID();

        var clientToken = testClientTokens.createClientUserToken(clientGroupId, clientId, userId);
        bulkCreateTransactionCycles(5, () -> userId,
                (builder, i) -> builder.cycleId(new UUID(1, i)), cycle -> {
                    transactionCyclesService.saveBatch(List.of(cycle));
                    return cycle;
                });

        mockMvc.perform(MockMvcRequestBuilders.get("/v1/users/" + userId + "/transaction-cycles")
                .header(ClientTokenConstants.CLIENT_TOKEN_HEADER_NAME, clientToken.getSerialized())
                .header(MDCContextCreator.USER_ID_HEADER_NAME, userId.toString())
                .contentType(MediaType.APPLICATION_JSON))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.cycles", hasSize(5)))
                .andExpect(jsonPath("$.cycles[0].cycleId", equalTo(new UUID(1, 0).toString())))
                .andExpect(jsonPath("$.cycles[1].cycleId", equalTo(new UUID(1, 1).toString())))
                .andExpect(jsonPath("$.cycles[2].cycleId", equalTo(new UUID(1, 2).toString())))
                .andExpect(jsonPath("$.cycles[3].cycleId", equalTo(new UUID(1, 3).toString())))
                .andExpect(jsonPath("$.cycles[4].cycleId", equalTo(new UUID(1, 4).toString())));
    }
}
