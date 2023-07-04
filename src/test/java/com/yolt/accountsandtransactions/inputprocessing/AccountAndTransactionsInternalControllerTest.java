package com.yolt.accountsandtransactions.inputprocessing;

import com.yolt.accountsandtransactions.BaseIntegrationTest;
import com.yolt.accountsandtransactions.TestUtils;
import com.yolt.accountsandtransactions.accounts.Account;
import nl.ing.lovebird.clienttokens.ClientUserToken;
import nl.ing.lovebird.clienttokens.test.TestClientTokens;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;

import java.io.ByteArrayOutputStream;
import java.util.List;
import java.util.UUID;
import java.util.zip.GZIPOutputStream;

import static nl.ing.lovebird.clienttokens.constants.ClientTokenConstants.CLIENT_TOKEN_HEADER_NAME;
import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

class AccountAndTransactionsInternalControllerTest extends BaseIntegrationTest {


    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private TestClientTokens testClientTokens;

    @Test
    public void when_AGzippedProviderAccountMessageIsSent_then_itShouldBeProcessed() throws Exception {

        // Given a message
        UUID userId = UUID.randomUUID();
        UUID accountId = UUID.randomUUID();
        AccountsAndTransactionsRequestDTO message = TestUtils.ingestionRequestSuccessMessage(userId, UUID.randomUUID(), UUID.randomUUID(), "123", "PROVIDER", UUID.randomUUID());
        ClientUserToken clientUserToken = testClientTokens.createClientUserToken(UUID.randomUUID(), UUID.randomUUID(), userId);
        stubNewAccounts(message, accountId);

        // when it is sent (gzipped)
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
             GZIPOutputStream gzipOutputStream = new GZIPOutputStream(baos)) {
            objectMapper.writeValue(gzipOutputStream, message);
            mockMvc.perform(post("/internal/users/{userId}/provider-accounts", userId.toString())
                            .content(baos.toByteArray())
                            .header(CLIENT_TOKEN_HEADER_NAME, clientUserToken.getSerialized())
                            .contentType(MediaType.APPLICATION_JSON))
                    .andExpect(status().isOk());
        }

        // Then it is processed
        List<Account> accounts = accountRepository.getAccounts(userId);
        assertThat(accounts).hasSize(1);
        assertThat(accounts.get(0).getId()).isEqualTo(accountId);

    }
}