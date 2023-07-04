package com.yolt.accountsandtransactions.legacytransactions;

import com.yolt.accountsandtransactions.BaseIntegrationTest;
import com.yolt.accountsandtransactions.MutableClock;
import com.yolt.accountsandtransactions.accounts.AccountService;
import com.yolt.accountsandtransactions.inputprocessing.AccountFromProviders;
import com.yolt.accountsandtransactions.inputprocessing.ProviderTransactionWithId;
import com.yolt.accountsandtransactions.inputprocessing.TransactionInsertionStrategy;
import com.yolt.accountsandtransactions.transactions.TransactionService;
import nl.ing.lovebird.clienttokens.constants.ClientTokenConstants;
import nl.ing.lovebird.clienttokens.test.TestClientTokens;
import nl.ing.lovebird.extendeddata.common.CurrencyCode;
import nl.ing.lovebird.extendeddata.transaction.TransactionStatus;
import nl.ing.lovebird.providerdomain.AccountType;
import nl.ing.lovebird.providerdomain.ProviderTransactionDTO;
import nl.ing.lovebird.providerdomain.ProviderTransactionType;
import nl.ing.lovebird.providerdomain.YoltCategory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.Matchers.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

public class LegacyTransactionIntegrationTest extends BaseIntegrationTest {
    private static final LocalDateTime pointInTime = LocalDateTime.of(2018, 9, 25, 12, 1, 11);

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private AccountService accountService;

    @Autowired
    private TransactionService transactionService;

    @Autowired
    private MutableClock clock;

    @Autowired
    private TestClientTokens testClientTokens;

    @BeforeEach
    public void before() {
        clock.asFixed(pointInTime);
    }

    @AfterEach
    public void after() {
        clock.reset();
    }

    @Test
    public void when_thereAreALotOfTransactions_then_ItShouldBePossibleToGetThemInPages() throws Exception {
        UUID clientGroupId = UUID.randomUUID();
        UUID clientId = UUID.randomUUID();
        UUID userId = UUID.randomUUID();
        var clientUserToken = testClientTokens.createClientUserToken(clientGroupId, clientId, userId);

        UUID accountId = UUID.randomUUID();
        UUID userSiteId = UUID.randomUUID();
        UUID siteId = UUID.randomUUID();

        AccountFromProviders accountFromProviders = AccountFromProviders.accountsFromProvidersBuilder()
                .name("savings account")
                .yoltUserId(userId)
                .yoltUserSiteId(userSiteId)
                .yoltSiteId(siteId)
                .yoltAccountType(AccountType.SAVINGS_ACCOUNT)
                .lastRefreshed(ZonedDateTime.now(clock))
                .currentBalance(new BigDecimal("1000.12"))
                .currency(CurrencyCode.EUR)
                .accountId("ext id")
                .provider("provider-X")
                .build();
        accountService.createOrUpdateAccount(clientUserToken, accountFromProviders, accountId, userSiteId, siteId, true, Instant.now(clock));

        List<ProviderTransactionWithId> transactionDTOS = new ArrayList<>();
        for (int i = 0; i < 102; i++) {
            ZonedDateTime now = ZonedDateTime.now(clock);
            transactionDTOS.add(
                    new ProviderTransactionWithId(
                            ProviderTransactionDTO.builder()
                                    .externalId(String.format("%03d", i))
                                    .dateTime(now)
                                    .amount(new BigDecimal("20.0"))
                                    .status(TransactionStatus.BOOKED)
                                    .type(ProviderTransactionType.CREDIT)
                                    .description("test")
                                    .category(YoltCategory.INCOME)
                                    .build()
                            , String.format("%03d", i))
            );
        }
        transactionService.saveTransactionsBatch(accountId, clientUserToken, accountFromProviders, transactionDTOS, TransactionInsertionStrategy.Instruction.InstructionType.INSERT);

        MvcResult mvcResult = mockMvc.perform(get("/legacy-transactions/transactions-by-account/me?accountId={accountId}", accountId.toString())
                        .header(ClientTokenConstants.CLIENT_TOKEN_HEADER_NAME, clientUserToken.getSerialized())
                        .contentType(MediaType.APPLICATION_JSON))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$._links.next", notNullValue()))
                .andExpect(jsonPath("$.transactions", hasSize(100)))
                .andExpect(jsonPath("$.transactions[*].id", hasItems(is("000"), is("001"), is("002"), is("003"), /* ..., */ is("099"))))
                .andReturn();
        String contentAsString = mvcResult.getResponse().getContentAsString();
        LegacyTransactionsByAccountDTO transactionsPage = objectMapper.readValue(contentAsString, LegacyTransactionsByAccountDTO.class);
        String nextLink = transactionsPage.getLinks().getNext().getHref();

        assertThat(nextLink).contains("accountId=" + accountId);
        assertThat(nextLink).contains("dateInterval=2008-09-25/2018-09-25");

        mockMvc.perform(get(nextLink.replace("/transactions/", "/legacy-transactions/"))
                        .header(ClientTokenConstants.CLIENT_TOKEN_HEADER_NAME, clientUserToken.getSerialized())
                        .contentType(MediaType.APPLICATION_JSON))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$._links.next", nullValue()))
                .andExpect(jsonPath("$.transactions", hasSize(2)))
                .andExpect(jsonPath("$.transactions[*].id", hasItems(is("100"), is("101"))));
    }
}
