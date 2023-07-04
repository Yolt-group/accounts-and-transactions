package com.yolt.accountsandtransactions.legacyaccounts;

import com.fasterxml.jackson.core.type.TypeReference;
import com.yolt.accountsandtransactions.BaseIntegrationTest;
import com.yolt.accountsandtransactions.accounts.Account;
import com.yolt.accountsandtransactions.accounts.AccountRepository;
import nl.ing.lovebird.clienttokens.ClientUserToken;
import nl.ing.lovebird.clienttokens.constants.ClientTokenConstants;
import nl.ing.lovebird.clienttokens.test.TestClientTokens;
import nl.ing.lovebird.extendeddata.account.Status;
import nl.ing.lovebird.extendeddata.account.UsageType;
import nl.ing.lovebird.extendeddata.common.CurrencyCode;
import nl.ing.lovebird.extendeddata.transaction.AccountReferenceType;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;

import java.math.BigDecimal;
import java.sql.Date;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Currency;
import java.util.List;
import java.util.UUID;

import static com.yolt.accountsandtransactions.TestBuilders.createAllFieldsRandomAccount;
import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.Matchers.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

public class LegacyAccountsIntegrationTest extends BaseIntegrationTest {
    public static final Currency CURRENCY_EUR = Currency.getInstance("EUR");

    private static final UUID accountId = UUID.randomUUID();

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private AccountRepository accountRepository;

    @Autowired
    private TestClientTokens testClientTokens;
    @Test
    void when_accountInRepo_then_itShouldBeQueryable() throws Exception {
        ClientUserToken clientUserToken = testClientTokens.createClientUserToken(UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID());

        Account dbAccount = createAllFieldsRandomAccount(clientUserToken.getUserIdClaim(), accountId);
        accountRepository.saveBatch(List.of(dbAccount), 1);

        var json = mockMvc.perform(get("/legacy-accounts/user-accounts/me")
                        .header(ClientTokenConstants.CLIENT_TOKEN_HEADER_NAME, clientUserToken.getSerialized())
                        .contentType(MediaType.APPLICATION_JSON))
                .andExpect(status().isOk())
                .andReturn()
                .getResponse()
                .getContentAsString();

        final List<LegacyAccountGroupDTO> accounts = objectMapper.readValue(json, new TypeReference<>() {
        });
        // There are several accounts in the response, we add assertions for the 1st one and assume the other accounts
        // are mapped in the same way.
        assertThat(accounts).hasSize(1);
        assertThat(accounts.get(0).accounts).hasSize(1);
        assertThat(accounts.get(0).type).isEqualTo(LegacyAccountType.CURRENT_ACCOUNT);
        assertThat(accounts.get(0).totals).hasSize(1);
        assertThat(accounts.get(0).totals.get(0).getTotal()).isEqualTo(BigDecimal.TEN);
        assertThat(accounts.get(0).totals.get(0).getCurrencyCode()).isEqualTo(CURRENCY_EUR);

        final LegacyAccountDTO account = accounts.get(0).accounts.get(0);
        assertThat(account.getId()).isEqualTo(accountId);
        assertThat(account.getExternalId()).isEqualTo("external-id");
        assertThat(account.getName()).isEqualTo("account-1");
        assertThat(account.getNickname()).isNull();
        assertThat(account.getCurrencyCode()).isEqualTo(CURRENCY_EUR);
        assertThat(account.getBalance()).isEqualTo(BigDecimal.TEN);
        assertThat(account.getAvailableBalance()).isNull(); // AVAILABLE not set
        assertThat(account.getLastRefreshed()).isEqualTo(Date.from(Instant.EPOCH));
        assertThat(account.getUpdated()).isEqualTo(Date.from(Instant.EPOCH));
        assertThat(account.getStatus()).isEqualTo(LegacyAccountStatusCode.DATASCIENCE_FINISHED);
        assertThat(account.getUserSite().getId()).isEqualTo(dbAccount.getUserSiteId());
        assertThat(account.getUserSite().getSiteId()).isEqualTo(dbAccount.getSiteId());
        assertThat(account.getType()).isEqualTo(LegacyAccountType.CURRENT_ACCOUNT);
        assertThat(account.isHidden()).isFalse();
        assertThat(account.isClosed()).isFalse();
        assertThat(account.isMigrated()).isTrue();
        assertThat(account.getAccountNumber().getHolderName()).isEqualTo("account-holder");
        assertThat(account.getAccountNumber().getScheme()).isEqualTo(LegacyAccountNumberDTO.Scheme.IBAN);
        assertThat(account.getAccountNumber().getIdentification()).isEqualTo("iban");
        assertThat(account.getAccountNumber().getSecondaryIdentification()).isNull();
        assertThat(account.getCustomAccountNumber()).isNull();
        assertThat(account.getAccountMaskedIdentification()).isEqualTo("masped-pan");
        assertThat(account.getExtendedAccount()).isNotNull();
        assertThat(account.getExtendedAccount().getResourceId()).isNull();
        assertThat(account.getExtendedAccount().getAccountReferences()).hasSize(5);
        assertThat(account.getExtendedAccount().getAccountReferences().get(0).getType()).isEqualTo(AccountReferenceType.IBAN);
        assertThat(account.getExtendedAccount().getAccountReferences().get(0).getValue()).isEqualTo("iban");
        assertThat(account.getExtendedAccount().getAccountReferences().get(1).getType()).isEqualTo(AccountReferenceType.BBAN);
        assertThat(account.getExtendedAccount().getAccountReferences().get(1).getValue()).isEqualTo("bban");
        assertThat(account.getExtendedAccount().getAccountReferences().get(2).getType()).isEqualTo(AccountReferenceType.MASKED_PAN);
        assertThat(account.getExtendedAccount().getAccountReferences().get(2).getValue()).isEqualTo("masped-pan");
        assertThat(account.getExtendedAccount().getAccountReferences().get(3).getType()).isEqualTo(AccountReferenceType.PAN);
        assertThat(account.getExtendedAccount().getAccountReferences().get(3).getValue()).isEqualTo("pan");
        assertThat(account.getExtendedAccount().getAccountReferences().get(4).getType()).isEqualTo(AccountReferenceType.SORTCODEACCOUNTNUMBER);
        assertThat(account.getExtendedAccount().getAccountReferences().get(4).getValue()).isEqualTo("sort-code");
        assertThat(account.getExtendedAccount().getCurrency()).isEqualTo(CurrencyCode.EUR);
        assertThat(account.getExtendedAccount().getName()).isEqualTo("account-1");
        assertThat(account.getExtendedAccount().getProduct()).isEqualTo("product");
        assertThat(account.getExtendedAccount().getCashAccountType()).isNull();
        assertThat(account.getExtendedAccount().getStatus()).isEqualTo(Status.ENABLED);
        assertThat(account.getExtendedAccount().getBic()).isEqualTo("bic");
        assertThat(account.getExtendedAccount().getLinkedAccounts()).isEqualTo("linked-account");
        assertThat(account.getExtendedAccount().getUsage()).isEqualTo(UsageType.CORPORATE);
        assertThat(account.getExtendedAccount().getDetails()).isNull();
        assertThat(account.getExtendedAccount().getBalances()).hasSize(1);
        assertThat(account.getExtendedAccount().getBalances().get(0).getBalanceAmount().getAmount()).isEqualTo(BigDecimal.TEN);
        assertThat(account.getExtendedAccount().getBalances().get(0).getBalanceAmount().getCurrency()).isEqualTo(CurrencyCode.EUR);
        assertThat(account.getExtendedAccount().getBalances().get(0).getLastChangeDateTime()).isEqualTo(ZonedDateTime.ofInstant(Instant.EPOCH, ZoneOffset.UTC));
        assertThat(account.getExtendedAccount().getBalances().get(0).getReferenceDate()).isNull();
        assertThat(account.getExtendedAccount().getBalances().get(0).getLastCommittedTransaction()).isNull();
        assertThat(account.getYoltAccountType()).isNull();
        assertThat(account.getLinkedAccount()).isEqualTo("linked-account");
        assertThat(account.getBankSpecific()).hasSize(1);
        assertThat(account.getBankSpecific().get("Key")).isEqualTo("value");
        assertThat(account.getLinks()).isNotNull();
        assertThat(account.getLinks().getTransactions().getHref()).isEqualTo("/transactions/transactions-by-account/me?accountId=" + accountId);
        assertThat(account.getLinks().getHideAccounts().getHref()).isEqualTo("/accounts/user-accounts/me/accounts/hide-unhide");
    }

    @Test
    void when_hidingAccount_then_itUpdateTotals() throws Exception {
        ClientUserToken clientUserToken = testClientTokens.createClientUserToken(UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID());

        Account dbAccount = createAllFieldsRandomAccount(clientUserToken.getUserIdClaim(), accountId);
        accountRepository.saveBatch(List.of(dbAccount), 1);

        mockMvc.perform(get("/legacy-accounts/user-accounts/me")
                        .header(ClientTokenConstants.CLIENT_TOKEN_HEADER_NAME, clientUserToken.getSerialized())
                        .contentType(MediaType.APPLICATION_JSON))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.*", hasSize(1)))
                .andExpect(jsonPath("$[0].accounts", hasSize(1)))
                .andExpect(jsonPath("$[0].accounts[0].balance", is(10)))
                .andExpect(jsonPath("$[0].accounts[0].hidden", is(false)))
                .andExpect(jsonPath("$[0].totals", hasSize(1)))
                .andExpect(jsonPath("$[0].totals[0].total", is(10)))
                .andExpect(jsonPath("$[0].totals[0].currencyCode", is("EUR")));

        LegacyAccountDTO accountDTO = LegacyAccountDTO.builder().id(accountId).hidden(Boolean.TRUE).build();

        mockMvc.perform(post("/legacy-accounts/user-accounts/me/accounts/hide-unhide")
                        .content(objectMapper.writeValueAsString(new LegacyAccountsDTO(List.of(accountDTO))))
                        .header(ClientTokenConstants.CLIENT_TOKEN_HEADER_NAME, clientUserToken.getSerialized())
                        .contentType(MediaType.APPLICATION_JSON))
                .andExpect(status().isOk());

        mockMvc.perform(get("/legacy-accounts/user-accounts/me")
                        .header(ClientTokenConstants.CLIENT_TOKEN_HEADER_NAME, clientUserToken.getSerialized())
                        .contentType(MediaType.APPLICATION_JSON))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.*", hasSize(1)))
                .andExpect(jsonPath("$[0].accounts", hasSize(1)))
                .andExpect(jsonPath("$[0].accounts[0].balance", is(10)))
                .andExpect(jsonPath("$[0].accounts[0].hidden", is(true)))
                .andExpect(jsonPath("$[0].totals", empty()));

        accountDTO = accountDTO.toBuilder().hidden(Boolean.FALSE).build();

        mockMvc.perform(post("/legacy-accounts/user-accounts/me/accounts/hide-unhide")
                        .content(objectMapper.writeValueAsString(new LegacyAccountsDTO(List.of(accountDTO))))
                        .header(ClientTokenConstants.CLIENT_TOKEN_HEADER_NAME, clientUserToken.getSerialized())
                        .contentType(MediaType.APPLICATION_JSON))
                .andExpect(status().isOk());


        mockMvc.perform(get("/legacy-accounts/user-accounts/me")
                        .header(ClientTokenConstants.CLIENT_TOKEN_HEADER_NAME, clientUserToken.getSerialized())
                        .contentType(MediaType.APPLICATION_JSON))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.*", hasSize(1)))
                .andExpect(jsonPath("$[0].accounts", hasSize(1)))
                .andExpect(jsonPath("$[0].accounts[0].balance", is(10)))
                .andExpect(jsonPath("$[0].accounts[0].hidden", is(false)))
                .andExpect(jsonPath("$[0].totals", hasSize(1)))
                .andExpect(jsonPath("$[0].totals[0].total", is(10)))
                .andExpect(jsonPath("$[0].totals[0].currencyCode", is("EUR")));
    }
}
