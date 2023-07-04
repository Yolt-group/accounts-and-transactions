package com.yolt.accountsandtransactions;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.tomakehurst.wiremock.client.WireMock;
import com.jayway.jsonpath.JsonPath;
import com.yolt.accountsandtransactions.accounts.Account;
import com.yolt.accountsandtransactions.accounts.AccountDTO;
import com.yolt.accountsandtransactions.accounts.AccountService;
import com.yolt.accountsandtransactions.accounts.event.AccountEvent;
import com.yolt.accountsandtransactions.inputprocessing.AccountFromProviders;
import com.yolt.accountsandtransactions.inputprocessing.AccountsAndTransactionsRequestDTO;
import com.yolt.accountsandtransactions.inputprocessing.ProviderTransactionWithId;
import com.yolt.accountsandtransactions.inputprocessing.TransactionInsertionStrategy.Instruction.InstructionType;
import com.yolt.accountsandtransactions.transactions.TransactionService;
import com.yolt.accountsandtransactions.transactions.TransactionsPageDTO;
import nl.ing.lovebird.clienttokens.ClientUserToken;
import nl.ing.lovebird.clienttokens.test.TestClientTokens;
import nl.ing.lovebird.extendeddata.account.BalanceDTO;
import nl.ing.lovebird.extendeddata.account.BalanceType;
import nl.ing.lovebird.extendeddata.account.ExtendedAccountDTO;
import nl.ing.lovebird.extendeddata.common.AccountReferenceDTO;
import nl.ing.lovebird.extendeddata.common.BalanceAmountDTO;
import nl.ing.lovebird.extendeddata.common.CurrencyCode;
import nl.ing.lovebird.extendeddata.transaction.AccountReferenceType;
import nl.ing.lovebird.extendeddata.transaction.ExchangeRateDTO;
import nl.ing.lovebird.extendeddata.transaction.ExtendedTransactionDTO;
import nl.ing.lovebird.extendeddata.transaction.TransactionStatus;
import nl.ing.lovebird.providerdomain.*;
import org.awaitility.Duration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.stream.Collectors;

import static com.github.tomakehurst.wiremock.client.WireMock.noContent;
import static com.github.tomakehurst.wiremock.client.WireMock.urlPathMatching;
import static com.yolt.accountsandtransactions.accounts.event.AccountEventType.UPDATED;
import static nl.ing.lovebird.clienttokens.constants.ClientTokenConstants.CLIENT_TOKEN_HEADER_NAME;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.Matchers.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.delete;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultHandlers.print;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

public class NewAccountsAndTransactionAPIIntegrationTest extends BaseIntegrationTest {

    private static final LocalDateTime pointInTime = LocalDateTime.of(2018, 9, 25, 12, 1, 11);

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private KafkaTemplate<String, AccountFromProviders> stringKafkaTemplate;

    @Autowired
    private ActivityEventsTestConsumer activityEventsTestConsumer;

    @Autowired
    private AccountEventsTestConsumer accountEventsTestConsumer;

    @Autowired
    private TransactionService transactionService;

    @Autowired
    private AccountService accountService;

    @Autowired
    private ObjectMapper objectMapper;

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
    public void when_newAccountAndTransactionsComeIn_then_itShouldBeQueryableOnTheNewAPIs() throws Exception {
        UUID clientGroupId = UUID.randomUUID();
        UUID clientId = UUID.randomUUID();

        UUID siteId = UUID.randomUUID();
        UUID activityId = UUID.randomUUID();
        UUID userSiteId = UUID.randomUUID();
        UUID userId = UUID.randomUUID();
        UUID internalCurrentAccountId = UUID.randomUUID();
        UUID internalCreditAccountId = UUID.randomUUID();
        UUID internalSavingsAccountId = UUID.randomUUID();

        ClientUserToken clientUserToken = testClientTokens.createClientUserToken(clientGroupId, clientId, userId);

        List<ProviderTransactionDTO> currentAccountTransactions = new ArrayList<>();
        currentAccountTransactions.add(ProviderTransactionDTO.builder()
                .externalId("1")
                .dateTime(ZonedDateTime.of(2017, 10, 4, 0, 0, 0, 0, ZoneId.systemDefault()))
                .amount(new BigDecimal("10.0"))
                .status(TransactionStatus.BOOKED)
                .type(ProviderTransactionType.DEBIT)
                .description("Transaction 1")
                .category(YoltCategory.GENERAL)
                .build());
        currentAccountTransactions.add(ProviderTransactionDTO.builder()
                .externalId("2")
                .dateTime(ZonedDateTime.of(2018, 10, 4, 0, 0, 0, 0, ZoneId.systemDefault()))
                .amount(new BigDecimal("20.0"))
                .status(TransactionStatus.BOOKED)
                .type(ProviderTransactionType.DEBIT)
                .description("Transaction 2")
                .category(YoltCategory.GENERAL)
                .build());
        currentAccountTransactions.add(ProviderTransactionDTO.builder()
                .externalId("3")
                .dateTime(ZonedDateTime.of(2019, 10, 4, 0, 0, 0, 0, ZoneId.systemDefault()))
                .amount(new BigDecimal("30.0"))
                .status(TransactionStatus.BOOKED)
                .type(ProviderTransactionType.DEBIT)
                .category(YoltCategory.GENERAL)
                .description("Transaction 3")
                .build());
        currentAccountTransactions.add(ProviderTransactionDTO.builder()
                .externalId("hyyhyhyhyhyyhyhyhhyhyhyhy")
                .dateTime(ZonedDateTime.of(2020, 10, 4, 0, 0, 0, 0, ZoneId.systemDefault()))
                .amount(new BigDecimal("22.33"))
                .status(TransactionStatus.PENDING)
                .type(ProviderTransactionType.DEBIT)
                .category(YoltCategory.GENERAL)
                .description("Mc Donalds Spaklerweg Amsterdam")
                .extendedTransaction(ExtendedTransactionDTO.builder()
                        .transactionAmount(BalanceAmountDTO.builder().amount(new BigDecimal("-22.33")).build())
                        .endToEndId("90705030")
                        .creditorName("John")
                        .creditorAccount(new AccountReferenceDTO(AccountReferenceType.IBAN, "NL79ABNA9455762838"))
                        .creditorId("123qwe")
                        .debtorName("Marie")
                        .debtorAccount(new AccountReferenceDTO(AccountReferenceType.SORTCODEACCOUNTNUMBER, "56-34-2192282828"))
                        .exchangeRate(Collections.singletonList(new ExchangeRateDTO(CurrencyCode.PLN, "0.23",
                                CurrencyCode.EUR, null, null, null)))
                        .build())
                .build());
        currentAccountTransactions.add(ProviderTransactionDTO.builder()
                .externalId("will-be-removed")
                .dateTime(ZonedDateTime.of(2017, 10, 3, 0, 0, 0, 0, ZoneId.systemDefault()))
                .amount(new BigDecimal("15.0"))
                .status(TransactionStatus.HOLD)
                .type(ProviderTransactionType.DEBIT)
                .description("DIRECT DEBIT Energies")
                .category(YoltCategory.LEISURE)
                .merchant("merchantY")
                .extendedTransaction(
                        ExtendedTransactionDTO.builder()
                                .transactionAmount(BalanceAmountDTO.builder().amount(new BigDecimal("-15")).build())
                                .entryReference("ENT1234")
                                .creditorName("JCVD")
                                .ultimateCreditor("STVSGL")
                                .debtorName("JHNRMBO")
                                .purposeCode("WHOKNOWS")
                                .bankTransactionCode("YACHA")
                                .build()
                )
                .build());
        List<ProviderTransactionDTO> savingsAccountTransactions = new ArrayList<>();

        List<ProviderTransactionDTO> creditCardTransaction = new ArrayList<>();
        creditCardTransaction.add(ProviderTransactionDTO.builder()
                .externalId("12345x")
                .dateTime(ZonedDateTime.of(2017, 10, 4, 0, 0, 0, 0, ZoneId.systemDefault()))
                .amount(new BigDecimal("20.0"))
                .status(TransactionStatus.BOOKED)
                .type(ProviderTransactionType.CREDIT)
                .description("Transaction XYZ")
                .category(YoltCategory.INCOME)
                .merchant("merchantX")
                .extendedTransaction(
                        ExtendedTransactionDTO.builder()
                                .transactionAmount(BalanceAmountDTO.builder().amount(new BigDecimal("20")).build())
                                .entryReference("ENT1234")
                                .creditorName("JCVD")
                                .ultimateCreditor("STVSGL")
                                .debtorName("JHNRMBO")
                                .purposeCode("WHOKNOWS")
                                .bankTransactionCode("YACHA")
                                .build()
                ).build());
        creditCardTransaction.add(ProviderTransactionDTO.builder()
                .externalId("12345y")
                .dateTime(ZonedDateTime.of(2017, 10, 3, 0, 0, 0, 0, ZoneId.systemDefault()))
                .amount(new BigDecimal("10.0"))
                .status(TransactionStatus.PENDING)
                .type(ProviderTransactionType.DEBIT)
                .description("DIRECT DEBIT Energies")
                .category(YoltCategory.LEISURE)
                .merchant("merchantY")
                .extendedTransaction(
                        ExtendedTransactionDTO.builder()
                                .transactionAmount(BalanceAmountDTO.builder().amount(new BigDecimal("-10")).build())
                                .entryReference("ENT1234")
                                .creditorName("JCVD")
                                .ultimateCreditor("STVSGL")
                                .debtorName("JHNRMBO")
                                .purposeCode("WHOKNOWS")
                                .bankTransactionCode("YACHA")
                                .build()
                )
                .build());

        ProviderAccountNumberDTO savingsAccountProviderAccountNumberDTO = new ProviderAccountNumberDTO(ProviderAccountNumberDTO.Scheme.SORTCODEACCOUNTNUMBER, "somesortcodeaccountnumber");
        savingsAccountProviderAccountNumberDTO.setHolderName("John Doe");
        ProviderAccountNumberDTO currentAccountProviderAccountNumberDTO = new ProviderAccountNumberDTO(ProviderAccountNumberDTO.Scheme.SORTCODEACCOUNTNUMBER, "12-34-5612345678");
        currentAccountProviderAccountNumberDTO.setHolderName("John Doe");
        ProviderAccountNumberDTO creditAccountProviderAccountNumberDTO = new ProviderAccountNumberDTO(null, null);
        creditAccountProviderAccountNumberDTO.setHolderName("John Doe");
        HashMap<String, String> bankSpecific = new HashMap<>();
        bankSpecific.put("ing.rentepunten.amount", "123");
        AccountsAndTransactionsRequestDTO accountsAndTransactionsRequestDTO = new AccountsAndTransactionsRequestDTO(
                activityId,
                Arrays.asList(
                        AccountFromProviders.accountsFromProvidersBuilder()
                                .yoltUserId(userId)
                                .yoltUserSiteId(userSiteId)
                                .yoltSiteId(siteId)
                                .yoltAccountType(AccountType.SAVINGS_ACCOUNT)
                                .lastRefreshed(ZonedDateTime.now())
                                .currentBalance(new BigDecimal("1000.12"))
                                .currency(CurrencyCode.EUR)
                                .accountId("savings card external id")
                                .name("John's savings account")
                                .provider("provider-X")
                                .transactions(savingsAccountTransactions)
                                .accountNumber(savingsAccountProviderAccountNumberDTO)
                                .bic("RABONL2U")
                                .extendedAccount(ExtendedAccountDTO.builder()
                                        .product("Zilvervlootrekening-savings")
                                        .accountReferences(Collections.singletonList(
                                                new AccountReferenceDTO(AccountReferenceType.IBAN, "NL79ABNA9455762838")))
                                        .balances(List.of(BalanceDTO.builder()
                                                .balanceAmount(BalanceAmountDTO.builder().amount(BigDecimal.ONE).currency(CurrencyCode.EUR).build())
                                                .balanceType(BalanceType.AVAILABLE)
                                                .lastChangeDateTime(ZonedDateTime.now())
                                                .build()))
                                        .build())
                                .build(),
                        AccountFromProviders.accountsFromProvidersBuilder()
                                .yoltUserId(userId)
                                .yoltUserSiteId(userSiteId)
                                .yoltSiteId(siteId)
                                .yoltAccountType(AccountType.CURRENT_ACCOUNT)
                                .lastRefreshed(ZonedDateTime.now())
                                .availableBalance(new BigDecimal("240.45"))
                                .currency(CurrencyCode.EUR)
                                .accountId("current account external id")
                                .name("John's current account")
                                .provider("provider-X")
                                .transactions(currentAccountTransactions)
                                .accountNumber(currentAccountProviderAccountNumberDTO)
                                .extendedAccount(ExtendedAccountDTO.builder()
                                        .product("Zilvervlootrekening-current")
                                        .accountReferences(Arrays.asList(
                                                new AccountReferenceDTO(AccountReferenceType.IBAN, "NL79ABNA9455762838"),
                                                new AccountReferenceDTO(AccountReferenceType.BBAN, "5390 0754 7034"),
                                                new AccountReferenceDTO(AccountReferenceType.SORTCODEACCOUNTNUMBER, "12-34-5612345678"),
                                                new AccountReferenceDTO(AccountReferenceType.MASKED_PAN, "maskedpan?"),
                                                new AccountReferenceDTO(AccountReferenceType.PAN, "somepan")
                                        ))
                                        .build())
                                .bic("RABONL2U")
                                .bankSpecific(bankSpecific)
                                .build(),
                        AccountFromProviders.accountsFromProvidersBuilder()
                                .yoltUserId(userId)
                                .yoltUserSiteId(userSiteId)
                                .yoltSiteId(siteId)
                                .yoltAccountType(AccountType.CREDIT_CARD)
                                .lastRefreshed(ZonedDateTime.now())
                                .availableBalance(new BigDecimal("240.45"))
                                .currency(CurrencyCode.EUR)
                                .accountId("credit card external id")
                                .name("American Express Blue")
                                .provider("provider-X")
                                .transactions(creditCardTransaction)
                                .accountNumber(creditAccountProviderAccountNumberDTO)
                                .extendedAccount(ExtendedAccountDTO.builder()
                                        .product("American Express Blue")
                                        .accountReferences(List.of(
                                                new AccountReferenceDTO(AccountReferenceType.MASKED_PAN, "XXXXXXXXXX1234")
                                        ))
                                        .build())
                                .creditCardData(ProviderCreditCardDTO.builder()
                                        .cashApr(new BigDecimal("1.1"))
                                        .dueAmount(new BigDecimal("2.2"))
                                        .dueDate(ZonedDateTime.of(2017, 11, 1, 0, 0, 0, 0, ZoneId.of("UTC")))
                                        .availableCreditAmount(new BigDecimal("9000.0"))
                                        .runningBalanceAmount(new BigDecimal("4356.34"))
                                        .minPaymentAmount(new BigDecimal("3.33"))
                                        .newChargesAmount(new BigDecimal("4.44"))
                                        .lastPaymentAmount(new BigDecimal("55.5"))
                                        .lastPaymentDate(ZonedDateTime.of(2017, 11, 1, 0, 0, 0, 0, ZoneId.systemDefault()))
                                        .totalCreditLineAmount(new BigDecimal("6.66"))
                                        .cashLimitAmount(new BigDecimal("7.77"))
                                        .apr(new BigDecimal("8.88"))
                                        .build())
                                .build()),
                userSiteId,
                siteId
        );


        stubNewAccounts(accountsAndTransactionsRequestDTO, internalSavingsAccountId, internalCurrentAccountId, internalCreditAccountId);

        Message<AccountsAndTransactionsRequestDTO> message = MessageBuilder
                .withPayload(accountsAndTransactionsRequestDTO)
                .setHeader(KafkaHeaders.TOPIC, TestConfiguration.INPUT_TOPIC)
                .setHeader(KafkaHeaders.MESSAGE_KEY, userId.toString())
                .setHeader(CLIENT_TOKEN_HEADER_NAME, clientUserToken.getSerialized())
                .build();
        stringKafkaTemplate.send(message);

        await().atMost(Duration.TEN_SECONDS).untilAsserted(() -> assertThat(activityEventsTestConsumer.getConsumed()
                .stream()
                .filter(it -> it.getAbstractEvent().getActivityId().equals(activityId)))
                .hasSize(1));

        var json = mockMvc.perform(get("/v1/users/" + userId + "/accounts?userSiteId=" + userSiteId)
                        .header(CLIENT_TOKEN_HEADER_NAME, clientUserToken.getSerialized())
                        .contentType(MediaType.APPLICATION_JSON))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$", hasSize(3)))
                .andReturn()
                .getResponse()
                .getContentAsString();
        List<AccountDTO> accounts = objectMapper.readValue(json, new TypeReference<>() {
        });
        // There are several accounts in the response, we add assertions for the 1st one and assume the other accounts
        // are mapped in the same way.
        AccountDTO account = accounts.stream().filter(a -> a.getType() == AccountType.SAVINGS_ACCOUNT).findFirst().orElseThrow();
        assertThat(account.getId()).isEqualTo(internalSavingsAccountId);
        assertThat(account.getExternalId()).isEqualTo("savings card external id");
        assertThat(account.getType()).isEqualTo(AccountType.SAVINGS_ACCOUNT);
        assertThat(account.getUserSite().getUserSiteId()).isEqualTo(userSiteId);
        assertThat(account.getUserSite().getSiteId()).isEqualTo(siteId);
        assertThat(account.getCurrency()).isEqualTo(CurrencyCode.EUR);
        assertThat(account.getBalance()).isNotNull();
        assertThat(account.getStatus()).isEqualTo(Account.Status.ENABLED);
        assertThat(account.getName()).isEqualTo("John's savings account");
        assertThat(account.getProduct()).isEqualTo("Zilvervlootrekening-savings");
        assertThat(account.getAccountHolder()).isEqualTo("John Doe");
        // fixme bit weird that both IBAN and sortCodeAccountnumber are non-null.
        assertThat(account.getAccountReferences().getIban()).isEqualTo("NL79ABNA9455762838");
        assertThat(account.getAccountReferences().getSortCodeAccountNumber()).isEqualTo("somesortcodeaccountnumber");
        assertThat(account.getAccountReferences().getBban()).isNull();
        assertThat(account.getAccountReferences().getPan()).isNull();
        assertThat(account.getAccountReferences().getMaskedPan()).isNull();
        assertThat(account.getSavingsAccount().getBic()).isEqualTo("RABONL2U");
        // fixme is this field ever filled at all, it doesn't look like it?
        assertThat(account.getSavingsAccount().getIsMoneyPotOf()).isNull();
        assertThat(account.getLastDataFetchTime()).isAfter(Instant.now(clock).minus(1, ChronoUnit.MINUTES));
        assertThat(account.getBalances()).hasSize(1);
        // fixme why is this not a CurrencyCode but a String?
        assertThat(account.getBalances().get(0).getCurrency()).isEqualTo("EUR");
        assertThat(account.getBalances().get(0).getAmount()).isNotNull();
        assertThat(account.getBalances().get(0).getType()).isEqualTo(BalanceType.AVAILABLE);
        assertThat(account.getCreatedAt()).isEqualTo(Instant.parse("2018-09-25T12:01:11Z"));

        mockMvc.perform(get("/v1/users/" + userId + "/accounts?userSiteId=" + UUID.randomUUID())
                        .header(CLIENT_TOKEN_HEADER_NAME, clientUserToken.getSerialized())
                        .contentType(MediaType.APPLICATION_JSON))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$", hasSize(0)));


        mockMvc.perform(get("/v1/users/" + userId + "/transactions?accountIds=" + UUID.randomUUID() + "&dateInterval=2016-07-01/2016-07-31")
                        .header(CLIENT_TOKEN_HEADER_NAME, clientUserToken.getSerialized())
                        .contentType(MediaType.APPLICATION_JSON))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.next", nullValue()))
                .andExpect(jsonPath("$.transactions", hasSize(0)));

        mockMvc.perform(get("/v1/users/" + userId + "/transactions?accountIds= " + internalCurrentAccountId + "&dateInterval=2016-07-31/2020-07-01")
                        .header(CLIENT_TOKEN_HEADER_NAME, clientUserToken.getSerialized())
                        .contentType(MediaType.APPLICATION_JSON))
                .andExpect(status().isOk())
                // 1 of them is not in the timerange.
                .andExpect(jsonPath("$.transactions", hasSize(3)));


        // Get transactionIds first, these are generate server-side.
        String res = mockMvc.perform(get("/v1/users/" + userId + "/transactions?dateInterval=2020-10-03/2020-10-05")
                        .header(CLIENT_TOKEN_HEADER_NAME, clientUserToken.getSerialized())
                        .contentType(MediaType.APPLICATION_JSON))
                .andReturn()
                .getResponse()
                .getContentAsString();
        String id = JsonPath.compile("$.transactions[0].id").read(res);

        mockMvc.perform(get("/v1/users/" + userId + "/transactions?dateInterval=2020-10-03/2020-10-05")
                        .header(CLIENT_TOKEN_HEADER_NAME, clientUserToken.getSerialized())
                        .contentType(MediaType.APPLICATION_JSON))
                .andExpect(status().isOk())
                .andDo(print())
                .andExpect(jsonPath("$.transactions", hasSize(1)))
                .andExpect(content().json("{\"transactions\" : [{\n" +
                        "  \"id\": \"" + id + "\",\n" +
                        "  \"accountId\": \"" + internalCurrentAccountId + "\",\n" +
                        "  \"status\": \"PENDING\",\n" +
                        "  \"date\": \"2020-10-04\",\n" +
                        "  \"amount\": -22.33,\n" +
                        "  \"currency\": \"EUR\",\n" +
                        "  \"description\": \"Mc Donalds Spaklerweg Amsterdam\",\n" +
                        "  \"endToEndId\": \"90705030\",\n" +
                        "  \"creditor\": {\n" +
                        "    \"name\": \"John\",\n" +
                        "    \"accountReferences\":  {\n" +
                        "      \"iban\":  \"NL79ABNA9455762838\"\n" +
                        "    }\n" +
                        "  },\n" +
                        "  \"debtor\": {\n" +
                        "    \"name\": \"Marie\",\n" +
                        "    \"accountReferences\":  {\n" +
                        "      \"sortCodeAccountNumber\": \"56-34-2192282828\"\n" +
                        "    }\n" +
                        "  },\n" +
                        "  \"exchangeRate\": {\n" +
                        "    \"currencyFrom\": \"PLN\",\n" +
                        "    \"currencyTo\": \"EUR\",\n" +
                        "    \"rate\": 0.23\n" +
                        "  },\n" +
                        "  \"createdAt\": \"2018-09-25T12:01:11+0000\"\n" +
                        "}]" +
                        "}"));

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
        for (int i = 0; i < 999; i++) {
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
        transactionService.saveTransactionsBatch(accountId, clientUserToken, accountFromProviders, transactionDTOS, InstructionType.INSERT);

        MvcResult mvcResult = mockMvc.perform(get("/v1/users/" + userId + "/transactions")
                        .header(CLIENT_TOKEN_HEADER_NAME, clientUserToken.getSerialized())
                        .contentType(MediaType.APPLICATION_JSON))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.next", notNullValue()))
                .andExpect(jsonPath("$.transactions", hasSize(100)))
                .andExpect(jsonPath("$.transactions[*].id", hasItems(is("000"), is("001"), is("002"), is("003"), /* ..., */ is("099"))))
                .andReturn();
        String contentAsString = mvcResult.getResponse().getContentAsString();
        TransactionsPageDTO transactionsPage = objectMapper.readValue(contentAsString, TransactionsPageDTO.class);
        String next = transactionsPage.getNext();

        mockMvc.perform(get("/v1/users/" + userId + "/transactions?next=" + next)
                        .header(CLIENT_TOKEN_HEADER_NAME, clientUserToken.getSerialized())
                        .contentType(MediaType.APPLICATION_JSON))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.next", notNullValue()))
                .andExpect(jsonPath("$.transactions", hasSize(100)))
                .andExpect(jsonPath("$.transactions[*].id", hasItems(is("100"), is("101"), is("102"), /* ..., */ is("199"))));
    }

    @Test
    public void when_accountsAreDeleted_then_allTheAccountsAndTransactionsShouldBeDeleted() throws Exception {
        UUID clientGroupId = UUID.randomUUID();
        UUID clientId = UUID.randomUUID();

        UUID userId = UUID.randomUUID();
        UUID userSiteId1 = UUID.randomUUID();
        UUID userSiteId2 = UUID.randomUUID();
        UUID accountId1 = UUID.randomUUID();
        UUID accountId2 = UUID.randomUUID();

        ClientUserToken clientUserToken = testClientTokens.createClientUserToken(clientGroupId, clientId, userId);

        // add account1-usersite1, 1 transaction
        final AccountFromProviders accountFromProviders = AccountFromProviders.accountsFromProvidersBuilder()
                .name("account")
                .yoltUserId(userId)
                .yoltAccountType(AccountType.CURRENT_ACCOUNT)
                .currentBalance(new BigDecimal("1000.12"))
                .currency(CurrencyCode.EUR)
                .accountId("ext id")
                .provider("Abn Amro")
                .build();
        accountService.createOrUpdateAccount(clientUserToken, accountFromProviders, accountId1, userSiteId1, UUID.randomUUID(), true, Instant.now(clock));
        transactionService.saveTransactionsBatch(accountId1, clientUserToken, accountFromProviders, List.of(new ProviderTransactionWithId(ProviderTransactionDTO.builder()
                .externalId("1")
                .dateTime(ZonedDateTime.now(clock))
                .amount(new BigDecimal("20.0"))
                .status(TransactionStatus.BOOKED)
                .type(ProviderTransactionType.CREDIT)
                .description("test")
                .category(YoltCategory.INCOME)
                .build(), "1")
        ), InstructionType.INSERT);

        // add account2-usersite2, 1 transaction
        accountService.createOrUpdateAccount(clientUserToken, accountFromProviders, accountId2, userSiteId2, UUID.randomUUID(), true, Instant.now(clock));
        transactionService.saveTransactionsBatch(accountId2, clientUserToken, accountFromProviders, List.of(new ProviderTransactionWithId(ProviderTransactionDTO.builder()
                .externalId("1")
                .dateTime(ZonedDateTime.now(clock))
                .amount(new BigDecimal("20.0"))
                .status(TransactionStatus.BOOKED)
                .type(ProviderTransactionType.CREDIT)
                .description("test")
                .category(YoltCategory.INCOME)
                .build(), "1")
        ), InstructionType.INSERT);

        // get all, assert 2 transactions.
        mockMvc.perform(get("/v1/users/" + userId + "/transactions")
                        .header(CLIENT_TOKEN_HEADER_NAME, clientUserToken.getSerialized())
                        .contentType(MediaType.APPLICATION_JSON))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.transactions", hasSize(2)));

        // mock datascience responses
        final String accountDeletePath = "/users/" + userId + "/accounts/" + accountId1;
        WireMock.stubFor(
                WireMock.delete(urlPathMatching(accountDeletePath))
                        .willReturn(noContent()));

        // delete accounts from usersite1
        mockMvc.perform(delete("/v1/users/" + userId + "/accounts?userSiteId=" + userSiteId1)
                        .header(CLIENT_TOKEN_HEADER_NAME, clientUserToken.getSerialized())
                        .contentType(MediaType.APPLICATION_JSON))
                .andExpect(status().isOk());

        // verify datascience was called
        WireMock.verify(2, WireMock.deleteRequestedFor(WireMock.urlPathMatching(accountDeletePath)));

        // assert just 1 transaction left.
        mockMvc.perform(get("/v1/users/" + userId + "/transactions")
                        .header(CLIENT_TOKEN_HEADER_NAME, clientUserToken.getSerialized())
                        .contentType(MediaType.APPLICATION_JSON))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.transactions", hasSize(1)));

        // assert only account 2 left.
        mockMvc.perform(get("/v1/users/" + userId + "/accounts")
                        .header(CLIENT_TOKEN_HEADER_NAME, clientUserToken.getSerialized())
                        .contentType(MediaType.APPLICATION_JSON))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$", hasSize(1)))
                .andExpect(jsonPath("$[0].id", is(accountId2.toString())));
    }

    @Test
    public void when_anAccountIsUpdatedWeSendAKafkaMessage() {
        UUID clientGroupId = UUID.randomUUID();
        UUID clientId = UUID.randomUUID();

        UUID userId = UUID.randomUUID();
        UUID accountId = UUID.randomUUID();
        UUID userSiteId = UUID.randomUUID();
        UUID siteId = UUID.randomUUID();
        ProviderAccountNumberDTO accountNumber = new ProviderAccountNumberDTO(ProviderAccountNumberDTO.Scheme.IBAN, "");
        accountNumber.setHolderName("George Whittaker");

        ClientUserToken clientUserToken = testClientTokens.createClientUserToken(clientGroupId, clientId, userId);

        accountService.createOrUpdateAccount(clientUserToken, AccountFromProviders.accountsFromProvidersBuilder()
                .name("savings account")
                .yoltUserId(userId)
                .yoltUserSiteId(userSiteId)
                .yoltSiteId(siteId)
                .yoltAccountType(AccountType.SAVINGS_ACCOUNT)
                .accountNumber(accountNumber)
                .lastRefreshed(ZonedDateTime.now(clock))
                .currentBalance(new BigDecimal("1000.12"))
                .currency(CurrencyCode.EUR)
                .accountId("ext id")
                .provider("provider-X")
                .build(), accountId, userSiteId, siteId, true, Instant.now(clock));

        await().atMost(Duration.FIVE_SECONDS).untilAsserted(() -> {
            List<AccountEvent> accountEvents = accountEventsTestConsumer.getConsumed().stream()
                    .map(AccountEventsTestConsumer.Message::getAccountEvent)
                    .filter(accountEvent -> accountEvent.getType().equals(UPDATED))
                    .filter(event -> event.getUserId().equals(userId))
                    .collect(Collectors.toList());
            assertThat(accountEvents).containsOnly(new AccountEvent(UPDATED, userId, userSiteId, accountId, siteId, "George Whittaker"));
        });
    }

}
