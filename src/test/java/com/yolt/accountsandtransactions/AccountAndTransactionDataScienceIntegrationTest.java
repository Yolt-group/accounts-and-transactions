package com.yolt.accountsandtransactions;

import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.mapping.Mapper;
import com.datastax.driver.mapping.MappingManager;
import com.yolt.accountsandtransactions.accounts.event.AccountEvent;
import com.yolt.accountsandtransactions.datascience.DSCreditCardCurrent;
import com.yolt.accountsandtransactions.datascience.DataScienceService;
import com.yolt.accountsandtransactions.datascience.DsAccountCurrent;
import com.yolt.accountsandtransactions.datascience.DsTransaction;
import com.yolt.accountsandtransactions.inputprocessing.AccountFromProviders;
import com.yolt.accountsandtransactions.inputprocessing.AccountsAndTransactionsRequestDTO;
import com.yolt.accountsandtransactions.inputprocessing.ProviderTransactionWithId;
import lombok.extern.slf4j.Slf4j;
import nl.ing.lovebird.activityevents.events.IngestionFinishedEvent;
import nl.ing.lovebird.clienttokens.test.TestClientTokens;
import nl.ing.lovebird.extendeddata.common.CurrencyCode;
import nl.ing.lovebird.extendeddata.transaction.TransactionStatus;
import nl.ing.lovebird.providerdomain.*;
import org.awaitility.Duration;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;

import java.io.IOException;
import java.math.BigDecimal;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.*;
import java.util.stream.Collectors;

import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;
import static com.yolt.accountsandtransactions.TestUtils.ingestionRequestSuccessMessage;
import static com.yolt.accountsandtransactions.accounts.event.AccountEventType.CREATED;
import static com.yolt.accountsandtransactions.datascience.DsTransaction.USER_ID_COLUMN;
import static java.util.Collections.singletonMap;
import static nl.ing.lovebird.clienttokens.constants.ClientTokenConstants.CLIENT_TOKEN_HEADER_NAME;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@Slf4j
public class AccountAndTransactionDataScienceIntegrationTest extends BaseIntegrationTest {

    @Autowired
    private KafkaTemplate<String, AccountFromProviders> stringKafkaTemplate;

    @Autowired
    private ActivityEventsTestConsumer activityEventsTestConsumer;

    @Autowired
    private AccountEventsTestConsumer accountEventsTestConsumer;

    @Autowired
    private DataScienceService dataScienceService;

    @Autowired
    private TestClientTokens testClientTokens;

    private Mapper<DsTransaction> transactionMapper;
    private Mapper<DsAccountCurrent> accountCurrentMapper;
    private Mapper<DSCreditCardCurrent> creditCardMapperCurrent;

    @Override
    protected void setup() {
        MappingManager mappingManager = new MappingManager(session);
        transactionMapper = mappingManager.mapper(DsTransaction.class, dsKeyspace);
        accountCurrentMapper = mappingManager.mapper(DsAccountCurrent.class, dsKeyspace);
        creditCardMapperCurrent = mappingManager.mapper(DSCreditCardCurrent.class, dsKeyspace);
        activityEventsTestConsumer.getConsumed().clear();
    }


    @Test
    public void when_ANewAccountComesIn_then_itShouldBeStored() throws Exception {
        UUID clientGroupId = UUID.randomUUID();
        UUID clientId = UUID.randomUUID();
        UUID userId = UUID.randomUUID();
        var clientUserToken = testClientTokens.createClientUserToken(clientGroupId, clientId, userId);

        UUID activityId = UUID.randomUUID();
        UUID userSiteId = UUID.randomUUID();
        UUID siteId = UUID.randomUUID();
        String externalAccountId = "accountX";
        AccountsAndTransactionsRequestDTO accountsAndTransactionsRequestDTO = ingestionRequestSuccessMessage(userId, activityId, userSiteId, externalAccountId, "PROVIDER_X", siteId);

        UUID newCreatedInternalAccountId = UUID.randomUUID();

        stubNewAccounts(accountsAndTransactionsRequestDTO, newCreatedInternalAccountId);

        Message<AccountsAndTransactionsRequestDTO> message = MessageBuilder
                .withPayload(accountsAndTransactionsRequestDTO)
                .setHeader(KafkaHeaders.TOPIC, TestConfiguration.INPUT_TOPIC)
                .setHeader(KafkaHeaders.MESSAGE_KEY, UUID.randomUUID().toString())
                .setHeader(CLIENT_TOKEN_HEADER_NAME, clientUserToken.getSerialized())
                .build();

        stringKafkaTemplate.send(message);

        // Upstream message delivered and downstream message sent?
        await().atMost(Duration.TEN_SECONDS).untilAsserted(() -> {
            List<ActivityEventsTestConsumer.Message> activityEvents = activityEventsTestConsumer.getConsumed().stream()
                    .filter(it -> it.getAbstractEvent().getActivityId().equals(activityId)).collect(Collectors.toList());

            assertThat(activityEvents).hasSize(1);

            assertActivityEventMessage(
                    activityEvents.get(0),
                    "2017-10",
                    "2017-10",
                    userId,
                    userSiteId,
                    activityId,
                    singletonMap(newCreatedInternalAccountId, new IngestionFinishedEvent.AccountInformationDTO(
                            externalAccountId,
                            "PROVIDER_X",
                            "12345z"
                    ))
            );

            List<AccountEvent> accountEvents = accountEventsTestConsumer.getConsumed().stream()
                    .map(AccountEventsTestConsumer.Message::getAccountEvent)
                    .filter(accountEvent -> CREATED.equals(accountEvent.getType()))
                    .filter(event -> event.getUserId().equals(userId))
                    .collect(Collectors.toList());
            assertThat(accountEvents).containsOnly(new AccountEvent(CREATED, userId, userSiteId, newCreatedInternalAccountId, siteId, null));
        });

        // Assert that the account has been inserted correctly.
        List<DsAccountCurrent> dsAccountCurrent = accountCurrentMapper.map(session.execute(
                QueryBuilder.select().from(dsKeyspace, DsAccountCurrent.TABLE_NAME).where(eq(USER_ID_COLUMN, userId))
        )).all();
        assertThat(dsAccountCurrent).hasSize(1);
        DsAccountCurrent persistedAccount = dsAccountCurrent.get(0);
        AccountFromProviders upstreamAccount = accountsAndTransactionsRequestDTO.getIngestionAccounts().get(0);
        assertThat(persistedAccount.getUserId()).isEqualTo(userId);
        assertThat(persistedAccount.getAccountId()).isEqualTo(newCreatedInternalAccountId);
        assertThat(persistedAccount.getUserSiteId()).isEqualTo(accountsAndTransactionsRequestDTO.getUserSiteId());
        assertThat(persistedAccount.getSiteId()).isEqualTo(accountsAndTransactionsRequestDTO.getSiteId());
        assertThat(persistedAccount.getExternalAccountId()).isEqualTo(upstreamAccount.getAccountId());
        assertThat(persistedAccount.getExternalSiteId()).isEqualTo("");
        assertThat(persistedAccount.getCurrencyCode()).isEqualTo(upstreamAccount.getCurrency().name());
        assertThat(persistedAccount.getCurrentBalance()).isEqualTo(upstreamAccount.getCurrentBalance());
        assertThat(persistedAccount.getAvailableBalance()).isEqualTo(upstreamAccount.getAvailableBalance());
        assertThat(persistedAccount.getStatus()).isEqualTo("PROCESSING_TRANSACTIONS_FINISHED");
        assertThat(persistedAccount.getStatusDetail()).isEqualTo("0_SUCCESS");
        assertThat(persistedAccount.getProvider()).isEqualTo("PROVIDER_X");
        assertThat(persistedAccount.getBankSpecific()).isEqualTo("{\"key\":\"value\"}");
        // Assert that credit card data has been stored

        final ProviderCreditCardDTO expectedCreditCard = upstreamAccount.getCreditCardData();
        final List<DSCreditCardCurrent> creditCardsCurrent = creditCardMapperCurrent.map(session.execute(
                QueryBuilder.select().from(dsKeyspace, DSCreditCardCurrent.TABLE_NAME).where(eq(USER_ID_COLUMN, userId))
        )).all();

        assertThat(creditCardsCurrent).hasSize(1);
        final DSCreditCardCurrent dsCreditCardCurrent = creditCardsCurrent.get(0);
        assertThat(dsCreditCardCurrent.getAccountId()).isEqualTo(newCreatedInternalAccountId);
        assertThat(dsCreditCardCurrent.getApr().doubleValue()).isEqualTo(expectedCreditCard.getApr().doubleValue());
        assertThat(dsCreditCardCurrent.getAvailableCreditAmount()).isEqualTo(expectedCreditCard.getAvailableCreditAmount());
        assertThat(dsCreditCardCurrent.getCashApr().doubleValue()).isEqualTo(expectedCreditCard.getCashApr().doubleValue());
        assertThat(dsCreditCardCurrent.getCashLimitAmount()).isEqualTo(expectedCreditCard.getCashLimitAmount());
        assertThat(dsCreditCardCurrent.getCurrencyCode()).isEqualTo(upstreamAccount.getCurrency().name());
        assertThat(dsCreditCardCurrent.getDueAmount()).isEqualTo(expectedCreditCard.getDueAmount());
        assertThat(dsCreditCardCurrent.getDueDate()).isEqualTo(expectedCreditCard.getDueDate().toLocalDate().toString());
        assertThat(dsCreditCardCurrent.getExternalAccountId()).isEqualTo(upstreamAccount.getAccountId());
        assertThat(dsCreditCardCurrent.getExternalSiteId()).isEmpty();
        assertThat(dsCreditCardCurrent.getLastPaymentAmount()).isEqualTo(expectedCreditCard.getLastPaymentAmount());
        assertThat(dsCreditCardCurrent.getLastPaymentDate()).isEqualTo(expectedCreditCard.getLastPaymentDate().toLocalDate().toString());
        assertThat(dsCreditCardCurrent.getName()).isEqualTo(upstreamAccount.getName());
        assertThat(dsCreditCardCurrent.getNewChargesAmount()).isEqualTo(expectedCreditCard.getNewChargesAmount());
        assertThat(dsCreditCardCurrent.getRunningBalanceAmount()).isEqualTo(expectedCreditCard.getRunningBalanceAmount());
        assertThat(dsCreditCardCurrent.getSiteId()).isEqualTo(accountsAndTransactionsRequestDTO.getSiteId());
        assertThat(dsCreditCardCurrent.getTotalCreditLineAmount()).isEqualTo(expectedCreditCard.getTotalCreditLineAmount());
        assertThat(dsCreditCardCurrent.getUserId()).isEqualTo(upstreamAccount.getYoltUserId());
        assertThat(dsCreditCardCurrent.getUserSiteId()).isEqualTo(upstreamAccount.getYoltUserSiteId());

        // Assert that the transactions has been inserted correctly.
        List<DsTransaction> transactions = transactionMapper.map(
                session.execute(QueryBuilder.select().from(dsKeyspace, DsTransaction.TABLE_NAME).where(eq(USER_ID_COLUMN, userId)))
        ).all();

        List<DsTransaction> expectedTransactions = dataScienceService.toDsTransactionList(newCreatedInternalAccountId, userId, CurrencyCode.EUR, upstreamAccount.getTransactions().stream()
                .map(providerTransactionDTO -> new ProviderTransactionWithId(providerTransactionDTO, UUID.randomUUID().toString()))
                .collect(Collectors.toList()));

        expectedTransactions.forEach(it -> it.setAccountId(newCreatedInternalAccountId));
        assertThat(transactions)
                .usingElementComparatorIgnoringFields("transactionId", "lastUpdatedTime")
                .containsExactlyInAnyOrderElementsOf(expectedTransactions);

    }

    @Test
    public void when_thereAreNoTransactions_then_theAccountsShouldStillBeSaved() throws Exception {
        UUID clientGroupId = UUID.randomUUID();
        UUID clientId = UUID.randomUUID();
        UUID userId = UUID.randomUUID();
        var clientUserToken = testClientTokens.createClientUserToken(clientGroupId, clientId, userId);

        UUID activityId = UUID.randomUUID();
        UUID userSiteId = UUID.randomUUID();
        String externalAccountId = "accountX";
        AccountsAndTransactionsRequestDTO accountsAndTransactionsRequestDTO = ingestionRequestSuccessMessage(userId, activityId, userSiteId, externalAccountId, "PROVIDER_X", UUID.randomUUID());
        accountsAndTransactionsRequestDTO.getIngestionAccounts().forEach(it -> it.getTransactions().clear());
        UUID newCreatedInternalAccountId = UUID.randomUUID();

        stubNewAccounts(accountsAndTransactionsRequestDTO, newCreatedInternalAccountId);

        Message<AccountsAndTransactionsRequestDTO> message = MessageBuilder
                .withPayload(accountsAndTransactionsRequestDTO)
                .setHeader(KafkaHeaders.TOPIC, TestConfiguration.INPUT_TOPIC)
                .setHeader(KafkaHeaders.MESSAGE_KEY, userId.toString())
                .setHeader(CLIENT_TOKEN_HEADER_NAME, clientUserToken.getSerialized())
                .build();
        stringKafkaTemplate.send(message);


        // Upstream message delivered and downstream message sent?
        await().atMost(Duration.TEN_SECONDS).untilAsserted(() -> {
            List<ActivityEventsTestConsumer.Message> activityEvents = activityEventsTestConsumer.getConsumed().stream()
                    .filter(it -> it.getAbstractEvent().getActivityId().equals(activityId)).collect(Collectors.toList());

            assertThat(activityEvents).hasSize(1);

            assertActivityEventMessage(
                    activityEvents.get(0),
                    null,
                    null,
                    userId,
                    userSiteId,
                    activityId,
                    Collections.emptyMap()
            );
        });

        List<DsAccountCurrent> dsAccountCurrent = accountCurrentMapper.map(session.execute(
                QueryBuilder.select().from(dsKeyspace, DsAccountCurrent.TABLE_NAME).where(eq(USER_ID_COLUMN, userId))
        )).all();
        assertThat(dsAccountCurrent).hasSize(1);

        List<DsTransaction> transactions = transactionMapper.map(
                session.execute(QueryBuilder.select().from(dsKeyspace, DsTransaction.TABLE_NAME).where(eq(USER_ID_COLUMN, userId)))
        ).all();
        assertThat(transactions).isEmpty();
    }

    @Test
    public void when_thereIsASecondTransactionBatch_then_transactionsThatAreNotPresentInThatTimeFrameShouldNotBeDeletedIfProviderIsBudgetInsight() throws Exception {
        testMissingTransactionsNotDeleted("BUDGET_INSIGHT");
    }

    @Test
    public void when_aBatchOfAccountsIsProcessed_then_theStartYearAndEndYearMonthOnTheActivityEventsShouldBeAsWideAsPossible() throws IOException {
        // Apparently, that daterange depends on the pending transactions in the database + of the transactions that come in.
        UUID clientGroupId = UUID.randomUUID();
        UUID clientId = UUID.randomUUID();
        UUID userId = UUID.randomUUID();
        var clientUserToken = testClientTokens.createClientUserToken(clientGroupId, clientId, userId);
        // Here there's an example where the min date comes from pending transaction of account 2.
        // The max date from a new transaction in account 1.
        UUID accountId1 = new UUID(0, 1);
        UUID accountId2 = new UUID(0, 2);
        UUID userSiteId = UUID.fromString("b1c99460-3a8e-48b0-bbc6-72d14cd5879d");
        List<ProviderTransactionDTO> transactionsAccount1 = new ArrayList<>();
        // The maximum date.
        transactionsAccount1.add(ProviderTransactionDTO.builder()
                .externalId("1")
                .dateTime(ZonedDateTime.of(2019, 3, 2, 0, 0, 0, 0, ZoneId.of("UTC")))
                .status(TransactionStatus.BOOKED)
                .category(YoltCategory.GENERAL)
                .type(ProviderTransactionType.DEBIT)
                .description("description")
                .amount(BigDecimal.ONE)
                .build()
        );

        List<ProviderTransactionDTO> transactionsAccount2 = new ArrayList<>();
        transactionsAccount2.add(ProviderTransactionDTO.builder()
                .externalId("2")
                .dateTime(ZonedDateTime.of(2019, 1, 1, 0, 0, 0, 0, ZoneId.of("UTC")))
                .status(TransactionStatus.BOOKED)
                .category(YoltCategory.GENERAL)
                .type(ProviderTransactionType.DEBIT)
                .description("description")
                .amount(BigDecimal.ONE)
                .build()
        );


        Insert value = QueryBuilder
                .insertInto(dsKeyspace, DsTransaction.TABLE_NAME)
                .value(USER_ID_COLUMN, userId)
                .value(DsTransaction.PENDING_COLUMN, 2)
                .value(DsTransaction.ACCOUNT_ID_COLUMN, accountId1)
                .value(DsTransaction.DATE_COLUMN, "2018-10-29")
                .value(DsTransaction.TRANSACTION_ID_COLUMN, UUID.randomUUID().toString());

        // The minimum date.
        Insert value2 = QueryBuilder
                .insertInto(dsKeyspace, DsTransaction.TABLE_NAME)
                .value(USER_ID_COLUMN, userId)
                .value(DsTransaction.PENDING_COLUMN, 2)
                .value(DsTransaction.ACCOUNT_ID_COLUMN, accountId2)
                .value(DsTransaction.DATE_COLUMN, "2018-09-26")
                .value(DsTransaction.TRANSACTION_ID_COLUMN, UUID.randomUUID().toString());
        session.execute(value);
        session.execute(value2);


        UUID activityId = UUID.randomUUID();
        AccountsAndTransactionsRequestDTO accountsAndTransactionsRequestDTO = AccountsAndTransactionsRequestDTO.builder()
                .activityId(activityId)
                .userSiteId(userSiteId)
                .siteId(new UUID(0, 0))
                .ingestionAccounts(Arrays.asList(
                        AccountFromProviders.accountsFromProvidersBuilder()
                                .name("current account 1")
                                .yoltUserId(userId)
                                .yoltUserSiteId(userSiteId)
                                .yoltSiteId(new UUID(0, 0))
                                .provider("YODLEE")
                                .accountId(accountId1.toString())
                                .yoltAccountType(AccountType.CURRENT_ACCOUNT)
                                .currency(CurrencyCode.EUR)
                                .currentBalance(BigDecimal.ONE)
                                .lastRefreshed(ZonedDateTime.now())
                                .transactions(transactionsAccount1)
                                .accountNumber(new ProviderAccountNumberDTO(ProviderAccountNumberDTO.Scheme.IBAN, "NL47INGB00000001"))
                                .build(),
                        AccountFromProviders.accountsFromProvidersBuilder()
                                .name("current account 2")
                                .yoltUserId(userId)
                                .yoltUserSiteId(userSiteId)
                                .yoltSiteId(new UUID(0, 0))
                                .provider("YODLEE")
                                .accountId(accountId2.toString())
                                .transactions(transactionsAccount2)
                                .yoltAccountType(AccountType.CURRENT_ACCOUNT)
                                .currency(CurrencyCode.EUR)
                                .currentBalance(BigDecimal.ONE)
                                .lastRefreshed(ZonedDateTime.now())
                                .accountNumber(new ProviderAccountNumberDTO(ProviderAccountNumberDTO.Scheme.IBAN, "NL47INGB0000002"))
                                .build()
                ))
                .build();

        stubExistingAccounts(userId, accountsAndTransactionsRequestDTO, accountId1, accountId2);

        // Send
        Message<AccountsAndTransactionsRequestDTO> message = MessageBuilder
                .withPayload(accountsAndTransactionsRequestDTO)
                .setHeader(KafkaHeaders.TOPIC, TestConfiguration.INPUT_TOPIC)
                .setHeader(KafkaHeaders.MESSAGE_KEY, userId.toString())
                .setHeader(CLIENT_TOKEN_HEADER_NAME, clientUserToken.getSerialized())
                .build();
        stringKafkaTemplate.send(message);

        // Assert
        await().atMost(Duration.TEN_SECONDS).untilAsserted(() -> {
            List<ActivityEventsTestConsumer.Message> activityEvents = activityEventsTestConsumer.getConsumed().stream()
                    .filter(it -> it.getAbstractEvent().getActivityId().equals(activityId)).collect(Collectors.toList());

            assertThat(activityEvents).hasSize(1);

            assertActivityEventMessage(
                    activityEvents.get(0),
                    "2018-09",
                    "2019-03",
                    userId,
                    userSiteId,
                    activityId,
                    Map.of(accountId1, new IngestionFinishedEvent.AccountInformationDTO(
                                    accountId1.toString(),
                                    "YODLEE",
                                    "1"
                            ),
                            accountId2, new IngestionFinishedEvent.AccountInformationDTO(
                                    accountId2.toString(),
                                    "YODLEE",
                                    "2"
                            )
                    )
            );
        });
    }

    private void testMissingTransactionsNotDeleted(String provider) throws Exception {
        UUID clientGroupId = UUID.randomUUID();
        UUID clientId = UUID.randomUUID();
        UUID userId = UUID.randomUUID();
        var clientUserToken = testClientTokens.createClientUserToken(clientGroupId, clientId, userId);

        UUID activityId = UUID.randomUUID();
        UUID userSiteId = UUID.randomUUID();
        UUID internalAccountId = UUID.randomUUID();
        String externalAccountId = "accountX";
        AccountsAndTransactionsRequestDTO accountsAndTransactionsRequestDTO = ingestionRequestSuccessMessage(userId, activityId, userSiteId, externalAccountId, provider, UUID.randomUUID());

        stubNewAccounts(accountsAndTransactionsRequestDTO, internalAccountId);

        Message<AccountsAndTransactionsRequestDTO> message = MessageBuilder
                .withPayload(accountsAndTransactionsRequestDTO)
                .setHeader(KafkaHeaders.TOPIC, TestConfiguration.INPUT_TOPIC)
                .setHeader(KafkaHeaders.MESSAGE_KEY, userId.toString())
                .setHeader(CLIENT_TOKEN_HEADER_NAME, clientUserToken.getSerialized())
                .build();
        stringKafkaTemplate.send(message);


        // Send the second message (with a changed transactions)
        List<ProviderTransactionDTO> transactions = accountsAndTransactionsRequestDTO.getIngestionAccounts().get(0).getTransactions();
        List<ProviderTransactionDTO> oldTransactions = new ArrayList<>(transactions);

        List<ProviderTransactionDTO> newTransactions = transactions.stream().map(it -> it.toBuilder()
                .externalId(it.getExternalId() + "newid")
                .build()).collect(Collectors.toList());
        transactions.clear();
        transactions.addAll(newTransactions);

        Message<AccountsAndTransactionsRequestDTO> message2 = MessageBuilder
                .withPayload(accountsAndTransactionsRequestDTO)
                .setHeader(KafkaHeaders.TOPIC, TestConfiguration.INPUT_TOPIC)
                .setHeader(KafkaHeaders.MESSAGE_KEY, userId.toString())
                .setHeader(CLIENT_TOKEN_HEADER_NAME, clientUserToken.getSerialized())
                .build();
        stringKafkaTemplate.send(message2);

        await().atMost(Duration.TEN_SECONDS).untilAsserted(() -> assertThat(activityEventsTestConsumer.getConsumed()
                .stream().filter(it -> it.getAbstractEvent().getActivityId().equals(activityId))).hasSize(2));

        List<DsTransaction> storedTransactions = transactionMapper.map(
                session.execute(QueryBuilder.select().from(dsKeyspace, DsTransaction.TABLE_NAME).where(eq(USER_ID_COLUMN, userId)))
        ).all();
        assertThat(storedTransactions).hasSize(2);
        assertThat(storedTransactions.stream().map(DsTransaction::getTransactionId)).containsAll(
                newTransactions.stream()
                        .filter(t -> t.getStatus() == TransactionStatus.BOOKED)
                        .map(ProviderTransactionDTO::getExternalId).distinct().collect(Collectors.toList()));

        List<String> expectedOldTransactionIds = oldTransactions.stream()
                .map(it -> {
                    if (it.getStatus().equals(TransactionStatus.BOOKED)) {
                        return it.getExternalId();
                    } else {
                        return null;
                    }
                })
                .filter(Objects::nonNull)
                .distinct()
                .collect(Collectors.toList());

        assertThat(storedTransactions.stream().map(DsTransaction::getTransactionId)).containsAll(expectedOldTransactionIds);

    }

    @Test
    public void when_thereIsASecondTransactionBatch_then_pendingTransactionsFromPreviousRunsShouldBeRemoved() throws Exception {
        UUID clientGroupId = UUID.randomUUID();
        UUID clientId = UUID.randomUUID();
        UUID userId = UUID.randomUUID();
        var clientUserToken = testClientTokens.createClientUserToken(clientGroupId, clientId, userId);

        UUID activityId = UUID.randomUUID();
        UUID userSiteId = UUID.randomUUID();
        UUID internalAccountId = UUID.randomUUID();
        String externalAccountId = "accountX";
        AccountsAndTransactionsRequestDTO accountsAndTransactionsRequestDTO = ingestionRequestSuccessMessage(userId, activityId, userSiteId, externalAccountId, "provider-X", UUID.randomUUID());

        stubNewAccounts(accountsAndTransactionsRequestDTO, internalAccountId);

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
        List<DsTransaction> transactions = transactionMapper.map(
                session.execute(QueryBuilder.select().from(dsKeyspace, DsTransaction.TABLE_NAME).where(eq(USER_ID_COLUMN, userId)))
        ).all();
        assertThat(transactions).hasSize(3);
        assertThat(transactions.stream().filter(it -> it.getPending().equals(2))).hasSize(2); // pendingType = 2 is 'pending'...

        // Send the second message without transactions
        accountsAndTransactionsRequestDTO.getIngestionAccounts().forEach(it -> it.getTransactions().clear());
        Message<AccountsAndTransactionsRequestDTO> message2 = MessageBuilder
                .withPayload(accountsAndTransactionsRequestDTO)
                .setHeader(KafkaHeaders.TOPIC, TestConfiguration.INPUT_TOPIC)
                .setHeader(KafkaHeaders.MESSAGE_KEY, userId.toString())
                .setHeader(CLIENT_TOKEN_HEADER_NAME, clientUserToken.getSerialized())
                .build();
        stringKafkaTemplate.send(message2);


        // Second upstream message delivered and downstream message sent?
        await().atMost(Duration.TEN_SECONDS).untilAsserted(() -> assertThat(activityEventsTestConsumer.getConsumed()
                .stream()
                .filter(it -> it.getAbstractEvent().getActivityId().equals(activityId)))
                .hasSize(2));

        List<DsTransaction> transactions2 = transactionMapper.map(
                session.execute(QueryBuilder.select().from(dsKeyspace, DsTransaction.TABLE_NAME).where(eq(USER_ID_COLUMN, userId)))
        ).all();
        assertThat(transactions2).hasSize(3);
        assertThat(transactions2.stream().filter(it -> it.getPending().equals(2))).hasSize(2);
    }


    public static void assertActivityEventMessage(
            ActivityEventsTestConsumer.Message message,
            final String expectedStartYearMonth,
            final String expectedEndYearMonth,
            final UUID expectedUserId,
            final UUID userSiteId,
            final UUID expectedActivityId,
            final Map<UUID, IngestionFinishedEvent.AccountInformationDTO> expectedAccountIdToLastTransactionIdMap
    ) {
        IngestionFinishedEvent ingestionFinishedEvent = (IngestionFinishedEvent) message.getAbstractEvent();
        assertThat(ingestionFinishedEvent.getUserId()).isEqualTo(expectedUserId);
        assertThat(ingestionFinishedEvent.getUserSiteId()).isEqualTo(userSiteId);
        assertThat(ingestionFinishedEvent.getStartYearMonth()).isEqualTo(expectedStartYearMonth);
        assertThat(ingestionFinishedEvent.getEndYearMonth()).isEqualTo(expectedEndYearMonth);
        assertThat(ingestionFinishedEvent.getActivityId()).isEqualTo(expectedActivityId);
        assertThat(ingestionFinishedEvent.getAccountIdToAccountInformation()).isEqualTo(expectedAccountIdToLastTransactionIdMap);
    }
}
