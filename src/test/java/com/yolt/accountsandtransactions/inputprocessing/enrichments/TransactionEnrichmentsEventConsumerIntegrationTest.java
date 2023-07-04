package com.yolt.accountsandtransactions.inputprocessing.enrichments;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.yolt.accountsandtransactions.ActivityEventsTestConsumer;
import com.yolt.accountsandtransactions.BaseIntegrationTest;
import com.yolt.accountsandtransactions.inputprocessing.AccountFromProviders;
import com.yolt.accountsandtransactions.inputprocessing.ProviderTransactionWithId;
import com.yolt.accountsandtransactions.inputprocessing.TransactionInsertionStrategy.Instruction.InstructionType;
import com.yolt.accountsandtransactions.inputprocessing.enrichments.activities.ActivityEnrichmentService;
import com.yolt.accountsandtransactions.inputprocessing.enrichments.api.base.EnrichmentMessage;
import com.yolt.accountsandtransactions.inputprocessing.enrichments.api.categories.CategoriesEnrichmentMessage;
import com.yolt.accountsandtransactions.inputprocessing.enrichments.api.counterparties.CounterpartiesEnrichmentMessage;
import com.yolt.accountsandtransactions.inputprocessing.enrichments.api.cycles.CyclesEnrichmentMessage;
import com.yolt.accountsandtransactions.inputprocessing.enrichments.api.labels.LabelsEnrichmentMessage;
import com.yolt.accountsandtransactions.transactions.TransactionService;
import lombok.extern.slf4j.Slf4j;
import nl.ing.lovebird.activityevents.events.TransactionsEnrichmentFinishedEvent;
import nl.ing.lovebird.clienttokens.ClientUserToken;
import nl.ing.lovebird.clienttokens.test.TestClientTokens;
import nl.ing.lovebird.providerdomain.ProviderTransactionDTO;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.messaging.support.MessageBuilder;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Paths;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Stream;

import static com.yolt.accountsandtransactions.inputprocessing.enrichments.activities.ActivityEnrichmentType.REFRESH;
import static java.math.BigDecimal.TEN;
import static java.nio.file.Files.lines;
import static java.util.UUID.randomUUID;
import static java.util.stream.Collectors.joining;
import static nl.ing.lovebird.activityevents.events.TransactionsEnrichmentFinishedEvent.Status.SUCCESS;
import static nl.ing.lovebird.clienttokens.constants.ClientTokenConstants.CLIENT_TOKEN_HEADER_NAME;
import static nl.ing.lovebird.extendeddata.common.CurrencyCode.EUR;
import static nl.ing.lovebird.extendeddata.transaction.TransactionStatus.PENDING;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.awaitility.Duration.ONE_MINUTE;
import static org.springframework.kafka.support.KafkaHeaders.MESSAGE_KEY;
import static org.springframework.kafka.support.KafkaHeaders.TOPIC;

@Slf4j
public class TransactionEnrichmentsEventConsumerIntegrationTest extends BaseIntegrationTest {

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @Autowired
    private ActivityEnrichmentService activityEnrichmentService;

    @Autowired
    private TransactionService transactionService;

    @Autowired
    private ObjectMapper objectMapper;

    @Autowired
    private ActivityEventsTestConsumer testConsumer;

    @Autowired
    private TestClientTokens testClientTokens;

    @AfterEach
    void afterEach() {
        testConsumer.clearMessages();
    }

    @Test
    public void shouldBeConsumedAndStoredInCassandraAndMarkedAsCompleted() {
        // Given
        var activityId = UUID.fromString("7183ea5e-4151-4a95-ad82-4076cf7c4b71");
        var userId = UUID.fromString("751e7b04-5eee-47ef-8a36-2d2d409d1b00");
        var accountId = UUID.fromString("f7a10a55-33e0-4e43-b723-35e73b03cf67");
        var transactionId = UUID.fromString("5ecd3a7d-0b4c-42ac-8645-6371a11b7d85");
        var transactionDateTime = ZonedDateTime.parse("2020-08-04T16:00:00.000000+02:00");
        var clientGroupId = UUID.randomUUID();
        var clientId = UUID.randomUUID();
        var clientUserToken = testClientTokens.createClientUserToken(clientGroupId, clientId, userId);

        saveTransactionForAccount(accountId, transactionId, transactionDateTime, clientUserToken);

        // when
        activityEnrichmentService.startActivityEnrichment(clientUserToken, REFRESH, activityId);
        sendEnrichmentMessages(Stream.of(
                                Pair.of("data/ds-counterparties-enrichment.json", CounterpartiesEnrichmentMessage.class),
                                Pair.of("data/ds-categories-enrichment.json", CategoriesEnrichmentMessage.class),
                                Pair.of("data/ds-transactioncycles-enrichment.json", CyclesEnrichmentMessage.class),
                                Pair.of("data/ds-labels-enrichment.json", LabelsEnrichmentMessage.class))
                        .flatMap(filenameAndClass -> createPayload(filenameAndClass.getLeft(), filenameAndClass.getRight()).stream()).toList(),
                clientUserToken);

        // then
        assertThatThereIsExactlyOneTransactionFinishedEvent(activityId);

        // Transaction is updated
        assertThat(transactionService.getTransaction(userId, accountId, transactionDateTime.toLocalDate(), transactionId.toString()))
                .isPresent()
                .get()
                .matches(tx -> tx.getEnrichment().getCounterparty().getName().equals("Ajax"))
                .matches(tx -> tx.getEnrichment().getCounterparty().isKnownMerchant())
                .matches(tx -> tx.getEnrichment().getMerchant().getName().equals("Ajax"))
                .matches(tx -> tx.getEnrichment().getCategory().equals("Drinks"))
                .matches(tx -> tx.getEnrichment().getCycleId().equals(UUID.fromString("6005c4ee-5abc-4219-bbb0-91e39a14cff3")))
                .matches(tx -> tx.getDescription().equals("aTransaction"));
    }

    private void sendEnrichmentMessages(List<EnrichmentMessage> payloads, ClientUserToken clientUserToken) {
        payloads.forEach(payload -> {
            var msg = MessageBuilder
                    .withPayload(payload)
                    .setHeader(TOPIC, "transactionEnrichments")
                    .setHeader(MESSAGE_KEY, "SomeString")
                    .setHeader(CLIENT_TOKEN_HEADER_NAME, clientUserToken.getSerialized())
                    .setHeader("payload-type", payload.getDomain())
                    .setHeader("payload-version", 1)
                    .build();
            kafkaTemplate.send(msg);
        });
    }

    @Test
    public void given_enrichmentMessagesInPages_then_thereShouldBeATransactionFinishedEvent() {
        // Given
        var userId = UUID.fromString("751e7b04-5eee-47ef-8a36-2d2d409d1b00");
        var accountId = UUID.fromString("f7a10a55-33e0-4e43-b723-35e73b03cf67");
        var transactionId = UUID.fromString("5ecd3a7d-0b4c-42ac-8645-6371a11b7d85");
        var transactionId2 = UUID.fromString("4151958e-ee50-4ccf-ba6a-b0c3b6fe6d7d");
        var transactionDateTime = ZonedDateTime.parse("2020-08-04T16:00:00.000000+02:00");
        var activityId = UUID.fromString("7183ea5e-4151-4a95-ad82-4076cf7c4b71");
        var clientGroupId = UUID.randomUUID();
        var clientId = UUID.randomUUID();
        var clientUserToken = testClientTokens.createClientUserToken(clientGroupId, clientId, userId);

        saveTransactionForAccount(accountId, transactionId, transactionDateTime, clientUserToken);
        saveTransactionForAccount(accountId, transactionId2, transactionDateTime, clientUserToken);

        // when
        activityEnrichmentService.startActivityEnrichment(clientUserToken, REFRESH, activityId);

        sendEnrichmentMessages(Stream.of(
                                Pair.of("data/ds-counterparties-enrichment.json", CounterpartiesEnrichmentMessage.class),
                                Pair.of("data/ds-categories-enrichment-page1.json", CategoriesEnrichmentMessage.class),
                                Pair.of("data/ds-categories-enrichment-page2.json", CategoriesEnrichmentMessage.class),
                                Pair.of("data/ds-transactioncycles-enrichment.json", CyclesEnrichmentMessage.class),
                                Pair.of("data/ds-labels-enrichment.json", LabelsEnrichmentMessage.class))
                        .flatMap(filenameAndClass -> createPayload(filenameAndClass.getLeft(), filenameAndClass.getRight()).stream()).toList(),
                clientUserToken);

        // then
        assertThatThereIsExactlyOneTransactionFinishedEvent(activityId);

        // Transaction is updated
        assertThat(transactionService.getTransaction(userId, accountId, transactionDateTime.toLocalDate(), transactionId.toString()))
                .isPresent()
                .get()
                .matches(tx -> tx.getEnrichment().getCounterparty().getName().equals("Ajax"), "counter party is not equal to ajax")
                .matches(tx -> tx.getEnrichment().getCounterparty().isKnownMerchant())
                .matches(tx -> tx.getEnrichment().getMerchant().getName().equals("Ajax"))
                .matches(tx -> tx.getEnrichment().getCategory().equals("Drinks"), "category should be drinks")
                .matches(tx -> tx.getEnrichment().getCycleId().equals(UUID.fromString("6005c4ee-5abc-4219-bbb0-91e39a14cff3")))
                .matches(tx -> tx.getDescription().equals("aTransaction"));
        assertThat(transactionService.getTransaction(userId, accountId, transactionDateTime.toLocalDate(), transactionId2.toString()))
                .isPresent()
                .get()
                .matches(tx -> tx.getEnrichment().getCategory().equals("enriched-category-transaction-4151958e-ee50-4ccf-ba6a-b0c3b6fe6d7d"));
    }

    private void saveTransactionForAccount(UUID accountId, UUID transactionId, ZonedDateTime transactionDateTime, ClientUserToken clientUserToken) {
        var accountFromProviders = AccountFromProviders.accountsFromProvidersBuilder()
                .yoltUserSiteId(randomUUID())
                .currency(EUR)
                .provider("aProvider")
                .build();

        transactionService.saveTransactionsBatch(
                accountId,
                clientUserToken,
                accountFromProviders,
                List.of(
                        new ProviderTransactionWithId(
                                ProviderTransactionDTO.builder()
                                        .amount(TEN)
                                        .description("aTransaction")
                                        .status(PENDING)
                                        .dateTime(transactionDateTime)
                                        .build(),
                                transactionId.toString())
                ), InstructionType.INSERT);
    }

    private void assertThatThereIsExactlyOneTransactionFinishedEvent(UUID activityId) {
        await().timeout(ONE_MINUTE)
                .until(() -> testConsumer.getConsumed(),
                        events -> {
                            var transactionFinishedEvents = events.stream()
                                    .map(ActivityEventsTestConsumer.Message::getAbstractEvent)
                                    // Make sure this test only looks at the 2 messages that we sent above.
                                    .filter(event -> event.getActivityId().equals(activityId))
                                    .filter(event -> event instanceof TransactionsEnrichmentFinishedEvent)
                                    .filter(event -> ((TransactionsEnrichmentFinishedEvent) event).getStatus() == SUCCESS)
                                    .toList();
                            return transactionFinishedEvents.size() == 1;
                        });
    }

    private Optional<EnrichmentMessage> createPayload(String filename, Class<? extends EnrichmentMessage> clazz) {
        try {
            return Optional.of(objectMapper.readValue(
                    lines(Paths.get(getClass().getClassLoader().getResource(filename).toURI())).collect(joining()),
                    clazz));
        } catch (IOException | URISyntaxException e) {
            log.warn("Failed to load file {}", filename);
        }
        return Optional.empty();
    }
}
