package com.yolt.accountsandtransactions.inputprocessing.enrichments;


import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.Appender;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.yolt.accountsandtransactions.inputprocessing.enrichments.activities.ActivityEnrichmentService;
import com.yolt.accountsandtransactions.inputprocessing.enrichments.api.base.EnrichmentMessageKey;
import com.yolt.accountsandtransactions.inputprocessing.enrichments.api.categories.CategoriesEnrichmentMessage;
import com.yolt.accountsandtransactions.inputprocessing.enrichments.api.counterparties.CounterpartiesEnrichmentMessage;
import com.yolt.accountsandtransactions.inputprocessing.enrichments.api.cycles.CyclesEnrichmentMessage;
import com.yolt.accountsandtransactions.inputprocessing.enrichments.api.labels.LabelsEnrichmentMessage;
import com.yolt.accountsandtransactions.inputprocessing.enrichments.api.preprocessing.PreprocessingEnrichmentMessage;
import com.yolt.accountsandtransactions.metrics.AccountsAndTransactionMetrics;
import nl.ing.lovebird.clienttokens.ClientUserToken;
import org.jose4j.jwt.JwtClaims;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URISyntaxException;
import java.time.Clock;
import java.time.LocalDate;
import java.time.Period;
import java.time.ZonedDateTime;
import java.util.Currency;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Stream;

import static com.yolt.accountsandtransactions.inputprocessing.enrichments.api.EnrichmentMessageType.*;
import static java.lang.String.format;
import static java.nio.file.Files.lines;
import static java.nio.file.Paths.get;
import static java.util.UUID.randomUUID;
import static java.util.stream.Collectors.joining;
import static nl.ing.lovebird.clienttokens.constants.ClientTokenConstants.*;
import static org.apache.tomcat.util.codec.binary.Base64.encodeBase64String;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
public class TransactionEnrichmentsEventConsumerTest {
    private TransactionEnrichmentsEventConsumer eventConsumer;

    private ObjectMapper objectMapper;

    @Mock
    private TransactionEnrichmentsMessageHandler transactionEnrichmentsMessageHandler;

    @Mock
    private ActivityEnrichmentService activityEnrichmentService;

    @Mock
    private AccountsAndTransactionMetrics accountsAndTransactionMetrics;

    @Mock
    protected Appender<ILoggingEvent> logAppender;

    @Captor
    protected ArgumentCaptor<ILoggingEvent> loggingEventArgumentCaptor;

    @Captor
    protected ArgumentCaptor<CategoriesEnrichmentMessage> categoriesEnrichmentMessageArgumentCaptor;

    @Captor
    protected ArgumentCaptor<LabelsEnrichmentMessage> labelsEnrichmentMessageArgumentCaptor;

    @Captor
    protected ArgumentCaptor<CounterpartiesEnrichmentMessage> counterpartiesEnrichmentMessageArgumentCaptor;

    @Captor
    protected ArgumentCaptor<CyclesEnrichmentMessage> cyclesEnrichmentMessageArgumentCaptor;

    @BeforeEach
    public void init() {
        objectMapper = new ObjectMapper()
                .registerModule(new JavaTimeModule())
                .registerModule(new Jdk8Module());

        eventConsumer = new TransactionEnrichmentsEventConsumer(transactionEnrichmentsMessageHandler, activityEnrichmentService, accountsAndTransactionMetrics, Clock.systemUTC());

        var root = (Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
        root.addAppender(logAppender);
    }

    @Test
    public void testVersionNotSupported() {
        var userId = randomUUID();
        var activityId = randomUUID();

        eventConsumer.consume(new CategoriesEnrichmentMessage(2, activityId, ZonedDateTime.now(), new EnrichmentMessageKey(userId, randomUUID()), List.of(), 1, 1),
                clientUserToken(userId, CLAIM_DATA_ENRICHMENT_CATEGORIZATION));

        verify(this.logAppender, atLeastOnce()).doAppend(this.loggingEventArgumentCaptor.capture());
        Assertions.assertTrue(loggingEventArgumentCaptor.getAllValues().stream()
                .anyMatch(iLoggingEvent -> iLoggingEvent.getFormattedMessage().equals("Version 2 for CATEGORIES not supported yet.")));
    }

    @Test
    public void testIrrelevantNotSupported() {
        var userId = randomUUID();
        var activityId = randomUUID();

        eventConsumer.consume(new CategoriesEnrichmentMessage(2, activityId, ZonedDateTime.now(), new EnrichmentMessageKey(userId, randomUUID()), List.of(), 1, 1),
                clientUserToken(userId, "some_claim"));

        verify(this.logAppender, atLeastOnce()).doAppend(this.loggingEventArgumentCaptor.capture());
        Assertions.assertTrue(loggingEventArgumentCaptor.getAllValues().stream()
                .anyMatch(iLoggingEvent -> iLoggingEvent.getFormattedMessage().equals("Data-Science not active for CATEGORIES. Skipping.")));
    }

    @Test
    public void testHandlePreprocessingEnrichment() {

        var userId = UUID.randomUUID();
        var activityId = UUID.randomUUID();
        var clientUserToken = clientUserToken(userId, "dummy-claim");

        var preprocessing = new PreprocessingEnrichmentMessage(PREPROCESSING, 1, activityId, ZonedDateTime.now(), new EnrichmentMessageKey(userId, UUID.randomUUID()), 1, 1);
        eventConsumer.consume(preprocessing, clientUserToken);

        verifyNoMoreInteractions(transactionEnrichmentsMessageHandler, activityEnrichmentService);
    }

    @Test
    public void testHandleCategoriesEnrichment() throws URISyntaxException, IOException {
        var userId = UUID.fromString("751e7b04-5eee-47ef-8a36-2d2d409d1b00");
        var lines = lines(get(getClass().getClassLoader().getResource("data/ds-categories-enrichment.json").toURI())).collect(joining());
        var payload = objectMapper.readValue(lines, CategoriesEnrichmentMessage.class);
        var clientUserToken = clientUserToken(userId, CLAIM_DATA_ENRICHMENT_CATEGORIZATION);

        eventConsumer.consume(payload, clientUserToken);

        verify(transactionEnrichmentsMessageHandler).process(categoriesEnrichmentMessageArgumentCaptor.capture());
        var enrichmentMessage = categoriesEnrichmentMessageArgumentCaptor.getValue();
        assertThat(enrichmentMessage.getDomain()).isEqualTo(CATEGORIES);
        assertThat(enrichmentMessage.getMessageKey().getUserId()).isEqualTo(userId);
        assertThat(enrichmentMessage.getTransactions().size()).isEqualTo(1);
        assertThat(enrichmentMessage.getTransactions().get(0).getKey().getAccountId()).isEqualTo(UUID.fromString("f7a10a55-33e0-4e43-b723-35e73b03cf67"));
        assertThat(enrichmentMessage.getTransactions().get(0).getCategory()).isEqualTo("Drinks");
    }

    @Test
    public void testHandleLabelsEnrichment() throws URISyntaxException, IOException {
        var userId = UUID.fromString("751e7b04-5eee-47ef-8a36-2d2d409d1b00");
        var lines = lines(get(getClass().getClassLoader().getResource("data/ds-labels-enrichment.json").toURI())).collect(joining());
        var payload = objectMapper.readValue(lines, LabelsEnrichmentMessage.class);
        var clientUserToken = clientUserToken(userId, CLAIM_DATA_ENRICHMENT_LABELS);

        eventConsumer.consume(payload, clientUserToken);

        verify(transactionEnrichmentsMessageHandler).process(labelsEnrichmentMessageArgumentCaptor.capture());
        var enrichmentMessage = labelsEnrichmentMessageArgumentCaptor.getValue();
        assertThat(enrichmentMessage.getDomain()).isEqualTo(LABELS);
        assertThat(enrichmentMessage.getMessageKey().getUserId()).isEqualTo(userId);
        assertThat(enrichmentMessage.getTransactions().size()).isEqualTo(1);
        assertThat(enrichmentMessage.getTransactions().get(0).getKey().getAccountId()).isEqualTo(UUID.fromString("f7a10a55-33e0-4e43-b723-35e73b03cf67"));
        assertThat(enrichmentMessage.getTransactions().get(0).getLabels()).containsAll(List.of("Red", "Yellow"));
    }

    @Test
    public void testHandleCounterPartiesEnrichment() throws URISyntaxException, IOException {
        var userId = UUID.fromString("751e7b04-5eee-47ef-8a36-2d2d409d1b00");
        var lines = lines(get(getClass().getClassLoader().getResource("data/ds-counterparties-enrichment.json").toURI())).collect(joining());
        var payload = objectMapper.readValue(lines, CounterpartiesEnrichmentMessage.class);
        var clientUserToken = clientUserToken(userId, CLAIM_DATA_ENRICHMENT_MERCHANT_RECOGNITION);

        eventConsumer.consume(payload, clientUserToken);

        verify(transactionEnrichmentsMessageHandler).process(counterpartiesEnrichmentMessageArgumentCaptor.capture());
        var enrichmentMessage = counterpartiesEnrichmentMessageArgumentCaptor.getValue();
        assertThat(enrichmentMessage.getDomain()).isEqualTo(COUNTER_PARTIES);
        assertThat(enrichmentMessage.getMessageKey().getUserId()).isEqualTo(UUID.fromString("751e7b04-5eee-47ef-8a36-2d2d409d1b00"));
        assertThat(enrichmentMessage.getTransactions().size()).isEqualTo(1);
        assertThat(enrichmentMessage.getTransactions().get(0).getKey().getAccountId()).isEqualTo(UUID.fromString("f7a10a55-33e0-4e43-b723-35e73b03cf67"));
        assertThat(enrichmentMessage.getTransactions().get(0).getCounterparty()).isEqualTo("Ajax");
        Assertions.assertTrue(enrichmentMessage.getTransactions().get(0).isMerchant());
    }

    @Test
    public void testHandleTransactionCyclesEnrichment() throws URISyntaxException, IOException {
        var userId = UUID.fromString("751e7b04-5eee-47ef-8a36-2d2d409d1b00");
        var lines = lines(get(getClass().getClassLoader().getResource("data/ds-transactioncycles-enrichment.json").toURI())).collect(joining());
        var payload = objectMapper.readValue(lines, CyclesEnrichmentMessage.class);
        var clientUserToken = clientUserToken(userId, CLAIM_DATA_ENRICHMENT_CYCLE_DETECTION);


        eventConsumer.consume(payload, clientUserToken);

        verify(transactionEnrichmentsMessageHandler).process(cyclesEnrichmentMessageArgumentCaptor.capture());
        var enrichmentMessage = cyclesEnrichmentMessageArgumentCaptor.getValue();
        assertThat(enrichmentMessage.getDomain()).isEqualTo(TRANSACTION_CYCLES);
        assertThat(enrichmentMessage.getMessageKey().getUserId()).isEqualTo(UUID.fromString("751e7b04-5eee-47ef-8a36-2d2d409d1b00"));
        assertThat(enrichmentMessage.getTransactions().size()).isEqualTo(1);
        assertThat(enrichmentMessage.getTransactions().get(0).getKey().getAccountId()).isEqualTo(UUID.fromString("f7a10a55-33e0-4e43-b723-35e73b03cf67"));
        assertThat(enrichmentMessage.getTransactions().get(0).getKey().getTransactionId()).isEqualToIgnoringCase("5ecd3a7d-0b4c-42ac-8645-6371a11b7d85");
        assertThat(enrichmentMessage.getTransactions().get(0).getKey().getUserId()).isEqualTo(UUID.fromString("751e7b04-5eee-47ef-8a36-2d2d409d1b00"));
        assertThat(enrichmentMessage.getTransactions().get(0).getKey().getDate()).isEqualTo("2020-08-04");
        assertThat(enrichmentMessage.getTransactions().get(0).getCycleId()).isEqualTo(UUID.fromString("6005c4ee-5abc-4219-bbb0-91e39a14cff3"));
        assertThat(enrichmentMessage.getCycles().getCredits().size()).isEqualTo(1);
        assertThat(enrichmentMessage.getCycles().getCredits().get(0).getAmount()).isEqualTo("5.0");
        assertThat(enrichmentMessage.getCycles().getCredits().get(0).getCurrency()).isEqualTo(Currency.getInstance("GBP"));
        assertThat(enrichmentMessage.getCycles().getCredits().get(0).getPeriod()).isEqualTo(Period.parse("P1M"));
        assertThat(enrichmentMessage.getCycles().getCredits().get(0).getPredictedOccurrences().size()).isEqualTo(1);
        assertThat(enrichmentMessage.getCycles().getCredits().get(0).getPredictedOccurrences()).allMatch(occurrence -> occurrence.equals(LocalDate.parse("2020-08-04")));
        assertThat(enrichmentMessage.getCycles().getCredits().get(0).getLabel()).isEqualTo(Optional.of("RENT"));

        assertThat(enrichmentMessage.getCycles().getDebits().size()).isEqualTo(0);
    }

    private ClientUserToken clientUserToken(UUID userId, String... claims) {
        var clientId = randomUUID();
        var serialized = encodeBase64String(format("fake-client-token-for-%s", clientId).getBytes());

        var jwtClaims = new JwtClaims();
        jwtClaims.setClaim("client-id", clientId.toString());
        jwtClaims.setClaim("user-id", userId.toString());
        Stream.of(claims)
                .forEach(claim -> jwtClaims.setClaim(claim, true));

        return new ClientUserToken(serialized, jwtClaims);
    }
}