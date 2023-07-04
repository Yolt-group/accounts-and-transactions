package com.yolt.accountsandtransactions.inputprocessing.enrichments.activities;

import brave.baggage.BaggageField;
import com.yolt.accountsandtransactions.inputprocessing.AccountsAndTransactionsRequestDTO;
import com.yolt.accountsandtransactions.inputprocessing.enrichments.TransactionEnrichmentsFinishedActivityEventProducer;
import com.yolt.accountsandtransactions.inputprocessing.enrichments.UsersClient;
import com.yolt.accountsandtransactions.inputprocessing.enrichments.api.EnrichmentMessageType;
import com.yolt.accountsandtransactions.inputprocessing.enrichments.api.base.EnrichmentMessage;
import com.yolt.accountsandtransactions.inputprocessing.enrichments.api.base.EnrichmentMessageKey;
import com.yolt.accountsandtransactions.inputprocessing.enrichments.api.categories.CategoriesEnrichmentMessage;
import com.yolt.accountsandtransactions.inputprocessing.enrichments.api.counterparties.CounterpartiesEnrichmentMessage;
import com.yolt.accountsandtransactions.inputprocessing.enrichments.api.cycles.CyclesEnrichmentMessage;
import com.yolt.accountsandtransactions.inputprocessing.enrichments.api.cycles.DsTransactionCycles;
import com.yolt.accountsandtransactions.inputprocessing.enrichments.api.labels.LabelsEnrichmentMessage;
import com.yolt.accountsandtransactions.inputprocessing.enrichments.api.preprocessing.PreprocessingEnrichmentMessage;
import com.yolt.accountsandtransactions.metrics.AccountsAndTransactionMetrics;
import com.yolt.accountsandtransactions.transactions.enrichments.api.CategoryTransactionEnrichment;
import com.yolt.accountsandtransactions.transactions.enrichments.api.CounterpartyTransactionEnrichment;
import com.yolt.accountsandtransactions.transactions.enrichments.api.LabelsTransactionEnrichment;
import nl.ing.lovebird.clienttokens.ClientUserToken;
import org.jose4j.jwt.JwtClaims;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.Clock;
import java.time.LocalDate;
import java.time.ZonedDateTime;
import java.util.*;
import java.util.stream.Stream;

import static com.yolt.accountsandtransactions.inputprocessing.enrichments.activities.ActivityEnrichmentType.REFRESH;
import static java.lang.String.format;
import static java.util.UUID.randomUUID;
import static nl.ing.lovebird.clienttokens.constants.ClientTokenConstants.*;
import static org.apache.tomcat.util.codec.binary.Base64.encodeBase64String;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
public class ActivityEnrichmentServiceTest {
    private static final UUID USER_ID = randomUUID();

    private ActivityEnrichmentService activityEnrichmentService;

    @Mock
    private TransactionEnrichmentsFinishedActivityEventProducer transactionEnrichmentsFinishedActivityEventProducer;

    @Mock
    private AccountsAndTransactionMetrics accountsAndTransactionMetrics;

    @Mock
    private ActivityEnrichmentRepository activityEnrichmentRepository;

    @Mock
    private UsersClient usersClient;

    @BeforeEach
    public void init() {
        activityEnrichmentService = new ActivityEnrichmentService(10, activityEnrichmentRepository, transactionEnrichmentsFinishedActivityEventProducer, accountsAndTransactionMetrics, Clock.systemUTC(), BaggageField.create("user-id"), BaggageField.create("client-id"), usersClient);
    }

    @ParameterizedTest
    @EnumSource(ActivityEnrichmentType.class)
    public void given_noEnrichmentClaimsForClient_enrichmentShouldNotBeStarted(ActivityEnrichmentType enrichmentType) {
        activityEnrichmentService.startActivityEnrichment(clientUserTokenWithNoEnrichments(), enrichmentType, UUID.randomUUID());
        verify(activityEnrichmentRepository, times(0)).save(any());
    }

    /**
     * This is because we start the activity multiple times for 1 activity refresh-user-sites with multiple user sites.
     * Once per each usersite in {@link com.yolt.accountsandtransactions.inputprocessing.AccountsAndTransactionsService#processAccountsAndTransactionsForUserSite(ClientUserToken, AccountsAndTransactionsRequestDTO)}
     */
    @Test
    public void given_thatWeCallStartActivityRefreshTwiceThenThatShouldBeIdempotent() {
        mockActivityRepositoryAsInMemoryRepository();
        var activityId = UUID.randomUUID();
        activityEnrichmentService.startActivityEnrichment(clientTokenWithAllEnrichmentsEnabled(), REFRESH, activityId);
        activityEnrichmentService.startActivityEnrichment(clientTokenWithAllEnrichmentsEnabled(), REFRESH, activityId);

        // Store it only once.
        verify(activityEnrichmentRepository, times(1)).save(any());
    }

    @Test
    public void given_noEnrichmentClaimsForClient_enrichmentEventShouldNotBeProcessed() {
        CategoriesEnrichmentMessage categoriesEnrichmentMessage = new CategoriesEnrichmentMessage(0, randomUUID(), ZonedDateTime.now(), new EnrichmentMessageKey(randomUUID(), randomUUID()), List.of(), 1, 1);
        activityEnrichmentService.processActivityEnrichment(categoriesEnrichmentMessage, Set.of(), clientUserTokenWithNoEnrichments());
        verifyNoInteractions(activityEnrichmentRepository, transactionEnrichmentsFinishedActivityEventProducer);
    }

    @Test
    void given_AStartedRefresh_andAnActivityEnrichmentForAllTypes_then_aTransactionFinishedEventShouldBeSent() {
        mockActivityRepositoryAsInMemoryRepository();
        var activityId = UUID.randomUUID();
        ClientUserToken clientUserToken = clientTokenWithAllEnrichmentsEnabled();

        activityEnrichmentService.startActivityEnrichment(clientUserToken, REFRESH, activityId);
        activityEnrichmentService.processActivityEnrichment(createEnrichmentMessage(EnrichmentMessageType.CATEGORIES, activityId, clientUserToken.getUserIdClaim()), Set.of(), clientUserToken);
        activityEnrichmentService.processActivityEnrichment(createEnrichmentMessage(EnrichmentMessageType.LABELS, activityId, clientUserToken.getUserIdClaim()), Set.of(), clientUserToken);
        activityEnrichmentService.processActivityEnrichment(createEnrichmentMessage(EnrichmentMessageType.COUNTER_PARTIES, activityId, clientUserToken.getUserIdClaim()), Set.of(), clientUserToken);
        activityEnrichmentService.processActivityEnrichment(createEnrichmentMessage(EnrichmentMessageType.TRANSACTION_CYCLES, activityId, clientUserToken.getUserIdClaim()), Set.of(), clientUserToken);

        verify(transactionEnrichmentsFinishedActivityEventProducer, times(1)).markAsCompleted(eq(activityId), any(), any(), eq(clientUserToken));
    }

    /**
     * This is because that is how the current pipeline works:
     * A feedback event will trigger 1 enrichmentEvent with the processed feedback.
     * Then a full DS pipeline is triggered to make sure those changes are propagated to other enrichments.
     * For example: The transaction cycles are dependent on the categories. If categories change, the full pipeline is triggered
     * which is turns updates the other enrichment types.
     */
    @ParameterizedTest
    @EnumSource(ActivityEnrichmentType.class)
    void given_AFeedBackEvent_andAnActivityEnrichmentForAllTypesPlusTheFeedBackEvent_then_aTransactionFinishedEventShouldBeSent(ActivityEnrichmentType activityEnrichmentType) {
        if (activityEnrichmentType == REFRESH) {
            // skip this one, does not apply.
            return;
        }

        // Given
        mockActivityRepositoryAsInMemoryRepository();
        var activityId = UUID.randomUUID();
        ClientUserToken clientUserToken = clientTokenWithAllEnrichmentsEnabled();

        // When
        activityEnrichmentService.startActivityEnrichment(clientUserToken, activityEnrichmentType, activityId);
        activityEnrichmentService.processActivityEnrichment(createEnrichmentMessage(EnrichmentMessageType.CATEGORIES, activityId, clientUserToken.getUserIdClaim()), Set.of(), clientUserToken);
        activityEnrichmentService.processActivityEnrichment(createEnrichmentMessage(EnrichmentMessageType.LABELS, activityId, clientUserToken.getUserIdClaim()), Set.of(), clientUserToken);
        activityEnrichmentService.processActivityEnrichment(createEnrichmentMessage(EnrichmentMessageType.COUNTER_PARTIES, activityId, clientUserToken.getUserIdClaim()), Set.of(), clientUserToken);
        activityEnrichmentService.processActivityEnrichment(createEnrichmentMessage(EnrichmentMessageType.TRANSACTION_CYCLES, activityId, clientUserToken.getUserIdClaim()), Set.of(), clientUserToken);

        // Then it is not finished yet
        verify(transactionEnrichmentsFinishedActivityEventProducer, times(0)).markAsCompleted(eq(activityId), any(), any(), eq(clientUserToken));

        // When
        activityEnrichmentType.additionalEnrichments.forEach(additionalEnrichment ->
                activityEnrichmentService.processActivityEnrichment(createEnrichmentMessage(additionalEnrichment, activityId, clientUserToken.getUserIdClaim()), Set.of(), clientUserToken));
        // Then it is done
        verify(transactionEnrichmentsFinishedActivityEventProducer, times(1)).markAsCompleted(eq(activityId), any(), any(), eq(clientUserToken));
    }

    @Test
    void given_ABunchOfEnrichmentMessages_then_weShouldAccumulateAllTheImpactedAccountIdsWithTheOldestTransactions() {
        mockActivityRepositoryAsInMemoryRepository();
        var activityId = UUID.randomUUID();
        ClientUserToken clientUserToken = clientTokenWithAllEnrichmentsEnabled();
        UUID userId = clientUserToken.getUserIdClaim();
        var accountId1 = UUID.randomUUID();
        var oldestTrnxAccount1 = LocalDate.of(2020, 1, 1);
        var accountId2 = UUID.randomUUID();
        var oldestTrnxAccount2 = LocalDate.of(2020, 2, 2);

        // Given started refresh:
        activityEnrichmentService.startActivityEnrichment(clientUserToken, REFRESH, activityId);

        // Given a category enrichment -pages with an old transaction transId1 on account 1
        activityEnrichmentService.processActivityEnrichment(
                new CategoriesEnrichmentMessage(0, activityId, ZonedDateTime.now(), new EnrichmentMessageKey(userId, randomUUID()), List.of(), 0, 1),
                Set.of(
                        new CategoryTransactionEnrichment(userId, accountId1, oldestTrnxAccount1, "transId1", "category", Optional.empty(), Optional.empty())
                ),
                clientUserToken);

        // Given empty transaction cycles enrichment
        activityEnrichmentService.processActivityEnrichment(createEnrichmentMessage(EnrichmentMessageType.TRANSACTION_CYCLES, activityId, userId), Set.of(), clientUserToken);

        // Given a label enrichment with an old transaction transId2 on account 2
        activityEnrichmentService.processActivityEnrichment(
                createEnrichmentMessage(EnrichmentMessageType.LABELS, activityId, userId),
                Set.of(
                        new LabelsTransactionEnrichment(userId, accountId2, oldestTrnxAccount2, "transId2", Set.of())
                ),
                clientUserToken);
        // Given counterparties with a recent trx only
        activityEnrichmentService.processActivityEnrichment(createEnrichmentMessage(
                        EnrichmentMessageType.COUNTER_PARTIES, activityId, userId),
                Set.of(
                        new CounterpartyTransactionEnrichment(userId, accountId2, LocalDate.now(), "transId3", "counterparty", true)
                ),
                clientUserToken);


        // then the oldest trx with accounts should be sent on the transaction finished event.
        verify(transactionEnrichmentsFinishedActivityEventProducer, times(1)).markAsCompleted(eq(activityId), eq(Map.of(accountId1, oldestTrnxAccount1, accountId2, oldestTrnxAccount2)), any(), eq(clientUserToken));
    }


    private void mockActivityRepositoryAsInMemoryRepository() {
        Map<UUID, ActivityEnrichment> activityEnrichmentMap = new HashMap<>();

        when(activityEnrichmentRepository.save(any(ActivityEnrichment.class))).thenAnswer(invocationOnMock -> {
            ActivityEnrichment argument = invocationOnMock.getArgument(0);
            activityEnrichmentMap.put(argument.getActivityId(), argument);
            return null;
        });

        when(activityEnrichmentRepository.findById(any(UUID.class)))
                .thenAnswer(invocationOnMock -> {
                    UUID activityId = invocationOnMock.getArgument(0);
                    return Optional.ofNullable(activityEnrichmentMap.get(activityId))
                            .map(it -> {
                                // This is some behaviour of JPA. if you get the entity from the repo, you get a mutable PersistentSet...
                                ActivityEnrichment activityWithMutableAccountSet = new ActivityEnrichment(it.getActivityId(), it.getStartedAt(), it.getEnrichmentType(), it.getUserId(), it.getChecksum(),
                                        new HashSet<>(it.getActivityEnrichmentAccounts()));
                                activityEnrichmentMap.replace(activityId, activityWithMutableAccountSet);
                                return activityWithMutableAccountSet;
                            });
                });
    }

    EnrichmentMessage createEnrichmentMessage(EnrichmentMessageType type, UUID activityId, UUID userId) {
        EnrichmentMessageKey enrichmentMessageKey = new EnrichmentMessageKey(userId, randomUUID());

        return switch (type) {
            case CATEGORIES -> new CategoriesEnrichmentMessage(0, activityId, ZonedDateTime.now(), enrichmentMessageKey, List.of(), 0, 1);
            case COUNTER_PARTIES -> new CounterpartiesEnrichmentMessage(0, activityId, ZonedDateTime.now(), enrichmentMessageKey, List.of(), 0, 1);
            case TRANSACTION_CYCLES -> new CyclesEnrichmentMessage(0, activityId, ZonedDateTime.now(), enrichmentMessageKey, List.of(), new DsTransactionCycles(List.of(), List.of()), 0, 1);
            case LABELS -> new LabelsEnrichmentMessage(0, activityId, ZonedDateTime.now(), enrichmentMessageKey, List.of(), 0, 1);
            case PREPROCESSING -> new PreprocessingEnrichmentMessage(EnrichmentMessageType.values()[0], 0L, activityId, ZonedDateTime.now(), enrichmentMessageKey, 0, 1);

        };
    }

    private ClientUserToken clientTokenWithAllEnrichmentsEnabled() {
        return clientUserToken(CLAIM_DATA_ENRICHMENT_MERCHANT_RECOGNITION, CLAIM_DATA_ENRICHMENT_CATEGORIZATION, CLAIM_DATA_ENRICHMENT_LABELS, CLAIM_DATA_ENRICHMENT_CYCLE_DETECTION);
    }

    private ClientUserToken clientUserToken(String... claims) {
        var clientId = randomUUID();
        var serialized = encodeBase64String(format("fake-client-token-for-%s", clientId).getBytes());

        var jwtClaims = new JwtClaims();
        jwtClaims.setClaim("client-id", clientId.toString());
        jwtClaims.setClaim("user-id", USER_ID.toString());
        Stream.of(claims)
                .forEach(claim -> jwtClaims.setClaim(claim, true));

        return new ClientUserToken(serialized, jwtClaims);
    }

    private ClientUserToken clientUserTokenWithNoEnrichments() {
        var clientId = randomUUID();
        var serialized = encodeBase64String(format("fake-client-token-for-%s", clientId).getBytes());

        var claims = new JwtClaims();
        claims.setClaim("client-id", clientId.toString());
        claims.setClaim("user-id", USER_ID.toString());

        return new ClientUserToken(serialized, claims);
    }
}