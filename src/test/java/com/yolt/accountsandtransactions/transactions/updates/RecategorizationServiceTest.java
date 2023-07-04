package com.yolt.accountsandtransactions.transactions.updates;

import com.yolt.accountsandtransactions.datascience.categories.DataScienceRequestFailedException;
import com.yolt.accountsandtransactions.datascience.preprocessing.PreProcessingServiceClient;
import com.yolt.accountsandtransactions.inputprocessing.enrichments.activities.ActivityEnrichmentService;
import com.yolt.accountsandtransactions.metrics.AccountsAndTransactionMetrics;
import com.yolt.accountsandtransactions.transactions.TransactionService;
import com.yolt.accountsandtransactions.transactions.updates.api.SeedTransactionKey;
import com.yolt.accountsandtransactions.transactions.updates.updatesession.BulkUpdateSessionService;
import nl.ing.lovebird.clienttokens.ClientUserToken;
import nl.ing.lovebird.clienttokens.test.TestJwtClaims;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import reactor.core.publisher.Mono;

import java.util.UUID;

import static com.yolt.accountsandtransactions.inputprocessing.enrichments.activities.ActivityEnrichmentType.FEEDBACK_CATEGORIES;
import static java.util.UUID.randomUUID;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;


@ExtendWith(MockitoExtension.class)
public class RecategorizationServiceTest {
    private static final UUID USER_ID = randomUUID();
    private static final UUID CLIENT_GROUP_ID = randomUUID();
    private static final UUID CLIENT_ID = randomUUID();
    private static final UUID ACCOUNT_ID = randomUUID();

    private RecategorizationService recategorizationService;

    @Mock
    private PreProcessingServiceClient preProcessingServiceClient;
    @Mock
    private AccountsAndTransactionMetrics accountsAndTransactionMetrics;
    @Mock
    private ActivityEnrichmentService activityEnrichmentService;
    @Mock
    private TransactionService transactionService;
    @Mock
    private BulkUpdateSessionService bulkUpdateSessionService;

    @BeforeEach
    public void init() {
        recategorizationService = new RecategorizationService(
                preProcessingServiceClient,
                activityEnrichmentService,
                transactionService,
                bulkUpdateSessionService,
                accountsAndTransactionMetrics);
    }

    @Test
    public void applyFeedbackSuccessfully() {
        var clientUserToken = clientUserToken();
        var transactionKey = SeedTransactionKey.builder().accountId(ACCOUNT_ID).build();
        var newCategory = "something else";

        when(preProcessingServiceClient.applyCategoriesFeedbackAsync(eq(clientUserToken), any(), eq(transactionKey), eq(newCategory)))
                .thenReturn(Mono.empty());

        var result = recategorizationService.applyFeedback(
                clientUserToken,
                transactionKey,
                newCategory);

        assertThat(result).isPresent();

        verify(activityEnrichmentService).startActivityEnrichment(any(), eq(FEEDBACK_CATEGORIES), any());
        verify(accountsAndTransactionMetrics).incrementSingleRecategorizationCount();
        verify(accountsAndTransactionMetrics, never()).incrementSingleRecategorizationFailureCount(any());
    }

    @Test
    public void applyFeedbackFailing() {
        var clientToken = clientUserToken();
        var transactionKey = SeedTransactionKey.builder().accountId(ACCOUNT_ID).build();
        var newCategory = "something else";

        when(preProcessingServiceClient.applyCategoriesFeedbackAsync(eq(clientToken), any(), eq(transactionKey), eq(newCategory)))
                .thenReturn(Mono.error(new DataScienceRequestFailedException("failed", 500, null, null)));

        var result = recategorizationService.applyFeedback(
                clientToken,
                transactionKey,
                newCategory);

        assertThat(result).isNotPresent();

        verify(activityEnrichmentService).startActivityEnrichment(any(), eq(FEEDBACK_CATEGORIES), any());
        verify(accountsAndTransactionMetrics, never()).incrementSingleRecategorizationCount();
        verify(accountsAndTransactionMetrics).incrementSingleRecategorizationFailureCount(any());
    }

    private static ClientUserToken clientUserToken() {
        return new ClientUserToken("mock-client-token", TestJwtClaims.createClientClaims("junit", CLIENT_GROUP_ID, CLIENT_ID));
    }
}