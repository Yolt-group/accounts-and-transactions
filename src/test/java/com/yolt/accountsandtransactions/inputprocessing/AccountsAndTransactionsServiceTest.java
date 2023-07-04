package com.yolt.accountsandtransactions.inputprocessing;

import com.yolt.accountsandtransactions.TestUtils;
import com.yolt.accountsandtransactions.accounts.Account;
import com.yolt.accountsandtransactions.accounts.AccountService;
import com.yolt.accountsandtransactions.datascience.DataScienceService;
import com.yolt.accountsandtransactions.datascience.DsTransaction;
import com.yolt.accountsandtransactions.datascience.TransactionSyncService;
import com.yolt.accountsandtransactions.inputprocessing.dataquality.StartBalanceStrategyAnalyzer;
import com.yolt.accountsandtransactions.inputprocessing.enrichments.activities.ActivityEnrichmentService;
import com.yolt.accountsandtransactions.transactions.TransactionRepository;
import com.yolt.accountsandtransactions.transactions.TransactionService;
import nl.ing.lovebird.activityevents.events.IngestionFinishedEvent;
import nl.ing.lovebird.clienttokens.ClientToken;
import nl.ing.lovebird.clienttokens.ClientUserToken;
import nl.ing.lovebird.clienttokens.test.TestJwtClaims;
import nl.ing.lovebird.extendeddata.common.CurrencyCode;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.LocalDate;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.yolt.accountsandtransactions.TestAccountBuilder.builder;
import static com.yolt.accountsandtransactions.TestUtils.toTransaction;
import static java.time.Clock.systemUTC;
import static java.util.UUID.randomUUID;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
public class AccountsAndTransactionsServiceTest {

    @Mock
    private DataScienceService dataScienceService;
    @Mock
    private AccountsAndTransactionsFinishedActivityEventProducer accountsAndTransactionsFinishedActivityEventProducer;
    @Mock
    private AccountService accountService;
    @Mock
    private TransactionService transactionService;
    @Mock
    private TransactionSyncService transactionSyncService;
    @Mock
    private ActivityEnrichmentService activityEnrichmentService;
    @Captor
    private ArgumentCaptor<List<DsTransaction>> captor;
    @Mock
    private StartBalanceStrategyAnalyzer startBalanceStrategyAnalyzer;
    @Mock
    private AccountsAndTransactionDiagnosticsService accountsAndTransactionDiagnosticsService;

    private AccountsAndTransactionsService accountsAndTransactionsService;

    @Mock
    private AccountIdProvider<UUID> accountIdProvider;

    @Mock
    private TransactionIdProvider<UUID> transactionIdProvider;

    @Mock
    private TransactionRepository transactionRepository;

    @BeforeEach
    public void setup() {
        accountsAndTransactionsService = new AccountsAndTransactionsService(dataScienceService,
                accountsAndTransactionsFinishedActivityEventProducer,
                accountService,
                transactionService,
                transactionSyncService,
                activityEnrichmentService,
                startBalanceStrategyAnalyzer,
                accountIdProvider,
                transactionIdProvider,
                accountsAndTransactionDiagnosticsService,
                transactionRepository,
                systemUTC()
        );
    }

    @Test
    public void shouldProcessValidUpdate() throws Exception {
        final UUID accountId = randomUUID();
        final UUID activityId = randomUUID();
        final UUID userSiteId = randomUUID();
        final UUID userId = randomUUID();
        AccountsAndTransactionsRequestDTO ingestionRequest = TestUtils.ingestionRequestSuccessMessage(userId, activityId, userSiteId, "externalId", "PROVIDER_X", randomUUID());
        AccountFromProviders accountFromProviders = ingestionRequest.getIngestionAccounts().get(0);
        List<DsTransaction> extractedTransactions = accountFromProviders
                .getTransactions().stream().map(it -> toTransaction(userId, accountId, "EUR", it)).collect(Collectors.toList());

        Account account = builder()
                .userId(userId)
                .id(accountId)
                .currency(CurrencyCode.EUR)
                .userSiteId(userSiteId)
                .siteId(ingestionRequest.getSiteId())
                .build();

        when(accountService.getAccountsForUserSite(any(ClientUserToken.class), any())).thenReturn(Collections.singletonList(account));
        when(accountService.createOrUpdateAccount(any(), any(), eq(accountId), any(), any(), anyBoolean(), any())).thenReturn(account);

        when(dataScienceService.toDsTransactionList(eq(accountId), eq(userId), eq(CurrencyCode.EUR), any())).thenReturn(extractedTransactions);
        when(dataScienceService.getDatesPendingTransactions(userId, Collections.singletonList(accountId)))
                .thenReturn(Stream.of("2017-10", "2017-11"));
        when(transactionSyncService.reconcile(any(), eq(accountId), anyList(), eq("PROVIDER_X"))).thenReturn(
                new TransactionInsertionStrategy.Instruction(Collections.emptyList(), Collections.emptyList(), accountFromProviders.getTransactions().stream()
                        .map(t -> new ProviderTransactionWithId(t, "id"))
                        .collect(Collectors.toList()), Collections.emptyList(), null, Optional.of(LocalDate.EPOCH))
        );

        var clientToken = new ClientUserToken("mock-client-token", TestJwtClaims.createClientUserClaims("junit", randomUUID(), randomUUID(), userId));
        accountsAndTransactionsService.processAccountsAndTransactionsForUserSite(clientToken, ingestionRequest);

        verify(dataScienceService).saveTransactionBatch(captor.capture());
        verify(dataScienceService).saveAccount(account, accountFromProviders);
        verify(accountService, times(2)).createOrUpdateAccount(any(), any(), eq(accountId), any(), any(), anyBoolean(), any());
        ArgumentCaptor<IngestionFinishedEvent> ingestionFinishedEventArgumentCaptor = ArgumentCaptor.forClass(IngestionFinishedEvent.class);
        verify(accountsAndTransactionsFinishedActivityEventProducer).sendMessage(ingestionFinishedEventArgumentCaptor.capture(), any(ClientToken.class));
        assertThat(captor.getValue().get(0)).isEqualTo(extractedTransactions.get(0));
        assertThat(captor.getValue().get(1)).isEqualTo(extractedTransactions.get(1));
        assertThat(ingestionFinishedEventArgumentCaptor.getValue().getActivityId()).isEqualTo(activityId);
        assertThat(ingestionFinishedEventArgumentCaptor.getValue().getStartYearMonth()).isEqualTo("2017-10");
        assertThat(ingestionFinishedEventArgumentCaptor.getValue().getEndYearMonth()).isEqualTo("2017-11");
        assertThat(ingestionFinishedEventArgumentCaptor.getValue().getAccountIdToOldestTransactionChangeDate()).isEqualTo(Map.of(accountId, LocalDate.EPOCH));
    }

}
