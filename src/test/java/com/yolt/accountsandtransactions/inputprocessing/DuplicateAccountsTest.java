package com.yolt.accountsandtransactions.inputprocessing;

import com.yolt.accountsandtransactions.TestAccountBuilder;
import com.yolt.accountsandtransactions.accounts.Account;
import com.yolt.accountsandtransactions.accounts.AccountService;
import com.yolt.accountsandtransactions.datascience.DataScienceService;
import com.yolt.accountsandtransactions.datascience.TransactionSyncService;
import com.yolt.accountsandtransactions.inputprocessing.dataquality.StartBalanceStrategyAnalyzer;
import com.yolt.accountsandtransactions.inputprocessing.enrichments.activities.ActivityEnrichmentService;
import com.yolt.accountsandtransactions.transactions.TransactionRepository;
import nl.ing.lovebird.clienttokens.ClientUserToken;
import nl.ing.lovebird.extendeddata.common.CurrencyCode;
import nl.ing.lovebird.providerdomain.AccountType;
import nl.ing.lovebird.providerdomain.ProviderAccountNumberDTO;
import org.junit.After;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.Clock;
import java.util.Arrays;
import java.util.Collections;
import java.util.UUID;
import java.util.stream.Stream;

import static com.yolt.accountsandtransactions.Predef.some;
import static java.util.Optional.empty;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
public class DuplicateAccountsTest {

    private static final UUID ACTIVITY_ID = UUID.randomUUID();
    private static final UUID USER_ID = UUID.randomUUID();
    private static final AccountsAndTransactionsRequestDTO REQUEST_DTO = prepareIncomingRequestWithDuplicateAccounts();

    private AccountsAndTransactionsService service;

    @Mock
    private DataScienceService dataScienceServiceMock;

    @Mock
    private AccountService accountServiceMock;

    @Mock
    private AccountsAndTransactionsFinishedActivityEventProducer producerMock;

    @Mock
    private ActivityEnrichmentService activityEnrichmentServiceMock;

    @Mock
    private StartBalanceStrategyAnalyzer startBalanceStrategyAnalyzer;

    @Mock
    private AccountIdProvider<UUID> accountIdProvider;

    @Mock
    private TransactionIdProvider<UUID> transactionIdProvider;

    @Mock
    private TransactionSyncService transactionSyncService;

    @Mock
    private AccountsAndTransactionDiagnosticsService accountsAndTransactionDiagnosticsService;

    @Mock
    private TransactionRepository transactionRepository;

    @BeforeEach
    public void setUp() {
        when(accountServiceMock.getAccountsForUserSite(any(), any(UUID.class)))
                .thenReturn(Collections.emptyList());
        when(dataScienceServiceMock.getDatesPendingTransactions(USER_ID, Collections.emptyList()))
                .thenReturn(Stream.<String>builder().build());
        when(accountServiceMock.createOrUpdateAccount(any(), eq(REQUEST_DTO.getIngestionAccounts().get(0)), any(), any(), any(), anyBoolean(), any()))
                .thenReturn(buildFromIngestionAccount(REQUEST_DTO.getIngestionAccounts().get(0)));
        when(transactionSyncService.reconcile(any(),
                any(UUID.class), eq(Collections.emptyList()), eq("TRIODOS")))
                .thenReturn(new TransactionInsertionStrategy.Instruction(Collections.emptyList(), Collections.emptyList(), Collections.emptyList(), Collections.emptyList(), null, empty()));

        service = new AccountsAndTransactionsService(
                dataScienceServiceMock,
                producerMock,
                accountServiceMock,
                null,
                transactionSyncService,
                activityEnrichmentServiceMock,
                startBalanceStrategyAnalyzer,
                accountIdProvider,
                transactionIdProvider,
                accountsAndTransactionDiagnosticsService,
                transactionRepository,
                Clock.systemUTC()
        );
    }

    @After
    public void tearDown() {
        verifyNoMoreInteractions(accountServiceMock);
    }

    @Test
    public void whenDuplicatedAccountsInRequest_shouldDedupThem() {
        var clientUserToken = mock(ClientUserToken.class);
        var userId = USER_ID;
        when(clientUserToken.getUserIdClaim()).thenReturn(userId);
        service.processAccountsAndTransactionsForUserSite(clientUserToken, REQUEST_DTO);

        verify(accountServiceMock).getAccountsForUserSite(any(), any(UUID.class));
        verify(accountServiceMock, times(2)).createOrUpdateAccount(any(), eq(REQUEST_DTO.getIngestionAccounts().get(0)), any(), any(), any(), anyBoolean(), any());
    }

    private static AccountsAndTransactionsRequestDTO prepareIncomingRequestWithDuplicateAccounts() {
        AccountFromProviders accountFromProviders = AccountFromProviders.accountsFromProvidersBuilder()
                .accountId("sjx985uctv1o2t25n8zfh473hefeh9sorwdu")
                .accountNumber(new ProviderAccountNumberDTO(
                        ProviderAccountNumberDTO.Scheme.IBAN, "XXXX XXXX XXXX 1234"
                ))
                .provider("TRIODOS")
                .currency(CurrencyCode.GBP)
                .name("Name")
                .yoltUserId(USER_ID)
                .yoltAccountType(AccountType.CREDIT_CARD)
                .transactions(Collections.emptyList())
                .build();

        return AccountsAndTransactionsRequestDTO.builder()
                .activityId(ACTIVITY_ID)
                .userSiteId(UUID.randomUUID())
                .siteId(UUID.randomUUID())
                .ingestionAccounts(Arrays.asList(accountFromProviders, accountFromProviders))
                .build();
    }

    private static Account buildFromIngestionAccount(AccountFromProviders accountFromProviders) {
        return TestAccountBuilder.builder()
                .id(UUID.randomUUID())
                .externalId(accountFromProviders.getAccountId())
                .accountNumber(new Account.AccountNumber(
                        some(accountFromProviders.getName()),
                        some(Account.AccountNumber.Scheme.IBAN),
                        some(accountFromProviders.getAccountNumber().getIdentification())))
                .currency(accountFromProviders.getCurrency())
                .name(accountFromProviders.getName())
                .build();
    }
}
