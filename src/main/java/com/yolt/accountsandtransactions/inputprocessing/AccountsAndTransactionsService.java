package com.yolt.accountsandtransactions.inputprocessing;

import com.yolt.accountsandtransactions.ValidationException;
import com.yolt.accountsandtransactions.accounts.Account;
import com.yolt.accountsandtransactions.accounts.AccountService;
import com.yolt.accountsandtransactions.datascience.DataScienceService;
import com.yolt.accountsandtransactions.datascience.DsTransaction;
import com.yolt.accountsandtransactions.datascience.TransactionSyncService;
import com.yolt.accountsandtransactions.inputprocessing.TransactionInsertionStrategy.Instruction.InstructionType;
import com.yolt.accountsandtransactions.inputprocessing.dataquality.StartBalanceStrategyAnalyzer;
import com.yolt.accountsandtransactions.inputprocessing.enrichments.activities.ActivityEnrichmentService;
import com.yolt.accountsandtransactions.inputprocessing.matching.Matchers;
import com.yolt.accountsandtransactions.legacyaccounts.AccountMatcher;
import com.yolt.accountsandtransactions.legacyaccounts.AccountMatcher.AccountMatchResult;
import com.yolt.accountsandtransactions.transactions.Transaction;
import com.yolt.accountsandtransactions.transactions.TransactionRepository;
import com.yolt.accountsandtransactions.transactions.TransactionService;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import nl.ing.lovebird.activityevents.events.IngestionFinishedEvent;
import nl.ing.lovebird.clienttokens.ClientUserToken;
import nl.ing.lovebird.providerdomain.ProviderTransactionDTO;
import org.springframework.stereotype.Service;

import java.time.*;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.stream.Stream;

import static com.yolt.accountsandtransactions.inputprocessing.TransactionInsertionStrategy.EMPTY_INSTRUCTION;
import static com.yolt.accountsandtransactions.inputprocessing.TransactionInsertionStrategy.Mode.ACTIVE;
import static com.yolt.accountsandtransactions.inputprocessing.TransactionInsertionStrategy.Mode.TEST;
import static com.yolt.accountsandtransactions.inputprocessing.enrichments.activities.ActivityEnrichmentType.REFRESH;
import static com.yolt.accountsandtransactions.inputprocessing.matching.Matchers.ACTIVATED_ATTR_MATCHERS;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.toList;

@Service
@AllArgsConstructor
@Slf4j
public class AccountsAndTransactionsService {

    private static final DateTimeFormatter MONTH_TRUNCATING_DATE_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM");
    private static final String PROVIDER_BUDGET_INSIGHT = "BUDGET_INSIGHT";

    private final DataScienceService dataScienceService;
    private final AccountsAndTransactionsFinishedActivityEventProducer accountsAndTransactionsFinishedActivityEventProducer;
    private final AccountService accountService;
    private final TransactionService transactionService;
    private final TransactionSyncService transactionSyncService;
    private final ActivityEnrichmentService activityEnrichmentService;
    private final StartBalanceStrategyAnalyzer startBalanceStrategyAnalyzer;
    private final AccountIdProvider<UUID> accountIdProvider;
    private final TransactionIdProvider<UUID> transactionIdProvider;
    private final AccountsAndTransactionDiagnosticsService accountsAndTransactionDiagnosticsService;
    private final TransactionRepository transactionRepository;
    private final Clock clock;

    public void processAccountsAndTransactionsForUserSite(
            ClientUserToken clientUserToken,
            AccountsAndTransactionsRequestDTO accountsAndTransactionsRequestDTO
    ) {
        // Note down the current time.  We use this timestamp:
        // a) To update the field lastDataFetchTime for the accounts that were present in the message
        //    The lastDataFetchTime for all accounts that were included is updated (other accounts are
        //    not updated).  The set of accounts for which lastDataFetchTime contains the most recent timestamp are
        //    still 'connected': data is coming in from the bank.  Other accounts are 'disconnected': we are no longer
        //    receiving data.
        // b) As the time field in the IngestionFinished kafka message we send out to listeners.
        var currentTime = Instant.now(clock);

        UUID userSiteId = accountsAndTransactionsRequestDTO.getUserSiteId();
        UUID activityId = accountsAndTransactionsRequestDTO.getActivityId();
        UUID siteId = accountsAndTransactionsRequestDTO.getSiteId();
        List<Account> accountsForUserSite = accountService.getAccountsForUserSite(clientUserToken, userSiteId);

        Map<UUID, AccountFromProviders> processedAccountsByInternalId = new HashMap<>();

        // Mark the start of the enrichment of this activity.
        activityEnrichmentService.startActivityEnrichment(clientUserToken, REFRESH, activityId);

        // Some data providers from time to time send duplicated accounts in a single response.
        // While we check whether account exists in DB in order to make distinction whether to create or update it,
        // we haven't previously checked whether upstream accounts contain duplicates which resulted in saving
        // duplicates to DB.
        final List<AccountFromProviders> deduplicatedAccounts = AccountsDeduplicator.
                deduplicateAccounts(accountsAndTransactionsRequestDTO.getIngestionAccounts());

        // For the ingestion finished event  we need to know the absolute date-boundaries over which transactions are changed.
        // This date range is used by datascience. Datascience will take into account all transactions within this range for recategorization/
        // merchant-detection / transaction-cycles etc.
        // Those boundaries come from 1) the widest range of upstream transactions (all accounts) and 2) the widest range of deleted
        // pending transactions (all accounts).
        // We 'remember' the pending transaction range, because this will be changed in storeInDatascience.
        // Therefore, we cannot 'evaluate' this afterwards.
        MonthRange widestRangePendingTransactionsToBeDeleted = getMonthRangeOfPendingTransactionsOfUpstreamAccounts(accountsAndTransactionsRequestDTO, clientUserToken.getUserIdClaim(), accountsForUserSite);

        // Keep track of the oldest changes transaction for each account ingested.
        var accountIdToOldestTransactionChangeDate = new HashMap<UUID, LocalDate>();

        //
        // Process the accounts one by one.
        //
        for (AccountFromProviders upstreamAccount : deduplicatedAccounts) {
            DurationLogger perAccountIngestionDurationLogger = new DurationLogger();

            Optional<AccountMatchResult> existingAccountOpt = AccountMatcher.findExisting(accountsForUserSite, upstreamAccount, true);
            accountsAndTransactionDiagnosticsService.updateAccountMatchingStatistics(upstreamAccount, existingAccountOpt);

            boolean isAccountPresent = existingAccountOpt.isPresent();
            existingAccountOpt
                    .ifPresent(account -> validateCurrencyEquality(account.getAccount(), upstreamAccount));

            // Create and/or update the accounts repository
            UUID accountId = existingAccountOpt
                    .map(AccountMatchResult::getAccount)
                    .map(Account::getId) //existing account
                    .orElseGet(() -> {
                        log.info("Generating identifier for new account in user-site: {}", userSiteId);
                        return accountIdProvider.generate(upstreamAccount);
                    }); // new account

            Account account = accountService.createOrUpdateAccount(clientUserToken, upstreamAccount, accountId, userSiteId, siteId, isAccountPresent, null);
            dataScienceService.saveAccount(account, upstreamAccount);
            perAccountIngestionDurationLogger.addEntry("accounts");

            try {
                //
                // Persist the transactions.
                //
                TransactionInsertionStrategy.Instruction instruction = persistTransactions(
                        upstreamAccount,
                        clientUserToken,
                        perAccountIngestionDurationLogger,
                        account,
                        isAccountPresent
                );

                // Update account with lastDataFetchTime after transactions were persisted
                updateAccountWithLastDataFetchTime(clientUserToken, currentTime, userSiteId, siteId, upstreamAccount, isAccountPresent, accountId);

                // Store the oldest change transaction date for the account
                instruction.getOldestTransactionChangeDate()
                        .ifPresent(oldestTransactionChangeDate -> accountIdToOldestTransactionChangeDate.put(accountId, oldestTransactionChangeDate));

                // Update account status.
                processedAccountsByInternalId.put(accountId, upstreamAccount);

                //
                // Record metrics and diagnostics.
                //
                accountsAndTransactionDiagnosticsService.recordInvalidRemittanceInformation(instruction.getTransactionsToInsert(), upstreamAccount.getProvider());
                accountsAndTransactionDiagnosticsService.recordInvalidRemittanceInformation(instruction.getTransactionsToUpdate(), upstreamAccount.getProvider());
                accountsAndTransactionDiagnosticsService.logAccountIngestionReport(clientUserToken.getUserIdClaim(), accountId, upstreamAccount, instruction);
                accountsAndTransactionDiagnosticsService.updateTransactionProcessingDurationMetrics(perAccountIngestionDurationLogger);
                startBalanceStrategyAnalyzer.analyze(upstreamAccount, isAccountPresent, instruction.getTransactionsToInsert(), instruction.getTransactionsToUpdate(), instruction.getTransactionsToDelete());

            } catch (Exception e) {
                log.warn("Error during saving transactions batch for account, accountId {} , lastUpdatedTime on account wont be updated. Exception : {}", upstreamAccount.getAccountId(), e);
            }

        }

        MonthRange monthRange = determineDateRangeOfAllTransactions(accountsAndTransactionsRequestDTO, widestRangePendingTransactionsToBeDeleted);


        publishIngestionFinishedEvent(clientUserToken, monthRange, activityId, userSiteId, currentTime.atZone(ZoneOffset.UTC), processedAccountsByInternalId, accountIdToOldestTransactionChangeDate);
    }

    private void updateAccountWithLastDataFetchTime(ClientUserToken clientUserToken, Instant currentTime, UUID userSiteId, UUID siteId, AccountFromProviders upstreamAccount, boolean isAccountPresent, UUID accountId) {
        accountService.createOrUpdateAccount(clientUserToken, upstreamAccount, accountId, userSiteId, siteId, isAccountPresent, currentTime);
    }

    private void validateCurrencyEquality(final Account account, final AccountFromProviders upstreamAccount) {
        if (!account.getCurrency().equals(upstreamAccount.getCurrency())) {
            throw new ValidationException(
                    String.format("Account currency %s is different from provider accountCurrency %s",
                            account.getCurrency(), upstreamAccount.getCurrency())
            );
        }
    }

    /**
     * @param account         the account that is being updated
     * @param isUpdate        if false this is the initial refresh, if true it's an update of an existing account
     */
    private TransactionInsertionStrategy.Instruction persistTransactions(AccountFromProviders upstreamAccount, ClientUserToken clientUserToken, DurationLogger durationLogger, Account account, boolean isUpdate) {
        final TransactionInsertionStrategy strategy = getStrategyFor(upstreamAccount.getProvider());

        log.info("Matching: Using {} strategy for provider {}", strategy.getClass().getSimpleName(), upstreamAccount.getProvider());

        TransactionInsertionStrategy.Instruction instruction;
        try {
            instruction = strategy.determineTransactionPersistenceInstruction(upstreamAccount.getTransactions(), clientUserToken, account.getId(), upstreamAccount.getProvider(), upstreamAccount.getCurrency());
        } catch (Exception e) {
            accountsAndTransactionDiagnosticsService.logReconciliationFailureEvent(strategy.getMode(), upstreamAccount.getProvider(), account.getId(), e);
            updateMetrics(upstreamAccount, durationLogger, account, isUpdate, EMPTY_INSTRUCTION);
            throw e;
        }

        updateMetrics(upstreamAccount, durationLogger, account, isUpdate, instruction);

        { // delete
            if (!instruction.getTransactionsToDelete().isEmpty()) {
                List<TransactionService.TransactionPrimaryKey> transactionIdsToDelete = instruction.getTransactionsToDelete().stream()
                        .map(tx -> new TransactionService.TransactionPrimaryKey(clientUserToken.getUserIdClaim(), account.getId(), tx.getDate(), tx.getId(), tx.getStatus()))
                        .collect(toList());
                dataScienceService.deleteSpecificTransactions(transactionIdsToDelete);
                transactionService.deleteSpecificTransactions(transactionIdsToDelete);

                // --- begin log transactions to delete
                try {
                    if (Matchers.isActivatedAttributeMatcher(upstreamAccount.getProvider())) {
                        outputTransactionsInTabular(instruction.getTransactionsToDelete(),
                                (header, group) -> log.info("AttributeInsertionStrategy: Transactions to {} for provider {}:\n{}\n{}", "delete", upstreamAccount.getProvider(), header, String.join("\n", group)));
                    }
                } catch (Exception e) {
                    log.error("failed to output instructions in tabular format.", e);
                }
            }

            durationLogger.addEntry("trx_delete");
        }

        { // insert
            if (!instruction.getTransactionsToInsert().isEmpty()) {
                List<DsTransaction> newTrxs = dataScienceService.toDsTransactionList(account.getId(), clientUserToken.getUserIdClaim(), upstreamAccount.getCurrency(),
                        instruction.getTransactionsToInsert());
                dataScienceService.saveTransactionBatch(newTrxs);
                var transactionsToInsert = transactionService.saveTransactionsBatch(account.getId(), clientUserToken, upstreamAccount, instruction.getTransactionsToInsert(), InstructionType.INSERT);

                // --- begin log transactions to insert
                try {
                    if (Matchers.isActivatedAttributeMatcher(upstreamAccount.getProvider())) {
                        outputTransactionsInTabular(transactionsToInsert,
                                (header, group) -> log.info("AttributeInsertionStrategy: Transactions to {} for provider {}:\n{}\n{}", "insert", upstreamAccount.getProvider(), header, String.join("\n", group)));
                    }
                } catch (Exception e) {
                    log.error("failed to output instructions in tabular format.", e);
                }
            }

            durationLogger.addEntry("trx_insert");
        }

        { // update
            if (!instruction.getTransactionsToUpdate().isEmpty()) {
                List<DsTransaction> updatedTrxs = dataScienceService.toDsTransactionList(account.getId(), clientUserToken.getUserIdClaim(), upstreamAccount.getCurrency(),
                        instruction.getTransactionsToUpdate());
                dataScienceService.saveTransactionBatch(updatedTrxs);
                var transactionsToUpdate = transactionService.saveTransactionsBatch(account.getId(), clientUserToken, upstreamAccount, instruction.getTransactionsToUpdate(), InstructionType.UPDATE);

                // --- begin log transactions to update
                try {
                    if (Matchers.isActivatedAttributeMatcher(upstreamAccount.getProvider())) {
                        outputTransactionsInTabular(transactionsToUpdate,
                                (header, group) -> log.info("AttributeInsertionStrategy: Transactions to {} for provider {}:\n{}\n{}", "update", upstreamAccount.getProvider(), header, String.join("\n", group)));
                    }
                } catch (Exception e) {
                    log.error("failed to output instructions in tabular format.", e);
                }
            }
            durationLogger.addEntry("trx_update");
        }

        return instruction;
    }

    private TransactionInsertionStrategy getStrategyFor(final String provider) {
        final var activeStrategy = getActiveStrategyFor(provider);
        final var optionalPassiveStrategy = getPassiveStrategyFor(provider);

        if (optionalPassiveStrategy.isPresent()) {
            return new ActivePassiveTransactionInsertionStrategy(activeStrategy, optionalPassiveStrategy.get());
        }

        return activeStrategy;
    }

    private TransactionInsertionStrategy getActiveStrategyFor(String provider) {
        if (Matchers.isActivatedAttributeMatcher(provider)) {
            return new AttributeInsertionStrategy(ACTIVE, transactionRepository::getTransactionsInAccountFromDate, transactionIdProvider, ACTIVATED_ATTR_MATCHERS.get(provider));
        }

        if (PROVIDER_BUDGET_INSIGHT.equals(provider)) {
            return new DeltaTransactionInsertionStrategy();
        }

        return new DefaultTransactionInsertionStrategy(transactionSyncService);
    }

    private Optional<TransactionInsertionStrategy> getPassiveStrategyFor(String provider) {
        return Optional.empty(); // All test matchers have been disabled, given the decommissioning of Yolt.
    }

    private void updateMetrics(AccountFromProviders upstreamAccount, DurationLogger durationLogger, Account account, boolean isUpdate, TransactionInsertionStrategy.Instruction instruction) {
        durationLogger.addEntry("trx_reconcile");
        accountsAndTransactionDiagnosticsService.updatePerAccountStatistics(instruction, upstreamAccount, account, isUpdate);
    }

    static void outputTransactionsInTabular(final @NonNull List<Transaction> transactions,
                                            final @NonNull BiConsumer<String, List<String>> groupOut) {

        var format = "|%1$-36s|%2$-36s|%3$-12s|%4$-32s|%5$-24s|%6$-24s|%7$-16s|";
        var counter = new AtomicInteger();

        transactions.stream()
                .collect(groupingBy(ignored -> counter.getAndIncrement() / 50)) // output in chunks of 50
                .values()
                .forEach(group -> {
                    var tuples = group.stream()
                            .map(transaction -> String.format(format,
                                    transaction.getAccountId(),
                                    transaction.getId(),
                                    transaction.getBookingDate(),
                                    transaction.getStatus(),
                                    Optional.ofNullable(transaction.getExternalId()).orElse("n/a"),
                                    Optional.ofNullable(transaction.getTimestamp()),
                                    transaction.getFillTypeOrDefault()
                            ))
                            .collect(toList());

                    var header = String.format(format,
                            "account-id",
                            "transaction-id",
                            "booking-date",
                            "status",
                            "external-id",
                            "timestamp",
                            "filltype"
                    );

                    groupOut.accept(header, tuples);
                });
    }

    private MonthRange getMonthRangeOfPendingTransactionsOfUpstreamAccounts(AccountsAndTransactionsRequestDTO accountsAndTransactionsRequestDTO, UUID userId, List<Account> accountsForUserSite) {
        List<UUID> accountIds = accountsAndTransactionsRequestDTO.getIngestionAccounts()
                .stream()
                .map(it -> AccountMatcher.findExisting(accountsForUserSite, it, false))
                .flatMap(Optional::stream)
                .map(AccountMatchResult::getAccount)
                .map(Account::getId)
                .collect(toList());

        Stream<String> datesPendingTransactions = dataScienceService.getDatesPendingTransactions(userId, accountIds);
        return getWidestRange(datesPendingTransactions);
    }

    private MonthRange determineDateRangeOfAllTransactions(AccountsAndTransactionsRequestDTO accountsAndTransactionsRequestDTO, MonthRange widestRangePendingTransactionsToBeDeleted) {
        Stream<String> incomingTransactionStreamDates = accountsAndTransactionsRequestDTO.getIngestionAccounts().stream()
                .flatMap(it -> it.getTransactions().stream())
                .map(ProviderTransactionDTO::getDateTime)
                .map(MONTH_TRUNCATING_DATE_FORMATTER::format);

        Stream<String> datesOfChangedTransactions = Stream.concat(
                Stream.of(widestRangePendingTransactionsToBeDeleted.getMin(), widestRangePendingTransactionsToBeDeleted.getMax()),
                incomingTransactionStreamDates
        );

        return getWidestRange(datesOfChangedTransactions);
    }

    private MonthRange getWidestRange(Stream<String> dates) {
        MonthRange monthRange = new MonthRange(null, null);
        // Widen the range so every date falls into this range.
        dates.forEach(date -> {
            if (monthRange.getMin() == null || monthRange.getMin().compareTo(date) > 0) {
                monthRange.setMin(date);
            }
            if (monthRange.getMax() == null || monthRange.getMax().compareTo(date) < 0) {
                monthRange.setMax(date);
            }
        });
        return monthRange;
    }

    private void publishIngestionFinishedEvent(
            final @NonNull ClientUserToken clientUserToken,
            final MonthRange monthRange,
            final UUID activityId,
            final UUID userSiteId,
            final @NonNull ZonedDateTime currentTime,
            final Map<UUID, AccountFromProviders> accountIdToAccountFromProviders,
            final Map<UUID, LocalDate> accountIdToOldestTransactionChangeDate) {

        Map<UUID, IngestionFinishedEvent.AccountInformationDTO> accountIdToLastTransactionId = new HashMap<>();
        accountIdToAccountFromProviders.forEach((id, acc) -> {
            if (acc.getTransactions().isEmpty()) {
                return;
            }
            accountIdToLastTransactionId.put(id, new IngestionFinishedEvent.AccountInformationDTO(
                    acc.getAccountId(),
                    acc.getProvider(),
                    acc.getTransactions().get(acc.getTransactions().size() - 1).getExternalId()
            ));
        });

        IngestionFinishedEvent ingestionFinishedEvent = new IngestionFinishedEvent(
                clientUserToken.getUserIdClaim(),
                activityId,
                currentTime,
                userSiteId,
                monthRange.getMin(),
                monthRange.getMax(),
                accountIdToLastTransactionId,
                accountIdToOldestTransactionChangeDate
        );

        accountsAndTransactionsFinishedActivityEventProducer.sendMessage(ingestionFinishedEvent, clientUserToken);
    }

    /**
     * Mutable object that helps reducing a stream of dates to the largest period that covers all those dates.
     */
    @Data
    @AllArgsConstructor
    public static class MonthRange {
        private String min;
        private String max;
    }

}
