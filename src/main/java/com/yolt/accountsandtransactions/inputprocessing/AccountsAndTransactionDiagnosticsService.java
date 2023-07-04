package com.yolt.accountsandtransactions.inputprocessing;

import com.google.common.annotations.VisibleForTesting;
import com.yolt.accountsandtransactions.accounts.Account;
import com.yolt.accountsandtransactions.datascience.DataScienceService;
import com.yolt.accountsandtransactions.datascience.DsTransaction;
import com.yolt.accountsandtransactions.inputprocessing.TransactionInsertionStrategy.Mode;
import com.yolt.accountsandtransactions.legacyaccounts.AccountMatcher;
import com.yolt.accountsandtransactions.metrics.AccountsAndTransactionMetrics;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import net.logstash.logback.marker.Markers;
import nl.ing.lovebird.extendeddata.account.BalanceDTO;
import nl.ing.lovebird.extendeddata.account.BalanceType;
import nl.ing.lovebird.extendeddata.account.ExtendedAccountDTO;
import nl.ing.lovebird.extendeddata.transaction.TransactionStatus;
import nl.ing.lovebird.logging.LogTypeMarker;
import nl.ing.lovebird.providerdomain.ProviderTransactionDTO;
import org.springframework.stereotype.Component;

import javax.validation.constraints.NotNull;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.*;
import java.time.chrono.ChronoZonedDateTime;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static com.yolt.accountsandtransactions.metrics.AccountsAndTransactionMetrics.BalanceAccuracy.*;
import static java.math.BigDecimal.ONE;
import static java.math.BigDecimal.ZERO;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptySet;
import static java.util.Comparator.comparing;
import static java.util.Optional.ofNullable;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;
import static nl.ing.lovebird.providerdomain.ProviderTransactionType.CREDIT;
import static org.springframework.util.StringUtils.isEmpty;

/**
 * Contains code that creates metrics and log statements so we can track data quality (diagnostic purposes).
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class AccountsAndTransactionDiagnosticsService {

    private final DataScienceService dataScienceService;
    private final AccountsAndTransactionMetrics metrics;
    private final Clock clock;

    @VisibleForTesting
    public void logReconciliationFailureEvent(final @NonNull Mode mode,
                                              final @NonNull String provider,
                                              final @NotNull UUID accountId,
                                              final @NonNull Exception e) {

        var marker = LogTypeMarker.getDataErrorMarker();
        marker.add(Markers.append("sub-system", "reconciliation"));
        marker.add(Markers.append("mode", mode.name()));
        marker.add(Markers.append("provider", provider));
        marker.add(Markers.append("account-id", accountId.toString()));

        log.error(marker, e.getMessage(), e);

        try {
            metrics.incrementReconciliationFailure(provider);
        } catch (Exception ex) {
            log.error("Failure while producing metric.", ex);
        }
    }

    void updatePerAccountStatistics(TransactionInsertionStrategy.Instruction instruction, AccountFromProviders upstreamAccount, Account account, boolean isUpdate) {
        try {
            ofNullable(instruction.getMetrics()).ifPresent(metrics::updateTransactionReconciliationMatchingStatistics);

            updateBalanceAvailabilityStatistics(upstreamAccount);
            updateBalanceAccuracyStatistics(isUpdate, account, upstreamAccount, instruction);

            updateFutureTransactionsStatistics(upstreamAccount.getProvider(), instruction.getTransactionsToInsert(), true);
            updateFutureTransactionsStatistics(upstreamAccount.getProvider(), instruction.getTransactionsToUpdate(), false);

            updatePendingTransactionStatistics(upstreamAccount.getProvider(), instruction);
        } catch (Throwable t) {
            log.warn("Unexpected exception in statistics", t);
        }
    }

    void updateAccountMatchingStatistics(AccountFromProviders upstreamAccount, Optional<AccountMatcher.AccountMatchResult> existingAccountOpt) {
        existingAccountOpt.ifPresentOrElse(
                it -> metrics.measureAccountMatchingStatistics(upstreamAccount.getProvider(), it.getAccountMatchType(), it.isAccountNumberIsNormalized()),
                () -> metrics.measureAccountMatchingStatistics(upstreamAccount.getProvider(), AccountMatcher.AccountMatchType.NO_MATCH, false)
        );
    }

    /**
     * YCO-1386: Add check to find providers that either
     * - set both the structured and unstructured remittance information
     * - set neither the structured nor the unstructured remittance information
     * <p>
     * Once those providers have been identified they will be fixed in the providers domain.
     * The validation below should then be implemented in {@link ProviderTransactionDTO#validate()}
     * and throw an appropriate exception and should be removed here.
     */
    @VisibleForTesting
    void recordInvalidRemittanceInformation(List<ProviderTransactionWithId> transactionWithIds, String provider) {
        transactionWithIds.stream()
                .map(ProviderTransactionWithId::getProviderTransactionDTO)
                .map(ProviderTransactionDTO::getExtendedTransaction)
                .filter(Objects::nonNull)
                .forEach(trx -> {
                    var remittanceInformationStructured = trx.getRemittanceInformationStructured();
                    var remittanceInformationUnstructured = trx.getRemittanceInformationUnstructured();

                    if (remittanceInformationStructured != null && remittanceInformationUnstructured != null) {
                        if (isEmpty(remittanceInformationStructured) && isEmpty(remittanceInformationUnstructured)) {
                            metrics.incrementTransactionWithoutStructuredAndUnstructuredRemittanceInformation(provider);
                        } else {
                            metrics.incrementTransactionWithStructuredAndUnstructuredRemittanceInformation(provider);
                        }
                    }
                });
    }

    void updateFutureTransactionsStatistics(String provider, List<ProviderTransactionWithId> transactions, boolean newTransactions) {
        var now = ZonedDateTime.now(clock);
        transactions.stream()
                .map(ProviderTransactionWithId::getProviderTransactionDTO)
                .filter(trx -> trx.getDateTime().isAfter(now))
                .forEach(trx -> metrics.incrementTransactionWithFutureDate(provider, newTransactions, trx.getStatus()));
    }

    void updateTransactionProcessingDurationMetrics(DurationLogger perAccountIngestionDurationLogger) {
        metrics.updateTransactionProcessingDurationMetrics(perAccountIngestionDurationLogger);
    }

    /**
     * Logs a report that can be used for analysis of issues and specifically for analysis of fields relevant to CAM
     * alerting (tx.accountNr, tx.counterparty, a.holderName).
     */
    void logAccountIngestionReport(UUID userId, UUID yoltAccountId, AccountFromProviders upstreamAccount,
                                   TransactionInsertionStrategy.Instruction executedInstruction) {

        try {
            var sorted = upstreamAccount.getTransactions().stream()
                    .sorted(comparing(ProviderTransactionDTO::getDateTime))
                    .collect(toList());

            var oldestTransactionInUpstream = sorted.stream()
                    .map(ProviderTransactionDTO::getDateTime)
                    .min(ChronoZonedDateTime::compareTo);

            var numTransactionsOlderThen40DayWindow = sorted.stream()
                    .map(ProviderTransactionDTO::getDateTime)
                    .map(date -> LocalDate.ofInstant(date.toInstant(), ZoneOffset.UTC))
                    .filter(date -> date.isBefore(LocalDate.ofInstant(Instant.now(clock), ZoneOffset.UTC).minusDays(40)))
                    .count();

            List<DsTransaction> newTransactions = dataScienceService.toDsTransactionList(
                    yoltAccountId, userId, upstreamAccount.getCurrency(), executedInstruction.getTransactionsToInsert());

            long newTransactionHavingAccountNumber = newTransactions.stream()
                    .filter(tx -> tx.getBankCounterpartyIban() != null ||
                            tx.getBankCounterpartyBban() != null ||
                            tx.getBankCounterpartyMaskedPan() != null ||
                            tx.getBankCounterpartyPan() != null ||
                            tx.getBankCounterpartySortCodeAccountNumber() != null)
                    .count();

            BigDecimal mainAccountBalance = upstreamAccount.getCurrentBalance() != null ? upstreamAccount.getCurrentBalance() : upstreamAccount.getAvailableBalance();

            List<BalanceDTO> balancesFromExtendedAccounts = getBalancesFromExtendedAccounts(upstreamAccount.getExtendedAccount());

            BalanceType type = balancesFromExtendedAccounts.stream()
                    .filter(b -> b.getBalanceAmount() != null)
                    .filter(b -> b.getBalanceAmount().getAmount() != null)
                    .filter(b -> b.getBalanceAmount().getAmount().equals(mainAccountBalance))
                    .findAny()
                    .map(BalanceDTO::getBalanceType)
                    .orElse(null);

            final String balanceTypes = balancesFromExtendedAccounts.stream()
                    .map(BalanceDTO::getBalanceType)
                    .filter(Objects::nonNull)
                    .map(Enum::toString)
                    .collect(joining(","));

            long newTransactionHavingCounterparty = newTransactions.stream()
                    .filter(tx -> tx.getBankCounterpartyName() != null)
                    .count();

            var totalNewTransactions = newTransactions.size();
            var totalDeletedTransactions = executedInstruction.getTransactionsToDelete().size();
            var totalUpdatedTransactions = executedInstruction.getTransactionsToUpdate().size();
            var totalUnchangedTransactions = executedInstruction.getTransactionsToIgnore().size();

            boolean hasAccountNumber = upstreamAccount.getAccountNumber() != null && upstreamAccount.getAccountNumber().getIdentification() != null;
            boolean hasAccountHolder = upstreamAccount.getAccountNumber() != null && upstreamAccount.getAccountNumber().getHolderName() != null;

            String newTransactionsCamQuality;

            if (totalNewTransactions == 0) {
                newTransactionsCamQuality = "n/a";
            } else {
                BigDecimal quality = new BigDecimal((double) Math.min(newTransactionHavingAccountNumber, newTransactionHavingCounterparty) / totalNewTransactions)
                        .setScale(2, RoundingMode.HALF_UP);
                newTransactionsCamQuality = quality.toPlainString();
            }

            log.info("Account ingestion report : " +
                            "account={}, " +
                            "provider={}, " +
                            "balance_types={}, " +
                            "main_balance_type={}, " +
                            "has_account_number={}, " +
                            "has_account_holder_name={}, " +
                            "tx_total={}, " +
                            "tx_total_oldest={}, " +
                            "tx_total_newest={}, " +
                            "tx_new={}, " +
                            "tx_deleted={}, " +
                            "tx_updated={}, " +
                            "tx_unchanged={}, " +
                            "tx_new_having_cp_account_nr={}, " +
                            "tx_new_having_cp_name={}, " +
                            "tx_oldest_in_upstream={}, " +
                            "tx_total_older_40_day_window_in_upstream={}, " +
                            "acct_cam_quality={}, " +
                            "new_tx_cam_quality={}",
                    yoltAccountId,
                    upstreamAccount.getProvider(),
                    balanceTypes,
                    type,
                    hasAccountNumber,
                    hasAccountHolder,
                    upstreamAccount.getTransactions().size(),
                    !sorted.isEmpty() ? sorted.get(0).getDateTime() : null,
                    !sorted.isEmpty() ? sorted.get(sorted.size() - 1).getDateTime() : null,
                    totalNewTransactions,
                    totalDeletedTransactions,
                    totalUpdatedTransactions,
                    totalUnchangedTransactions,
                    newTransactionHavingAccountNumber,
                    newTransactionHavingCounterparty,
                    oldestTransactionInUpstream.orElse(null),
                    numTransactionsOlderThen40DayWindow,
                    hasAccountNumber && hasAccountHolder ? "ok" : "nok",
                    newTransactionsCamQuality
            );
        } catch (Exception e) {
            log.error("Exception during account ingestion logging", e);
        }
    }

    /**
     * Per provider we keep track of how far in the past (in days) pending transactions are being sent to us.
     * <p>
     * For every pending transaction we compute how many days ago it took place and increment a metric accordingly.
     */
    private void updatePendingTransactionStatistics(String provider, TransactionInsertionStrategy.Instruction instruction) {
        // Seconds since the unix epoch.
        long now = Instant.now(clock).getEpochSecond();
        Stream.concat(instruction.getTransactionsToInsert().stream(), instruction.getTransactionsToUpdate().stream())
                // Only interested in pending transactions.
                .filter(t -> t.getProviderTransactionDTO().getStatus() == TransactionStatus.PENDING)
                // Grab the date+time the trx took place.
                .map(t -> t.getProviderTransactionDTO().getDateTime())
                // Convert to how many days ago the trx happened.
                .map(zdt -> TimeUnit.SECONDS.toDays(now - zdt.toEpochSecond()))
                // Increment metric.
                .forEach(daysInPast -> metrics.incrementPendingTransactionsIngested(provider, daysInPast));
    }

    /**
     * Keep track of which balances types are available on a per-provider basis.
     */
    private void updateBalanceAvailabilityStatistics(AccountFromProviders acct) {
        if (acct.getExtendedAccount() == null || acct.getExtendedAccount().getBalances() == null) {
            metrics.updateBalanceTypePresenceStatistics(acct.getProvider(), emptySet());
            return;
        }

        Set<BalanceType> balanceTypes = EnumSet.noneOf(BalanceType.class);
        for (BalanceDTO balance : acct.getExtendedAccount().getBalances()) {
            if (balance.getBalanceType() == null) {
                // This should not happen.
                log.warn("Received an invalid BalanceDTO with a balanceType == null for provider {}", acct.getProvider()); //NOSHERIFF
                continue;
            }
            balanceTypes.add(balance.getBalanceType());
        }
        metrics.updateBalanceTypePresenceStatistics(acct.getProvider(), balanceTypes);
    }

    /**
     * This method keeps track of metrics that will tell us if the data quality of a bank is "good".
     * <p>
     * Every invocation of this method always increments the metric for two balances: CURRENT and AVAILABLE.
     * <p>
     * The metric has a tag with 4 different values:
     * <p>
     * If a balance is null either in the stored or incoming account the result is: NOT_PRESENT.
     * If a balance is present but the refresh was not perfect (e.g. we had to delete >= 1 transaction) then we cannot perform the computation to check accuracy and the result is: CANT_RECONCILE_TRXS.
     * If the sum of the amounts of all hitherto unseen transactions and the stored balance is equal to the incoming balance then the balance is ACCURATE.
     * Otherwise the result is INACCURATE.
     * <p>
     * We can then graph --for every balance-- sum(ACCURATE)/sum(total) and we have the data quality.
     */
    private void updateBalanceAccuracyStatistics(boolean isUpdate, Account storedAccount, AccountFromProviders upstreamAccount, TransactionInsertionStrategy.Instruction instruction) {
        // Keep track of statistics with which we can gauge the data quality (does an accounts' previous balance and the new transactions sum to the expected balance?)
        final BigDecimal previousCurrentBalance = isUpdate ? storedAccount.getBalance() : ZERO;
        final BigDecimal expectedCurrentBalance = upstreamAccount.getCurrentBalance();
        boolean currentBalancePresent = previousCurrentBalance != null && expectedCurrentBalance != null;

        final BigDecimal previousAvailableBalance = isUpdate ? storedAccount.getAvailableBalance() : ZERO;
        final BigDecimal expectedAvailableBalance = upstreamAccount.getAvailableBalance();
        boolean availableBalancePresent = previousAvailableBalance != null && expectedAvailableBalance != null;

        AccountsAndTransactionMetrics.BalanceAccuracy currentBalanceAccuracy = currentBalancePresent ? null : NOT_PRESENT;
        AccountsAndTransactionMetrics.BalanceAccuracy availableBalanceAccuracy = availableBalancePresent ? null : NOT_PRESENT;

        // If we had to delete >= 1 transactions we cannot reasonably have a go at checking the balance accuracy because
        // having to delete a transaction means that we couldn't reconcile all incoming transactions with the stored
        // transactions.
        boolean isPerfectRefresh = instruction.getTransactionsToDelete().isEmpty();
        if (isPerfectRefresh) {
            // Sum the amounts of the 'new' (hitherto unseen) transactions.
            final BigDecimal delta = instruction.getTransactionsToInsert().stream()
                    .map(ProviderTransactionWithId::getProviderTransactionDTO)
                    .map(t -> t.getAmount().multiply(t.getType() == CREDIT ? ONE : ONE.negate()))
                    .reduce(ZERO, BigDecimal::add);

            if (currentBalancePresent) {
                currentBalanceAccuracy = previousCurrentBalance.add(delta).compareTo(expectedCurrentBalance) == 0 ? ACCURATE : INACCURATE;
            }
            if (availableBalancePresent) {
                availableBalanceAccuracy = previousAvailableBalance.add(delta).compareTo(expectedAvailableBalance) == 0 ? ACCURATE : INACCURATE;
            }
        } else {
            if (currentBalanceAccuracy == null) {
                currentBalanceAccuracy = CANT_RECONCILE_TRXS;
            }
            if (availableBalanceAccuracy == null) {
                availableBalanceAccuracy = CANT_RECONCILE_TRXS;
            }
        }

        metrics.updateBalanceAccuracyStatistics(upstreamAccount.getProvider(), "current", currentBalanceAccuracy);
        metrics.updateBalanceAccuracyStatistics(upstreamAccount.getProvider(), "available", availableBalanceAccuracy);
    }

    private List<BalanceDTO> getBalancesFromExtendedAccounts(ExtendedAccountDTO extendedAccount) {
        if (extendedAccount == null || extendedAccount.getBalances() == null) {
            return emptyList();
        }
        return extendedAccount.getBalances();
    }

}
