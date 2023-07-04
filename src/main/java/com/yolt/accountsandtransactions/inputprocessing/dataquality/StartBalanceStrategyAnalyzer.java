package com.yolt.accountsandtransactions.inputprocessing.dataquality;

import com.yolt.accountsandtransactions.inputprocessing.AccountFromProviders;
import com.yolt.accountsandtransactions.inputprocessing.ProviderTransactionWithId;
import com.yolt.accountsandtransactions.metrics.AccountsAndTransactionMetrics;
import com.yolt.accountsandtransactions.transactions.Transaction;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import nl.ing.lovebird.extendeddata.account.BalanceDTO;
import nl.ing.lovebird.extendeddata.account.BalanceType;
import nl.ing.lovebird.extendeddata.transaction.TransactionStatus;
import nl.ing.lovebird.providerdomain.ProviderTransactionDTO;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.time.Clock;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.stream.Stream;

import static com.yolt.accountsandtransactions.inputprocessing.dataquality.StartBalanceStrategyAnalyzer.ComputationType.*;
import static java.lang.String.format;
import static java.math.BigDecimal.ZERO;
import static java.time.ZonedDateTime.now;
import static java.time.temporal.ChronoUnit.DAYS;
import static java.util.stream.Collectors.*;
import static nl.ing.lovebird.extendeddata.transaction.TransactionStatus.BOOKED;
import static nl.ing.lovebird.extendeddata.transaction.TransactionStatus.PENDING;

@Component
@AllArgsConstructor
@Slf4j
public class StartBalanceStrategyAnalyzer {
    private final AccountsAndTransactionMetrics metrics;
    private final Clock clock;

    public void analyze(@NonNull AccountFromProviders account,
                        boolean existingAccount,
                        @NonNull List<ProviderTransactionWithId> transactionsToInsert,
                        @NonNull List<ProviderTransactionWithId> transactionsToUpdate,
                        @NonNull List<Transaction> transactionsToDelete) {

        if (canBeSkipped(account, existingAccount, transactionsToInsert, transactionsToUpdate, transactionsToDelete)) {
            log.debug("This account ({}) can be skipped for analysis.", account.getAccountId());
            return;
        }

        var transactions = transactionsToInsert.stream()
                .map(ProviderTransactionWithId::getProviderTransactionDTO)
                .collect(toList());
        var balances = account.getExtendedAccount().getBalances();

        var bookedAndPendingResults = checkBalances(balances, transactions, USE_BOOKED_AND_PENDING);
        var bookedAndPendingBeforeTodayResults = checkBalances(balances, transactions, USE_BOOKED_AND_PENDING_BEFORE_TODAY);
        var bookedResults = checkBalances(balances, transactions, USE_BOOKED);
        var bookedBeforeTodayResults = checkBalances(balances, transactions, USE_BOOKED_BEFORE_TODAY);

        var overallResults = Stream.of(bookedBeforeTodayResults, bookedResults, bookedAndPendingBeforeTodayResults, bookedAndPendingResults)
                .reduce(Stream.empty(), Stream::concat)
                .collect(toList());

        overallResults.forEach(result -> metrics.incrementComputeZeroBalanceResult(account.getProvider(), result.balanceType, result.reachedZero, result.computationType.toString()));

        summarizeAndLog(overallResults, account.getProvider());
    }

    private Stream<StartBalanceComputationResult> checkBalances(List<BalanceDTO> balances, List<ProviderTransactionDTO> transactions, ComputationType computationType) {
        var sumOfTransactions = getSumsForSelection(transactions, computationType.getApplicableStatusses(), computationType.getThresholdDate(clock));
        return balances.stream()
                .map(balance -> StartBalanceComputationResult.builder()
                        .reachedZero(balance.getBalanceAmount().getAmount().compareTo(sumOfTransactions) == 0)
                        .balanceType(balance.getBalanceType())
                        .computationType(computationType)
                        .build());
    }

    private BigDecimal getSumsForSelection(List<ProviderTransactionDTO> transactions, List<TransactionStatus> statuses, ZonedDateTime before) {
        return transactions.stream()
                .filter(tx -> statuses.contains(tx.getStatus()))
                .filter(tx -> !tx.getDateTime().isAfter(before))
                .map(ProviderTransactionDTO::getAmount)
                .reduce(ZERO, BigDecimal::add);
    }

    private void summarizeAndLog(List<StartBalanceComputationResult> computationResults, String provider) {
        var summary = computationResults.stream()
                .collect(groupingBy(StartBalanceComputationResult::getBalanceType,
                        partitioningBy(StartBalanceComputationResult::isReachedZero,
                                mapping(StartBalanceComputationResult::getComputationType, toList()))));

        var line = summary.entrySet().stream()
                .map(it -> format("pr: %s bt: %s %s", provider, it.getKey().getName(), it.getValue().entrySet().stream()
                        .map(it2 -> format("%s -> %s", it2.getKey(), it2.getValue()))
                        .collect(joining("\n\t", "\n\t", ""))))
                .collect(joining("\n", "StartBalanceAnalysis:\n", ""));

        log.info(line);
    }

    private boolean canBeSkipped(AccountFromProviders account,
                                 boolean existingAccount,
                                 List<ProviderTransactionWithId> transactionsToInsert,
                                 List<ProviderTransactionWithId> transactionsToUpdate,
                                 List<Transaction> transactionsToDelete) {
        if (account.getExtendedAccount() == null || account.getExtendedAccount().getBalances() == null || account.getExtendedAccount().getBalances().isEmpty()) {
            log.debug("Account without extended information.");
            return true;
        }

        if (existingAccount) {
            log.debug("Existing account.");
            return true;
        }

        if (!transactionsToUpdate.isEmpty() || !transactionsToDelete.isEmpty()) {
            log.debug("Account with updates/deletions.");
            return true;
        }

        var eighteenMonthsAgo = now(clock).minusMonths(18);

        return transactionsToInsert.stream()
                .map(ProviderTransactionWithId::getProviderTransactionDTO)
                .map(ProviderTransactionDTO::getDateTime)
                .anyMatch(transactionDate -> transactionDate.isBefore(eighteenMonthsAgo));
    }

    @Builder
    @Getter
    static class StartBalanceComputationResult {
        private final boolean reachedZero;
        private final BalanceType balanceType;
        private final ComputationType computationType;
    }

    @Getter
    enum ComputationType {
        USE_BOOKED("booked", List.of(BOOKED)),
        USE_BOOKED_BEFORE_TODAY("booked_before_today", List.of(BOOKED)),
        USE_BOOKED_AND_PENDING("booked_and_pending", List.of(BOOKED, PENDING)),
        USE_BOOKED_AND_PENDING_BEFORE_TODAY("booked_and_pending_before_today", List.of(BOOKED, PENDING));

        final List<TransactionStatus> applicableStatusses;
        final String name;

        ComputationType(String name, List<TransactionStatus> applicableStatusses) {
            this.name = name;
            this.applicableStatusses = applicableStatusses;
        }

        ZonedDateTime getThresholdDate(Clock clock) {
            var now = ZonedDateTime.now(clock);
            if (this == USE_BOOKED || this == USE_BOOKED_AND_PENDING) {
                return now;
            } else {
                return now.truncatedTo(DAYS);
            }
        }

        @Override
        public String toString() {
            return name;
        }
    }
}
