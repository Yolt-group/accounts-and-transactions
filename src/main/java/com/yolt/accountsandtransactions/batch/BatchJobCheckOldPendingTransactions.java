package com.yolt.accountsandtransactions.batch;

import com.yolt.accountsandtransactions.accounts.Account;
import com.yolt.accountsandtransactions.accounts.AccountRepository;
import com.yolt.accountsandtransactions.transactions.Transaction;
import com.yolt.accountsandtransactions.transactions.TransactionRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import nl.ing.lovebird.cassandrabatch.pager.EntitiesPage;
import nl.ing.lovebird.cassandrabatch.pager.SelectAllEntityPager;
import nl.ing.lovebird.cassandrabatch.throttler.CassandraBatchThrottler;
import nl.ing.lovebird.cassandrabatch.throttler.ThrottledTaskResult;
import nl.ing.lovebird.extendeddata.transaction.TransactionStatus;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

import java.time.*;
import java.time.temporal.ChronoUnit;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * Check for old pending transactions.
 * <p>
 * This job scans our transactions table and logs a message about all transactions that:
 * - are present in this table: accounts_and_transactions.transactions
 * - have status PENDING
 * - are older than 40 days [1]
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class BatchJobCheckOldPendingTransactions {

    private final TransactionRepository transactionRepository;
    private final AccountRepository accountRepository;
    private final Clock clock;

    @Async("batchJobCheckOldPendingTransactionsRunner")
    public void run() {
        var startTime = Instant.now(clock);
        var filterPendingTransactionsOlderThan = LocalDate.ofInstant(startTime.minus(40, ChronoUnit.DAYS), ZoneOffset.UTC);
        log.info("batchJobCheckOldPendingTransactions started");
        final CassandraBatchThrottler cassaThrottler = new CassandraBatchThrottler(2_500);

        var counters = new Counters();
        try {
            cassaThrottler.startBatch((pagingState, pageSize) -> {
                SelectAllEntityPager<Transaction> pager = new SelectAllEntityPager<>(
                        transactionRepository.getMapper().getManager().getSession(),
                        transactionRepository.getMapper(),
                        pageSize
                );
                EntitiesPage<Transaction> transactionPage = pager.getNextPage("transactions", pagingState);

                // Filter the transactions and keep only the ones that have status PENDING and that are older than 40 days.
                var oldPendingTrxs = transactionPage.getEntities().stream()
                        .filter(t -> t.getStatus() == TransactionStatus.PENDING)
                        .filter(t -> t.getDate().isBefore(filterPendingTransactionsOlderThan))
                        .collect(Collectors.toList());

                for (Transaction t : oldPendingTrxs) {
                    // Will give us a count of transactions per month, per status.  We expect not to find PENDING
                    // transactions older than 40 days.
                    counters.increment(t.getDate().getYear() * 1_00_00 + t.getDate().getMonthValue() * 1_00 + t.getDate().getDayOfMonth() + "");

                    UUID siteId = accountRepository.getAccounts(t.getUserId()).stream()
                            .filter(a -> a.getId().equals(t.getAccountId()))
                            .map(Account::getSiteId)
                            .findFirst()
                            .orElse(new UUID(0, 0));

                    counters.increment("old_pending_trxs_total");
                    counters.increment("old_pending_trx_site_" + siteId);
                }

                return new ThrottledTaskResult(transactionPage.getPagingState(), transactionPage.getEntities().size(), 0);
            });
        } catch (RuntimeException e) {
            log.error("batchJobCheckOldPendingTransactions threw exception. counters={}", counters.toString(), e); //NOSHERIFF
            return;
        }

        final Duration jobDuration = Duration.between(startTime, Instant.now(clock));
        log.info("batchJobCheckOldPendingTransactions finished. duration={}, counters={}",
                jobDuration,
                counters.toString()
        ); //NOSHERIFF
    }

}
