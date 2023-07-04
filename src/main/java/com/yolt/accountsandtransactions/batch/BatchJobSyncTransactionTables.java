package com.yolt.accountsandtransactions.batch;

import com.datastax.driver.core.PagingState;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.querybuilder.Select;
import com.yolt.accountsandtransactions.accounts.Account;
import com.yolt.accountsandtransactions.accounts.AccountRepository;
import com.yolt.accountsandtransactions.datascience.DsTransaction;
import com.yolt.accountsandtransactions.datascience.DsTransactionsRepository;
import com.yolt.accountsandtransactions.datascience.PendingType;
import com.yolt.accountsandtransactions.transactions.Transaction;
import com.yolt.accountsandtransactions.transactions.TransactionRepository;
import com.yolt.accountsandtransactions.transactions.TransactionService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import nl.ing.lovebird.cassandrabatch.pager.EntitiesPage;
import nl.ing.lovebird.cassandrabatch.pager.SelectAllEntityPager;
import nl.ing.lovebird.cassandrabatch.throttler.CassandraBatchThrottler;
import nl.ing.lovebird.cassandrabatch.throttler.ThrottledTaskResult;
import nl.ing.lovebird.extendeddata.transaction.TransactionStatus;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.stream.Collectors;

/**
 * This job:
 * 1) creates a list of all user_ids by scanning the transactions table in the datascience keyspace
 * 2) for every user, retrieves all transactions from the datascience keyspace and the A&T keyspace
 * 3a) deletes pending transactions from the AT keyspace that aren't present in the DS keyspace, see:
 * {@link #deleteUnmatchedPendingTransactionsFromKeyspaceAT}
 * 3b) NOT IMPLEMENTED YET: copy booked transactions not present in AT keyspace over from the DS keyspace
 * <p>
 * This job was introduced in nov 2020 to clean up our transactions table.  Our transactions
 * table (accounts_and_transactions.transactions) contains lingering pending transactions that
 * we need to delete.  This job is a first step: it will report the differences, if everything
 * looks reasonable we can act upon it by removing the transactions.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class BatchJobSyncTransactionTables {

    private final AccountRepository atAccRepo;
    private final DsTransactionsRepository dsTransactionsRepository;
    private final TransactionRepository atTransactionsRepository;
    private final TransactionService atTrxService;
    private final BatchSyncProgressStateRepository batchSyncProgressStateRepository;

    @Async("BatchJobSyncTransactionTablesRunner")
    public void run(boolean dryrun, int maxUsers) {
        doRun(dryrun, maxUsers);
    }

    public void doRun(boolean dryrun, int maxUsers) {
        log.info("batchJobSyncTransactionTables started (dryrun={})", dryrun);

        // Keep some global counters.
        Counters counters = new Counters();

        try {
            final Set<UUID> userIds = listUsers();
            log.info("batchJobSyncTransactionTables total_users={}", userIds.size());

            // We have made a list of all users based on the datascience transactions table.
            // Go over every user to compare the two sets of transactions (ds, at).
            for (UUID userId : userIds) {
                Counters userCounters = new Counters();

                if (!dryrun) {
                    if (batchSyncProgressStateRepository.get(userId)
                            .map(BatchSyncProgressState::isPendingTransactionsRemovedFromATKeyspace)
                            .orElse(false)) {
                        counters.increment("skipped_users");
                        continue;
                    }
                }

                // Retrieve all transactions for this user from both keyspaces.
                List<DsTransaction> dsTrx = listAllTransactionsForUserInKeyspaceDS(userId);
                List<Transaction> atTrx = listAllTransactionsForUserInKeyspaceAT(userId);

                // Keep track of the largest amount of trx seen across all users (no functional reason, just because it's interesting).
                if (dsTrx.size() > counters.get("dstrx_high_water_mark")) {
                    counters.set("dstrx_high_water_mark", dsTrx.size());
                }
                if (atTrx.size() > counters.get("attrx_high_water_mark")) {
                    counters.set("attrx_high_water_mark", atTrx.size());
                }

                // Group them by account.
                Map<UUID, List<DsTransaction>> dsTrxByAccount = dsTrx.stream().collect(Collectors.groupingBy(DsTransaction::getAccountId));
                Map<UUID, List<Transaction>> atTrxByAccount = atTrx.stream().collect(Collectors.groupingBy(Transaction::getAccountId));

                // accountsCtr[0] is a counter that keeps track of the accounts that we have seen.
                var accountsCtr = new int[]{1};

                // The datascience accounts are the source of truth.  Use these as 'base' and find out if there are
                // mismatches between the two tables.
                dsTrxByAccount.forEach((accountId, dsTransactions) -> {
                    // Look up the corresponding transactions for this account in the AT keyspace.
                    List<Transaction> atTransactions = atTrxByAccount.getOrDefault(accountId, Collections.emptyList());

                    // We now have:
                    // dsTransactions = transactions for 1 account in the datascience keyspace
                    // atTransactions = transactions for the same account in the accounts_and_transactions keyspace

                    // Group these by transactions status (PENDING / BOOKED).
                    Map<Integer, List<DsTransaction>> dsTransactionsByStatus = dsTransactions.stream().collect(Collectors.groupingBy(DsTransaction::getPending));
                    Map<TransactionStatus, List<Transaction>> atTransactionsByStatus = atTransactions.stream().collect(Collectors.groupingBy(Transaction::getStatus));

                    // Count them.
                    var dsPending = dsTransactionsByStatus.getOrDefault(PendingType.PENDING, Collections.emptyList());
                    var atPending = atTransactionsByStatus.getOrDefault(TransactionStatus.PENDING, Collections.emptyList());
                    var dsBooked = dsTransactionsByStatus.getOrDefault(PendingType.REGULAR, Collections.emptyList());
                    var atBooked = atTransactionsByStatus.getOrDefault(TransactionStatus.BOOKED, Collections.emptyList());

                    // Make a note of any difference, and if so: how much.
                    if (dsPending.size() != atPending.size()) {
                        // Report what the difference was (how many pending transactions).  This number is positive
                        // if there are more pending transactions in the A&T keyspace, and negative if there are more
                        // pending transactions in the ds keyspace.
                        userCounters.increment("pending_mismatch_acc" + accountsCtr[0], atPending.size() - dsPending.size());
                        // Increment a global counter that contains the number of accounts we found where there were
                        // differences in the number of pending transactions between keyspaces.
                        counters.increment("accs_with_pending_mismatch", 1);

                        // If there are more pending transactions in our own keyspace than there are in the ds keyspace
                        // that is a problem we know how to correct.
                        if (atPending.size() > dsPending.size()) {
                            int deleted = deleteUnmatchedPendingTransactionsFromKeyspaceAT(dryrun, dsPending, atPending);
                            counters.increment(dryrun ? "would_delete_pending" : "did_delete_pending", deleted);
                        }
                    }
                    if (dsBooked.size() != atBooked.size()) {
                        // Report what the difference was (how many booked transactions).  This number is positive
                        // if there are more booked transactions in the A&T keyspace, and negative if there are more
                        // booked transactions in the ds keyspace.
                        userCounters.increment("booked_mismatch_acc" + accountsCtr[0], atBooked.size() - dsBooked.size());
                        // Increment a global counter that contains the number of accounts we found where there were
                        // differences in the number of booked transactions between keyspaces.
                        counters.increment("accs_with_booked_mismatch", 1);
                    }

                    if (dsPending.size() == atPending.size() && dsBooked.size() == atBooked.size()) {
                        // If everything is equal we keep track of that too.
                        counters.increment("accs_perfect", 1);
                    }

                    accountsCtr[0]++;

                    // Sleep for a little while as a 'throttle'.  This puts an upperbound of 10 users / sec on the
                    // above process.  In practice it will be less because the code takes time to execute of course.
                    try {
                        Thread.sleep(100);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                });

                if (!dryrun) {
                    counters.increment("processed_users");
                    batchSyncProgressStateRepository.upsert(BatchSyncProgressState.builder()
                            .userId(userId)
                            .pendingTransactionsRemovedFromATKeyspace(true)
                            .build()
                    );
                    if (maxUsers != -1 && counters.get("processed_users") >= maxUsers) {
                        // Job can be configured to process at most n users per batch run.
                        break;
                    }
                }

                // Everything is OK for this user.  Continue with the next user.
                if (userCounters.isEmpty()) {
                    continue;
                }

                // Only log information if something is wrong with a user.
                log.info("batchJobSyncTransactionTables userId={} counters={}",
                        userId,
                        userCounters.toString()
                ); //NOSHERIFF
            } // for each user

        } catch (Exception e) {
            log.info("batchJobSyncTransactionTables threw exception", e); //NOSHERIFF
        } finally {
            log.info("batchJobSyncTransactionTables finished, counters={}", counters); //NOSHERIFF
        }
    }

    private List<DsTransaction> listAllTransactionsForUserInKeyspaceDS(UUID userId) {
        List<DsTransaction> trxs = new ArrayList<>();
        final CassandraBatchThrottler cassaThrottler = new CassandraBatchThrottler(5_000);
        cassaThrottler.startBatch((pagingState, pageSize) -> {
            Select select = dsTransactionsRepository.getTransactionForUserQuery(userId);
            select.setFetchSize(pageSize);
            select.setPagingState(pagingState);
            final ResultSet resultSet = dsTransactionsRepository.getMapper().getManager().getSession().execute(select);
            final PagingState newPagingState = resultSet.getExecutionInfo().getPagingState();
            List<DsTransaction> result = dsTransactionsRepository.getMapper().map(resultSet).all();
            trxs.addAll(result);
            return new ThrottledTaskResult(newPagingState, result.size(), 0);
        });
        return trxs;
    }

    private List<Transaction> listAllTransactionsForUserInKeyspaceAT(UUID userId) {
        List<Transaction> trxs = new ArrayList<>();
        final CassandraBatchThrottler cassaThrottler = new CassandraBatchThrottler(5_000);
        cassaThrottler.startBatch((pagingState, pageSize) -> {
            Select select = atTransactionsRepository.getTransactionsForUserQuery(userId);
            select.setFetchSize(pageSize);
            select.setPagingState(pagingState);
            final ResultSet resultSet = atTransactionsRepository.getMapper().getManager().getSession().execute(select);
            final PagingState newPagingState = resultSet.getExecutionInfo().getPagingState();
            List<Transaction> result = atTransactionsRepository.getMapper().map(resultSet).all();
            trxs.addAll(result);
            return new ThrottledTaskResult(newPagingState, result.size(), 0);
        });
        return trxs;
    }

    private int deleteUnmatchedPendingTransactionsFromKeyspaceAT(
            boolean dryrun,
            List<DsTransaction> dsPending,
            List<Transaction> atPending
    ) {
        // dsIdentifiers is the list of transactionIds that we should *KEEP*.
        var dsIdentifiers = dsPending.stream().map(DsTransaction::getTransactionId).collect(Collectors.toSet());
        // Compute the set of identifiers for transactions that we should *DELETE*.
        var identifiersToRemove = atPending.stream().map(Transaction::getId).collect(Collectors.toSet());
        identifiersToRemove.removeAll(dsIdentifiers);

        var transactionsToRemove = atPending.stream()
                .filter(t -> identifiersToRemove.contains(t.getId()))
                .filter(t -> t.getStatus() == TransactionStatus.PENDING) // To be 100% sure.
                .map(t -> new TransactionService.TransactionPrimaryKey(t.getUserId(), t.getAccountId(), t.getDate(), t.getId(), t.getStatus()))
                .collect(Collectors.toList());

        // Double check that we're on the right path.
        if (transactionsToRemove.size() != atPending.size() - dsPending.size()) {
            log.warn("batchJobSyncTransactionTables: want to remove {} pending transactions, expecting only {}.  Not doing anything.", identifiersToRemove.size(), atPending.size() - dsPending.size());
        } else {
            if (!dryrun) {
                // Issue the delete.
                atTrxService.deleteSpecificTransactions(transactionsToRemove);
            }
            // Keep track of the total number of removed transactions for posterity.
            return transactionsToRemove.size();
        }
        return 0;
    }

    /**
     * List all users by scanning the transactions table.
     */
    private Set<UUID> listUsers() {
        Set<UUID> userIds = new HashSet<>();
        final CassandraBatchThrottler cassaThrottler = new CassandraBatchThrottler(5_000);
        cassaThrottler.startBatch((pagingState, pageSize) -> {
            SelectAllEntityPager<Account> pager = new SelectAllEntityPager<>(
                    atAccRepo.getMapper().getManager().getSession(),
                    atAccRepo.getMapper(),
                    pageSize
            );
            EntitiesPage<Account> accountPage = pager.getNextPage("accounts", pagingState);
            accountPage.getEntities().forEach(acc -> userIds.add(acc.getUserId()));
            return new ThrottledTaskResult(accountPage.getPagingState(), accountPage.getEntities().size(), 0);
        });
        return userIds;
    }

}
