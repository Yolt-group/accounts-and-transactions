package com.yolt.accountsandtransactions.offloading;

import com.datastax.driver.mapping.Mapper;
import com.yolt.accountsandtransactions.accounts.Account;
import com.yolt.accountsandtransactions.accounts.AccountRepository;
import com.yolt.accountsandtransactions.inputprocessing.enrichments.UsersClient;
import com.yolt.accountsandtransactions.transactions.Transaction;
import com.yolt.accountsandtransactions.transactions.TransactionRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import nl.ing.lovebird.cassandrabatch.pager.EntitiesPage;
import nl.ing.lovebird.cassandrabatch.pager.SelectEntityPager;
import nl.ing.lovebird.cassandrabatch.throttler.CassandraBatchThrottler;
import nl.ing.lovebird.cassandrabatch.throttler.ThrottledTaskResult;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.concurrent.CompletableFuture.completedFuture;

@RequiredArgsConstructor
@Service
@Slf4j
public class BatchPushOffloadData {

    private final OffloadService offloadService;
    private final AccountRepository accountRepository;
    private final TransactionRepository transactionRepository;
    private final UsersClient usersClient;

    /**
     * A map to cache relevant information for a given user saving us many roundtrips to the users service.
     * <p>
     * userId -> clientId
     */
    private final Map<UUID, UUID> userIdToClientIdMap = new HashMap<>();
    /**
     * If we cannot retrieve the "usercontext" for a user we add the id to this set to prevent
     * hammering the users service.
     */
    private final Set<UUID> usersNotFound = new HashSet<>();

    @Async("BatchPushDataToOffloadTopic")
    public CompletableFuture<Void> offloadAccounts(boolean dryRun, int maxReadPerSecond) {
        log.info("starting batch to sync all accounts to datascience dry-run={} with {} reads per second", dryRun, maxReadPerSecond);

        try {
            Mapper<Account> accountMapper = accountRepository.getMapper();
            final CassandraBatchThrottler cassaThrottler = new CassandraBatchThrottler(maxReadPerSecond);
            cassaThrottler.startBatch((pagingState, pageSize) -> {
                AtomicInteger syncedAccounts = new AtomicInteger();
                SelectEntityPager<Account> pager = SelectEntityPager.selectAll(accountMapper.getManager().getSession(),
                        accountMapper,
                        pageSize,
                        "accounts");
                EntitiesPage<Account> accountPage = pager.getNextPage(pagingState);
                accountPage.getEntities().forEach(acc -> {
                    Optional<UUID> clientId = getClientId(acc.getUserId());
                    if (clientId.isPresent()) {
                        if (!dryRun) {
                            offloadService.offloadInsertOrUpdateAsync(acc, clientId.get());
                        }
                        syncedAccounts.getAndIncrement();
                    }
                });
                return new ThrottledTaskResult(accountPage.getPagingState(), accountPage.getEntities().size(), syncedAccounts.get());
            });

            return completedFuture(null);
        } catch (RuntimeException e) {
            log.error("batch failed with exception", e);
            return completedFuture(null);
        }
    }

    @Async("BatchPushDataToOffloadTopic")
    public CompletableFuture<Void> offloadTransactions(boolean dryRun, int maxReadPerSecond) {
        log.info("starting batch to sync all transactions to datascience dry-run={} with {} reads per second", dryRun, maxReadPerSecond);

        try {
            Mapper<Transaction> transactionMapper = transactionRepository.getMapper();
            final CassandraBatchThrottler cassaThrottler = new CassandraBatchThrottler(maxReadPerSecond);
            cassaThrottler.startBatch((pagingState, pageSize) -> {
                AtomicInteger syncedTransactions = new AtomicInteger();
                SelectEntityPager<Transaction> pager = SelectEntityPager.selectAll(transactionMapper.getManager().getSession(),
                        transactionMapper,
                        pageSize,
                        "transactions");
                EntitiesPage<Transaction> transactionPage = pager.getNextPage(pagingState);
                transactionPage.getEntities().forEach(trx -> {
                    try {
                        if (!dryRun) {
                            offloadService.offloadInsertOrUpdateAsync(trx);
                        }
                        syncedTransactions.getAndIncrement();
                    } catch (RuntimeException e) {
                        log.warn("Unable to sync transaction userId:{}, accountId:{}, date:{}, transactionId:{} due to exception", trx.getUserId(), trx.getAccountId(), trx.getDate(), trx.getId(), e); //NOSHERIFF
                    }
                });
                return new ThrottledTaskResult(transactionPage.getPagingState(), transactionPage.getEntities().size(), syncedTransactions.get());
            });

            return completedFuture(null);
        } catch (RuntimeException e) {
            log.error("batch failed with exception", e);
            return completedFuture(null);
        }
    }

    Optional<UUID> getClientId(UUID userId) {
        if (usersNotFound.contains(userId)) {
            return Optional.empty();
        }
        if (!userIdToClientIdMap.containsKey(userId)) {
            var optionalCtx = usersClient.getUser(userId);
            optionalCtx.ifPresentOrElse(
                    (ctx) -> userIdToClientIdMap.put(userId, ctx.clientId()),
                    () -> {
                        log.warn("Can't retrieve UserContext for userId={}.", userId);
                        usersNotFound.add(userId);
                    }
            );
        }
        return Optional.ofNullable(userIdToClientIdMap.get(userId));
    }
}
