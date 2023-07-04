package com.yolt.accountsandtransactions.batch;

import com.yolt.accountsandtransactions.datascience.DsTransaction;
import com.yolt.accountsandtransactions.datascience.DsTransactionsRepository;
import com.yolt.accountsandtransactions.inputprocessing.enrichments.UsersClient;
import com.yolt.accountsandtransactions.transactions.Transaction;
import com.yolt.accountsandtransactions.transactions.TransactionRepository;
import com.yolt.accountsandtransactions.transactions.TransactionService;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import nl.ing.lovebird.cassandrabatch.pager.EntitiesPage;
import nl.ing.lovebird.cassandrabatch.pager.SelectAllEntityPager;
import nl.ing.lovebird.cassandrabatch.throttler.CassandraBatchThrottler;
import nl.ing.lovebird.cassandrabatch.throttler.ThrottledTaskResult;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

import java.time.LocalDate;
import java.time.ZoneId;
import java.util.*;
import java.util.concurrent.CompletableFuture;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.CompletableFuture.failedFuture;
import static java.util.stream.Collectors.toList;

/**
 * This job deletes all transactions of users owned by clients from France that are older than a year.
 */
@Slf4j
@Service
public class BatchDeleteTransactionsOlderThanOneYearForFrance {

    /**
     * https://apy.ycs.yfb-prd.yolt.io/assistance-portal-yts/react/client-groups/141f08f5-cc7a-483e-beeb-3e28244404b1/clients/a5154eb9-9f47-43b4-81b1-fce67813c002
     */
    static final UUID clientIdFrance = UUID.fromString("a5154eb9-9f47-43b4-81b1-fce67813c002");

    /**
     * A map to cache relevant information for a given user saving us many roundtrips to the users service.
     * <p>
     * userId -> clientId
     */
    private final Map<UUID, UUID> userIdToClientIdMap = new HashMap<>();
    /**
     * If we cannot retrieve the user info for a user we add the id to this set to prevent
     * hammering the users service.
     */
    private final Set<UUID> userContextClientErrors = new HashSet<>();

    /**
     * Name of the environment we're running on (e.g. "yfb-prd")
     */
    private final String environmentName;

    private final UsersClient usersClient;
    private final TransactionRepository atTransactionsRepository;
    private final DsTransactionsRepository dsTransactionsRepository;

    public BatchDeleteTransactionsOlderThanOneYearForFrance(
            @Value("${environment}") String environmentName,
            UsersClient usersClient,
            TransactionRepository atTransactionsRepository,
            DsTransactionsRepository dsTransactionsRepository
    ) {
        this.environmentName = environmentName;
        this.usersClient = usersClient;
        this.atTransactionsRepository = atTransactionsRepository;
        this.dsTransactionsRepository = dsTransactionsRepository;
    }

    @Async("BatchDeleteTransactionsOlderThanOneYear")
    public CompletableFuture<Void> run(boolean dryrun) {
        final UUID clientId = clientIdFrance;
        crashUnlessPreconditionsMet(environmentName, clientId);

        //
        // France has a 'legal requirement' that we don't keep data around that is older than one year.  We have
        // interpreted one year as being equal to 365 days.
        //
        LocalDate referenceDate = LocalDate.now(ZoneId.of("Europe/Paris"))
                .atStartOfDay()
                .minusDays(365)
                .toLocalDate();

        var counters = new Counters();
        try {
            log.info("BatchDeleteTransactionsOlderThanOneYear starting (clientId={}, dryrun={})", clientId, dryrun);

            deleteFromAccountsAndTransactionsKeyspace(clientId, dryrun, counters, referenceDate);
            log.info("1/2 A&T keyspace done");

            deleteFromDatascienceKeyspace(clientId, dryrun, counters, referenceDate);
            log.info("2/2 DS keyspace done");

            if (!userContextClientErrors.isEmpty()) {
                log.warn("Failed to retrieve information about {} user(s).  Consequently, not all data might have been removed.  This is a known problem, see https://yolt.atlassian.net/browse/YCO-1917 for more information.", userContextClientErrors.size());
            }

            // The number of users we failed to look up in the users service.
            counters.set("user_context_errors", userContextClientErrors.size());
            // Clear the errors so we can re-run the job and try to fetch these users again.
            userContextClientErrors.clear();

        } catch (RuntimeException e) {
            log.error("failed with exception, counters so far: {}", counters, e); //NOSHERIFF
            return failedFuture(e);
        }
        log.info("finished, counters={}", counters); //NOSHERIFF
        return completedFuture(null);
    }

    /**
     * Remove old transactions from the A&T keyspace.
     */
    private void deleteFromAccountsAndTransactionsKeyspace(@NonNull UUID clientId, boolean dryrun, Counters counters, LocalDate referenceDate) {
        Set<UUID> accountIds = new HashSet<>();
        Set<UUID> userIds = new HashSet<>();

        final CassandraBatchThrottler cassaThrottler = new CassandraBatchThrottler(5_000);
        cassaThrottler.startBatch((pagingState, pageSize) -> {
            SelectAllEntityPager<Transaction> pager = new SelectAllEntityPager<>(
                    atTransactionsRepository.getMapper().getManager().getSession(),
                    atTransactionsRepository.getMapper(),
                    pageSize
            );
            EntitiesPage<Transaction> transactionPage = pager.getNextPage("transactions", pagingState);

            final List<Transaction> transactionsForClient = transactionPage.getEntities().stream()
                    .filter(t -> clientId.equals(clientId(t.getUserId())))
                    .collect(toList());

            // Total number of transactions in the A&T keyspace for the client (tracking this out of curiousity)
            counters.increment("at_trxs", transactionsForClient.size());
            transactionsForClient.stream().map(Transaction::getAccountId).forEach(accountIds::add);
            transactionsForClient.stream().map(Transaction::getUserId).forEach(userIds::add);

            // Deletion logic
            var transactionsToDelete = transactionsForClient.stream()
                    .filter(t -> isOlderThanReferenceDate(t, referenceDate))
                    .collect(toList());
            // Total number of transactions in A&T that can be deleted (old trxs belonging to the given client).
            counters.increment("at_trxs_can_delete", transactionsToDelete.size());
            counters.increment("at_trxs_did_delete", deleteTransactions(transactionsToDelete, dryrun));

            return new ThrottledTaskResult(transactionPage.getPagingState(), transactionPage.getEntities().size(), 0);
        });
        // Total number of users and accounts in the A&T keyspace for the client (tracking this out of curiousity)
        counters.set("at_users", userIds.size());
        counters.set("at_accounts", accountIds.size());
    }

    /**
     * Remove old transactions from the DS keyspace.
     */
    private void deleteFromDatascienceKeyspace(@NonNull UUID clientId, boolean dryrun, Counters counters, LocalDate referenceDate) {
        Set<UUID> accountIds = new HashSet<>();
        Set<UUID> userIds = new HashSet<>();

        final CassandraBatchThrottler cassaThrottler = new CassandraBatchThrottler(5_000);
        cassaThrottler.startBatch((pagingState, pageSize) -> {
            SelectAllEntityPager<DsTransaction> pager = new SelectAllEntityPager<>(
                    dsTransactionsRepository.getMapper().getManager().getSession(),
                    dsTransactionsRepository.getMapper(),
                    pageSize
            );
            EntitiesPage<DsTransaction> transactionPage = pager.getNextPage("transactions", pagingState);

            final List<DsTransaction> transactionsForClient = transactionPage.getEntities().stream()
                    .filter(t -> clientId.equals(clientId(t.getUserId())))
                    .collect(toList());

            // Total number of transactions in the DS keyspace (tracking this out of curiousity)
            counters.increment("ds_trxs", transactionsForClient.size());
            transactionsForClient.stream().map(DsTransaction::getAccountId).forEach(accountIds::add);
            transactionsForClient.stream().map(DsTransaction::getUserId).forEach(userIds::add);

            // Deletion logic
            var transactionsToDelete = transactionsForClient.stream()
                    .filter(t -> isOlderThanReferenceDate(t, referenceDate))
                    .collect(toList());
            // Total number of transactions in DS that can be deleted (old trxs belonging to the given client).
            counters.increment("ds_trxs_can_delete", transactionsToDelete.size());
            counters.increment("ds_trxs_did_delete", deleteDsTransactions(transactionsToDelete, dryrun));


            return new ThrottledTaskResult(transactionPage.getPagingState(), transactionPage.getEntities().size(), 0);
        });
        // Total number of users and accounts in the DS keyspace (tracking this out of curiousity)
        counters.set("ds_users", userIds.size());
        counters.set("ds_accounts", accountIds.size());
    }


    /**
     * @return the number of transactions that were deleted
     */
    private int deleteTransactions(List<Transaction> transactions, boolean dryrun) {
        if (dryrun || transactions.isEmpty()) {
            return 0;
        }
        var keys = transactions.stream()
                .map(t -> new TransactionService.TransactionPrimaryKey(t.getUserId(), t.getAccountId(), t.getDate(), t.getId(), t.getStatus()))
                .collect(toList());
        atTransactionsRepository.deleteSpecificTransactions(keys);
        return transactions.size();
    }

    /**
     * @return the number of transactions that were deleted
     */
    private int deleteDsTransactions(List<DsTransaction> transactions, boolean dryrun) {
        if (dryrun || transactions.isEmpty()) {
            return 0;
        }
        dsTransactionsRepository.deleteTransactions(transactions);
        return transactions.size();
    }

    boolean isOlderThanReferenceDate(Transaction trx, LocalDate referenceDate) {
        return trx.getDate().isBefore(referenceDate);
    }

    boolean isOlderThanReferenceDate(DsTransaction trx, LocalDate referenceDate) {
        return LocalDate.parse(trx.getDate()).isBefore(referenceDate);
    }

    UUID clientId(UUID userId) {
        if (userContextClientErrors.contains(userId)) {
            return null;
        }
        if (!userIdToClientIdMap.containsKey(userId)) {
            var optionalCtx = usersClient.getUser(userId);
            optionalCtx.ifPresentOrElse(
                    (ctx) -> userIdToClientIdMap.put(userId, ctx.clientId()),
                    () -> {
                        log.warn("Can't retrieve UserContext for userId={}.", userId);
                        userContextClientErrors.add(userId);
                    }
            );
        }
        return userIdToClientIdMap.get(userId);
    }

    /**
     * An as extra safeguard (to prevent catastrophic errors) we check that
     * we are not running on an unexpected environment or with an unexpected
     * clientId.
     */
    static void crashUnlessPreconditionsMet(String environmentName, UUID requestedClientId) {
        // Only run on these whitelisted environments.
        if (!"yfb-prd".equals(environmentName)) {
            throw new RuntimeException("refusing to run on environment \"" + environmentName + "\"");
        }
        // Only run on production with this whitelisted clientId
        if (!requestedClientId.equals(clientIdFrance)) {
            throw new RuntimeException("refusing to run on yfb-prd with clientId=" + requestedClientId);
        }
    }

}
