package com.yolt.accountsandtransactions.batch;

import com.yolt.accountsandtransactions.offloading.BatchPushOffloadData;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import static org.springframework.http.MediaType.APPLICATION_JSON_VALUE;

/**
 * Endpoints to trigger batch jobs.  Internal use only.
 */
@RestController
@RequiredArgsConstructor
public class BatchController {

    private final BatchJobCheckOldPendingTransactions batchJobCheckOldPendingTransactions;
    private final BatchJobSyncTransactionTables batchJobSyncTransactionTables;
    private final BatchDeleteTransactionsOlderThanOneYearForFrance batchDeleteTransactionsOlderThanOneYearForFrance;
    private final BatchPushOffloadData batchPushOffloadData;

    @PostMapping(value = "/batch/check-old-pending-transactions", produces = APPLICATION_JSON_VALUE)
    public ResponseEntity<Void> runBatchJobCheckOldPendingTransactions() {
        batchJobCheckOldPendingTransactions.run();
        return ResponseEntity.accepted().build();
    }

    @PostMapping(value = "/batch/sync-transaction-tables", produces = APPLICATION_JSON_VALUE)
    public ResponseEntity<Void> runBatchJobSyncTransactionTables(
            @RequestParam(value = "dryrun", defaultValue = "true") boolean dryrun,
            @RequestParam(value = "max_users", defaultValue = "50000") int maxUsers
    ) {
        batchJobSyncTransactionTables.run(dryrun, maxUsers);
        return ResponseEntity.accepted().build();
    }

    @PostMapping(value = "/batch/delete-transactions-older-than-one-year-for-france")
    public ResponseEntity<Void> deleteTransactionsOlderThanOneYearForFrance(
            @RequestParam(value = "dryrun", defaultValue = "true") boolean dryrun
    ) {
        batchDeleteTransactionsOlderThanOneYearForFrance.run(dryrun);
        return ResponseEntity.accepted().build();
    }

    @PostMapping(value = "/batch/push-all-accounts-to-offload-topic")
    public ResponseEntity<Void> pushAllAccountsToOffloadTopic(
            @RequestParam(value = "dryrun", defaultValue = "true") boolean dryrun,
            @RequestParam(value = "max-read-per-second", defaultValue = "5000") int maxReadPerSecond
    ) {
        batchPushOffloadData.offloadAccounts(dryrun, maxReadPerSecond);
        return ResponseEntity.accepted().build();
    }

    @PostMapping(value = "/batch/push-all-transactions-to-offload-topic")
    public ResponseEntity<Void> pushAllTransactionsToOffloadTopic(
            @RequestParam(value = "dryrun", defaultValue = "true") boolean dryrun,
            @RequestParam(value = "max-read-per-second", defaultValue = "5000") int maxReadPerSecond
    ) {
        batchPushOffloadData.offloadTransactions(dryrun, maxReadPerSecond);
        return ResponseEntity.accepted().build();
    }
}
