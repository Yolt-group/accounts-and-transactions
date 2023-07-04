package com.yolt.accountsandtransactions.inputprocessing;

import lombok.Builder;
import lombok.Value;

/**
 * This object contains counters that contain information about a reconciliation operation, this service must compare
 * transactions that come in from the bank with transactions that we've stored in the database.  This isn't always
 * straightforward so we monitor the process in Grafana for weird or out of place numbers.
 * <p>
 * USAGE
 * ---
 * <p>
 * How to interpret the counters is described below in Javadoc in the USAGE line.
 * <p>
 * <p>
 * Meaning of some terms
 * ---
 * <p>
 * upstream or incoming
 * transactions coming in from the bank, there is (except for the first data fetch) overlap with transactions we
 * have in the database, which brings us to ...
 * <p>
 * stored
 * the stored transactions that we already have in the database
 * <p>
 * matching or reconciliation
 * comparing upstream transactions to stored transactions to determine which ones are new, which ones need to be updated
 * and which ones are no longer needed
 */
@Value
@Builder
public class TransactionReconciliationResultMetrics {
    String provider;


    // Matching performance

    /**
     * USAGE Informational only.
     * <p>
     * Counts the number of ingested upstream transactions.
     */
    long upstreamTotal;

    /**
     * USAGE Informational only.
     * <p>
     * Number of upstream trxs that are *new* and that we don't already have in our database.
     * <p>
     * {@link #upstreamNew} <= {@link #upstreamTotal}
     */
    long upstreamNew;

    /**
     * USAGE Informational only.
     * <p>
     * Number of upstream transactions that we already have in our database and have not changed in terms of properties
     */
    long upstreamUnchanged;

    /**
     * USAGE Informational only.
     * <p>
     * Counts the number of stored transactions that were retrieved from the database to determine which ones need to
     * be updated / deleted, and which upstream transactions are new.
     */
    long storedTotal;

    /**
     * USAGE Reconciliation performance and data quality monitoring.  Ideally equal to {@link #storedTotal}.
     * <p>
     * Keep track of reconciliation performance.  Counts how many of the stored transactions we could find in the
     * upstream transactions based on the property externalId.  This is equal to {@link #storedTotal} for
     * well behaving banks.
     */
    long storedMatchedByExternalId;

    /**
     * USAGE Reconciliation performance and data quality monitoring.  Ideally equal to 0.
     * <p>
     * Keep track of reconciliation performance.  Counts how many of the stored transactions we could match in the
     * upstream transactions based on properties other than externalId of the transactions.  Some misbehaving
     * banks such as Barclays give us no identifier and thus {@link #storedMatchedByExternalId} is 0.  In that case all
     * we can do is look at attributes of a transaction.
     * <p>
     * The number of stored transactions that we have matched uniquely by looking at attributes of a transactions such
     * as the amount or date.
     */
    long storedMatchedByAttributesUnique;

    /**
     * USAGE Reconciliation performance and data quality monitoring.  Ideally equal to 0.
     * <p>
     * Similar to {@link #storedMatchedByAttributesUnique} with the difference that there were multiple matching
     * transactions and we picked one of them at random.  This should be OK since the attributes of the transaction
     * that are used to distinguish them for the purposes of datascience are all the same, e.g.: description, date,
     * amount.
     */
    long storedMatchedByAttributesOptimistic;


    /**
     * USAGE Reconciliation performance monitoring.  Ideally equal to 0.
     * <p>
     * The number of transactions with status BOOKED that we have stored in the database, and that we have deleted as
     * a result of the incoming batch.  If this is > 0 it doesn't mean we are deleting stored transactions, it means
     * that we are failing to find transactions that we know about (in our database) in the incoming batch of
     * transactions from the bank (where they are supposed to be).  We are most likely deleting the transaction and
     * re-inserting it under a new identifier.  This is still undesirable because inserting the transaction under a new
     * identifier can confuse clients, worse: we might lose information that datascience or an end-user has assigned to
     * the transaction.
     */
    long storedBookedNotMatched;

    /**
     * USAGE Informational.
     * <p>
     * The number of stored transactions with status PENDING that we are deleting because we could not find it in the
     * batch of upstream transactions.  This counter being > 0 can be legitimate since some pending transactions will
     * never 'confirm' and will be deleted as a result.
     */
    long storedPendingNotMatched;

    /**
     * USAGE Informational.
     * <p>
     * The number of stored transactions that we *could* match, but that we had to delete from the database before
     * inserting them again (and thereby possibly losing datascience enrichments or user data) because the primary
     * key has changed.  Causes can be: "date" field changed, "status" field changed, ...
     */
    long storedPrimaryKeyUpdated;

    /**
     * USAGE Informational only.
     * <p>
     * Number of stored transactions whose status was changed from pending to booked.
     */
    long storedPendingToBooked;

    /**
     * USAGE Reconciliation performance monitoring.  Ideally equal to 0.
     * <p>
     * Number of stored transactions whose status was changed from booked to pending.
     */
    long storedBookedToPending;


    // Data quality monitoring

    /**
     * USAGE
     * We have the expectation that a bank provides us with a stable and unique identifier for a transaction, but that
     * is not always the case.  Use this counter to fish out banks that misbehave, for example:
     * - Barclays sends us no identifiers at all
     * - ABN Amro sends us identifiers for most but not all transactions (e.g. ATM Transactions)
     * <p>
     * Number of incoming transactions where the field externalId is either null or the empty string.
     */
    long upstreamQualityMissingExternalIds;

    /**
     * USAGE
     * Use this to fish out misbehaving banks like {@link #upstreamQualityMissingExternalIds}, this counter counts
     * duplicate identifiers instead of missing ones.
     * <p>
     * The number of transactions coming from upstream that have overlapping ProviderTransactionDTO.externalId fields,
     * that is, the field externalId has a value, but the value is not unique among the batch of incoming transactions.
     * <p>
     * <p>
     * Example: this is a list of unique externalIds: [1, 2, 3, 4, 5].  If transactions come in like this, there
     * are no duplicates and this counter is 0.
     * <p>
     * Example: if a bank sends us this: [1, 2, 2, 3, 4, 5], the value of the counter would be 2, since there are 2
     * transactions that share the same identifier.
     * <p>
     * Example: if the bank sends us this: [1, 1, 1, 2, 2] the value of the counter is 5, since there are 5 transactions
     * that have an identifier that is not unique.
     */
    long upstreamQualityDuplicateExternalIds;
}
