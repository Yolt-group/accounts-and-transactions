package com.yolt.accountsandtransactions.batch;

import com.datastax.driver.mapping.annotations.Column;
import com.datastax.driver.mapping.annotations.PartitionKey;
import com.datastax.driver.mapping.annotations.Table;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.validation.constraints.NotNull;
import java.util.UUID;

/**
 * Auxiliary temporary table introduced in 2020/dec to keep track of the progress we're making on copying over
 * account and transaction data from the datascience keyspace to our own keyspace.
 * <p>
 * We can delete this functionality after all the data has been copied over.
 */
@AllArgsConstructor
@NoArgsConstructor
@Data
@Builder(toBuilder = true)
@Table(name = "batch_sync_progress_state")
public class BatchSyncProgressState {

    @NotNull
    @PartitionKey
    @Column(name = "user_id")
    private UUID userId;

    /**
     * Has all account data been copied over from the datascience keyspace to the A&T keyspace?
     * <p>
     * See {@link BatchJobSyncAccountAndTransactionsTables}
     */
    @Column(name = "acct_synced_from_ds")
    private boolean accountsCopiedFromDSKeyspace;

    /**
     * Have superfluous pending transactions been removed from the A&T keyspace?
     * <p>
     * See {@link BatchJobSyncTransactionTables}
     */
    @Column(name = "trxs_pending_removed")
    private boolean pendingTransactionsRemovedFromATKeyspace;

    /**
     * Has all transaction data been copied over from the datascience keyspace to the A&T keyspace?
     * <p>
     * See {@link BatchJobSyncTransactionTables}
     */
    @Column(name = "trxs_synced_from_ds")
    private boolean transactionsCopiedFromDSKeyspace;

}
