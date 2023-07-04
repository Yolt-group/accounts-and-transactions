package com.yolt.accountsandtransactions.transactions.updates.updatesession;

import com.datastax.driver.mapping.annotations.ClusteringColumn;
import com.datastax.driver.mapping.annotations.Column;
import com.datastax.driver.mapping.annotations.PartitionKey;
import com.datastax.driver.mapping.annotations.Table;
import lombok.*;
import nl.ing.lovebird.cassandra.codec.LocalDateTypeCodec;

import java.time.LocalDate;
import java.util.Map;
import java.util.UUID;

/**
 * A `BulkUpdateSession` contains information that is relevant during a bulk-update (bulk-feedback) session. A bulk
 * update can span multiple requests/responses. The information that is relevant in these phases is stored in the
 * `BulkUpdateSession` and can be accessed by the user through the combination of the `userId` and the `updateSessionId`.
 * The latter should be passed along with each request. This table has a TTL which will automatically invalidate the
 * session after a certain amount of time.
 */
@AllArgsConstructor
@NoArgsConstructor
@Table(name = "bulk_update_sessions")
@Data
@Builder
public class BulkUpdateSession {
    public static final String USER_ID_COLUMN = "user_id";
    public static final String ACCOUNT_ID_COLUMN = "account_id";
    public static final String UPDATE_SESSION_ID_COLUMN = "update_session_id";
    public static final String DATE_COLUMN = "date";
    public static final String TRANSACTION_ID_COLUMN = "transaction_id";
    public static final String DETAILS_COLUMN = "details";

    @NonNull
    @PartitionKey
    @Column(name = USER_ID_COLUMN)
    private UUID userId;

    @NonNull
    @ClusteringColumn
    @Column(name = UPDATE_SESSION_ID_COLUMN)
    private UUID updateSessionId;

    @NonNull
    @Column(name = ACCOUNT_ID_COLUMN)
    private UUID accountId;

    @NonNull
    @Column(name = DATE_COLUMN, codec = LocalDateTypeCodec.class)
    private LocalDate date;

    @NonNull
    @Column(name = TRANSACTION_ID_COLUMN)
    private String transactionId;

    @Column(name = DETAILS_COLUMN)
    private Map<String, String> details;
}
