package com.yolt.accountsandtransactions.inputprocessing.enrichments.activities;

import lombok.*;

import javax.persistence.*;
import java.io.Serializable;
import java.time.LocalDate;
import java.util.UUID;



@AllArgsConstructor
@NoArgsConstructor
@Table(name = "activity_enrichments_accounts")
@Entity
@Builder
@Getter
@Setter
@IdClass(ActivityEnrichmentAccount.ActivityEnrichmentAccountId.class)
public class ActivityEnrichmentAccount {

    @Id
    @Column(name="activity_id")
    private UUID activityId;

    @Id
    @Column(name="account_id")
    private UUID accountId;

    @NonNull
    @Column(name = "oldest_transaction_ts")
    private LocalDate oldestTransactionTs;

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class ActivityEnrichmentAccountId implements Serializable {
        UUID activityId;
        UUID accountId;
    }
}
