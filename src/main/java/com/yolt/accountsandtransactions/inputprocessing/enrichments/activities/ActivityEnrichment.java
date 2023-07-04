package com.yolt.accountsandtransactions.inputprocessing.enrichments.activities;

import com.yolt.accountsandtransactions.inputprocessing.enrichments.api.EnrichmentMessageType;
import lombok.*;

import javax.persistence.*;
import java.time.Instant;
import java.time.LocalDate;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

/**
 * This table contains information about an activity enrichment. In other words, it contains the progress of a
 * datascience pipeline that has been kicked off due to a {@link ActivityEnrichmentType}.
 *
 * A little note on the checksums:
 * For a refresh we expect 1 enrichment of every type (category, label, transaction cycles, ..)
 * expected_checksum : 1111
 * Unfortunately, due to legacy reasons, a feedback event results in 1 enrichment of every type, PLUS an extra enrichment of
 * the same type of the feedback. Example: A category feedback results in a category enrichent twice, and one of every other type.
 * expected_checksum: 2111
 *
 */
@AllArgsConstructor
@NoArgsConstructor
@Table(name = "activity_enrichments")
@Entity
@Builder
@Getter
public class ActivityEnrichment {

    @Id
    @Column(name = "activity_id")
    private UUID activityId;

    @NonNull
    @Column(name = "started_at")
    private Instant startedAt;

    @NonNull
    @Column(name = "enrichment_type")
    @Enumerated(EnumType.STRING)
    private ActivityEnrichmentType enrichmentType;

    @NonNull
    @Column(name = "user_id")
    private UUID userId;

    @Setter
    @Column(name = "checksum")
    private int checksum;

    @OneToMany(cascade = CascadeType.ALL, fetch = FetchType.EAGER, orphanRemoval = true)
    @JoinColumn(name = "activity_id", nullable = false, insertable = false, updatable = false)
    private Set<ActivityEnrichmentAccount> activityEnrichmentAccounts;

    public void addActivityEnrichmentAccount(ActivityEnrichmentAccount activityEnrichmentAccount) {
        activityEnrichmentAccounts.add(activityEnrichmentAccount);

    }
}
