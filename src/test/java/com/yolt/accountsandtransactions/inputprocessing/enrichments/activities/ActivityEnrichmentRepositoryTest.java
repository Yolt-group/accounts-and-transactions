package com.yolt.accountsandtransactions.inputprocessing.enrichments.activities;

import com.yolt.accountsandtransactions.BaseIntegrationTest;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.time.Instant;
import java.time.LocalDate;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

class ActivityEnrichmentRepositoryTest extends BaseIntegrationTest {

    @Autowired
    ActivityEnrichmentRepository activityEnrichmentRepository;

    @Test
    void given_AnActivityEnrichtmentSavedWithAccounts_then_itShoulBeRetrievableWithTheAccounts() {
        // Given
        var activityId = UUID.randomUUID();
        activityEnrichmentRepository.save(new ActivityEnrichment(activityId, Instant.now(), ActivityEnrichmentType.REFRESH, UUID.randomUUID(), 0, Set.of()));

        // When
        Optional<ActivityEnrichment> byId = activityEnrichmentRepository.findById(activityId);

        // Then
        assertThat(byId).isNotEmpty();

        // Given add account
        var accountId = UUID.randomUUID();
        ActivityEnrichment activityEnrichment = byId.get();
        activityEnrichment.addActivityEnrichmentAccount(new ActivityEnrichmentAccount(activityId, accountId, LocalDate.of(2000,1,1)));
        activityEnrichmentRepository.save(activityEnrichment);

        Optional<ActivityEnrichment> foundActivityEnrichment = activityEnrichmentRepository.findById(activityId);
        assertThat(foundActivityEnrichment).isPresent();
        assertThat(foundActivityEnrichment.get().getActivityEnrichmentAccounts().toArray()).extracting("accountId")
                .containsExactly(accountId);

    }
}