package com.yolt.accountsandtransactions.inputprocessing.enrichments.activities;

import org.springframework.data.repository.CrudRepository;
import org.springframework.stereotype.Repository;
import org.springframework.validation.annotation.Validated;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;
@Validated
@Repository
public interface ActivityEnrichmentRepository extends CrudRepository<ActivityEnrichment, UUID> {

    List<ActivityEnrichment> findAllByStartedAtBefore(Instant before);
}
