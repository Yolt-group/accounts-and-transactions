package com.yolt.accountsandtransactions.batch;

import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.mapping.Mapper;
import lombok.NonNull;
import nl.ing.lovebird.cassandra.CassandraRepository;
import org.springframework.stereotype.Repository;
import org.springframework.validation.annotation.Validated;

import javax.validation.Valid;
import java.util.Optional;
import java.util.UUID;

import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;
import static com.datastax.driver.core.querybuilder.QueryBuilder.truncate;

@Repository
@Validated
public class BatchSyncProgressStateRepository extends CassandraRepository<BatchSyncProgressState> {

    public BatchSyncProgressStateRepository(Session session) {
        super(session, BatchSyncProgressState.class);
    }

    public Optional<BatchSyncProgressState> get(@NonNull UUID userId) {
        return selectOne(QueryBuilder
                .select()
                .from("batch_sync_progress_state")
                .where(eq("user_id", userId))
        );
    }

    public void upsert(@NonNull @Valid BatchSyncProgressState batchSyncProgressState) {
        super.save(batchSyncProgressState, Mapper.Option.saveNullFields(false));
    }

    public void reset() {
        session.execute(truncate("batch_sync_progress_state"));
    }

}
