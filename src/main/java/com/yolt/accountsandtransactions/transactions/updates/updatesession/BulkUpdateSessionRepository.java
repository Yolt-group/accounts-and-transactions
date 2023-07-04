package com.yolt.accountsandtransactions.transactions.updates.updatesession;

import com.datastax.driver.core.Session;
import lombok.extern.slf4j.Slf4j;
import nl.ing.lovebird.cassandra.CassandraRepository;
import org.springframework.stereotype.Repository;
import org.springframework.validation.annotation.Validated;

import java.util.Optional;
import java.util.UUID;

import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;
import static com.yolt.accountsandtransactions.transactions.updates.updatesession.BulkUpdateSession.UPDATE_SESSION_ID_COLUMN;
import static com.yolt.accountsandtransactions.transactions.updates.updatesession.BulkUpdateSession.USER_ID_COLUMN;

@Validated
@Repository
@Slf4j
public class BulkUpdateSessionRepository extends CassandraRepository<BulkUpdateSession> {
    protected BulkUpdateSessionRepository(Session session) {
        super(session, BulkUpdateSession.class);
    }

    BulkUpdateSession persist(BulkUpdateSession bulkUpdateSession) {
        save(bulkUpdateSession);
        return bulkUpdateSession;
    }

    Optional<BulkUpdateSession> find(UUID userId, UUID sessionId) {
        var select = createSelect();
        select.where(eq(USER_ID_COLUMN, userId))
                .and(eq(UPDATE_SESSION_ID_COLUMN, sessionId));
        return select(select).stream().findFirst();
    }
}
