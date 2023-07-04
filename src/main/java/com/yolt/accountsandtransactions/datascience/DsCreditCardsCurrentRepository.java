package com.yolt.accountsandtransactions.datascience;

import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.Select;
import com.datastax.driver.mapping.Mapper;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import nl.ing.lovebird.cassandra.CassandraRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.UUID;

import static com.datastax.driver.core.querybuilder.QueryBuilder.*;

/**
 * Do **NOT** use the {@link Session} that is available on the {@link ApplicationContext}.
 */
@Repository
@Slf4j
public class DsCreditCardsCurrentRepository extends CassandraRepository<DSCreditCardCurrent> {
    @Autowired
    public DsCreditCardsCurrentRepository(final DataScienceCassandraSession session) {
        super(session.getSession(), DSCreditCardCurrent.class);
    }

    @Override
    public void save(final DSCreditCardCurrent creditCard) {
        // Avoid writing NULL's to C* to prevent creation of too many tombstones
        this.mapper.save(creditCard, Mapper.Option.saveNullFields(false));

        log.debug("Saved {}", creditCard);
    }

    public List<DSCreditCardCurrent> getAccountsForUser(final UUID userId) {
        Select select = super.createSelect();
        select.where(eq(DSCreditCardCurrent.USER_ID_COLUMN, userId));
        return select(select);
    }

    public Mapper<DSCreditCardCurrent> getMapper() {
        return mapper;
    }

    public void updateUserSiteId(@NonNull UUID userId, @NonNull UUID accountId, @NonNull UUID newUserSiteId) {
        var query = update("creditcards_current")
                .with(set("user_site_id", newUserSiteId))
                .where(eq("user_id", userId))
                .and(eq("account_id", accountId));
        session.execute(query);
    }

}
