package com.yolt.accountsandtransactions.accounts;

import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.Delete;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.mapping.Mapper;
import lombok.NonNull;
import nl.ing.lovebird.cassandra.CassandraRepository;
import org.springframework.stereotype.Repository;
import org.springframework.validation.annotation.Validated;

import javax.validation.Valid;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static com.datastax.driver.core.querybuilder.QueryBuilder.*;
import static java.util.stream.Collectors.toMap;

@Validated
@Repository
public class AccountRepository extends CassandraRepository<Account> {

    protected AccountRepository(Session session) {
        super(session, Account.class);
        setAuditLoggingEnabled(false);
    }

    public List<Account> getAccounts(UUID userId) {
        return select(
                QueryBuilder.select()
                        .from("accounts")
                        .where(eq("user_id", userId)));
    }

    public List<Account> getAccountsForSiteId(UUID siteId) {
        return select(QueryBuilder.select()
                .from("accounts")
                .where(eq("site_id", siteId))
                .allowFiltering());
    }

    public Map<UUID, UUID> getUserSiteIdsForAccountIds(UUID userId, Collection<UUID> accountIds) {
        var select = QueryBuilder.select("id", "user_site_id")
                .from("accounts");

        select.where(eq("user_id", userId))
                .and(in("id", accountIds));

        return session.execute(select).all().stream()
                .collect(toMap(row -> row.getUUID(0), row -> row.getUUID(1)));
    }

    public void upsert(@Valid Account account) {
        super.save(account, Mapper.Option.saveNullFields(false));
    }

    public void updateUserSiteId(@NonNull UUID userId, @NonNull UUID accountId, @NonNull UUID newUserSiteId) {
        var query = update("accounts")
                .with(set("user_site_id", newUserSiteId))
                .where(eq("user_id", userId))
                .and(eq("id", accountId));
        session.execute(query);
    }

    public void deleteAccounts(UUID userId, List<UUID> accountIds) {
        Delete delete = super.createDelete();
        delete.where(eq("user_id", userId)).and(in("id", accountIds));
        executeDelete(delete);
    }

    public void deleteAllAccountsForUser(@NonNull UUID userId) {
        Delete delete = super.createDelete();
        delete.where(eq("user_id", userId));
        executeDelete(delete);
    }

    public Mapper<Account> getMapper() {
        return mapper;
    }
}
