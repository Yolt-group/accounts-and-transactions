package com.yolt.accountsandtransactions.datascience;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.yolt.accountsandtransactions.ApplicationConfiguration;
import lombok.Getter;
import nl.ing.lovebird.cassandrabatch.pager.SelectAllEntityPager;
import nl.ing.lovebird.cassandrabatch.throttler.CassandraBatchThrottler;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;

/**
 * A separate {@link Session} that is used for the datascience keyspace.  We do this because LBC exposes one
 * {@link Session} on the {@link ApplicationContext} and it connects to a specific keyspace: our keyspace.
 *
 * Since both the accounts_and_transactions.transactions and datascience.transactions tables exist, it is
 * easy to make a mistake and query the wrong table: for example when using a {@link CassandraBatchThrottler}
 * with a {@link SelectAllEntityPager}.  These classes assume the table you want to query is in the
 * keyspace linked to {@link Session}.  This can lead to dangerous situations because the tables are alike.
 *
 * Health check for this: {@link ApplicationConfiguration#datascienceKeyspaceCassaHealth}
 */
@Component
public class DataScienceCassandraSession {

    public DataScienceCassandraSession(
            Cluster cluster,
            @Value("${spring.data.cassandra.ds-keyspace-name}") String dsKeyspace
    ) {
        session = cluster.connect(dsKeyspace);
    }

    @Getter
    Session session;

}