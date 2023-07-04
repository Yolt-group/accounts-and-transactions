package com.yolt.accountsandtransactions;

import brave.baggage.BaggageField;
import brave.baggage.CorrelationScopeConfig;
import brave.context.slf4j.MDCScopeDecorator;
import brave.propagation.CurrentTraceContext;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import com.datastax.driver.extras.codecs.enums.EnumNameCodec;
import com.datastax.driver.extras.codecs.jdk8.InstantCodec;
import com.datastax.driver.extras.codecs.jdk8.LocalDateCodec;
import com.yolt.accountsandtransactions.accounts.Account;
import com.yolt.accountsandtransactions.datascience.DataScienceCassandraSession;
import com.yolt.accountsandtransactions.inputprocessing.AccountIdProvider;
import com.yolt.accountsandtransactions.inputprocessing.AccountIdProvider.RandomAccountIdProvider;
import com.yolt.accountsandtransactions.inputprocessing.TransactionIdProvider;
import com.yolt.accountsandtransactions.inputprocessing.enrichments.api.EnrichmentMessageType;
import com.yolt.accountsandtransactions.transactions.Transaction;
import com.yolt.accountsandtransactions.transactions.cycles.CycleType;
import io.micrometer.core.aop.TimedAspect;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.RequiredArgsConstructor;
import nl.ing.lovebird.cassandra.codec.LocalDateTypeCodec;
import nl.ing.lovebird.extendeddata.account.BalanceType;
import nl.ing.lovebird.extendeddata.account.UsageType;
import nl.ing.lovebird.extendeddata.common.CurrencyCode;
import nl.ing.lovebird.extendeddata.transaction.TransactionStatus;
import nl.ing.lovebird.providerdomain.AccountType;
import nl.ing.lovebird.providerdomain.YoltCategory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.actuate.health.AbstractHealthIndicator;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableScheduling;

import javax.annotation.PostConstruct;
import java.time.Clock;
import java.util.UUID;

@RequiredArgsConstructor
@Configuration
public class ApplicationConfiguration {

    @Autowired
    private Cluster cluster;

    @PostConstruct
    public void registerCodecs() {
        this.cluster.getConfiguration().getCodecRegistry().register(new EnumNameCodec<>(AccountType.class));
        this.cluster.getConfiguration().getCodecRegistry().register(new EnumNameCodec<>(CurrencyCode.class));
        this.cluster.getConfiguration().getCodecRegistry().register(new EnumNameCodec<>(TransactionStatus.class));
        this.cluster.getConfiguration().getCodecRegistry().register(new EnumNameCodec<>(YoltCategory.class));
        this.cluster.getConfiguration().getCodecRegistry().register(new EnumNameCodec<>(Account.Status.class));
        this.cluster.getConfiguration().getCodecRegistry().register(new EnumNameCodec<>(UsageType.class));
        this.cluster.getConfiguration().getCodecRegistry().register(new EnumNameCodec<>(CycleType.class));
        this.cluster.getConfiguration().getCodecRegistry().register(new EnumNameCodec<>(EnrichmentMessageType.class));
        this.cluster.getConfiguration().getCodecRegistry().register(new EnumNameCodec<>(BalanceType.class));
        this.cluster.getConfiguration().getCodecRegistry().register(new EnumNameCodec<>(Transaction.FillType.class));
        this.cluster.getConfiguration().getCodecRegistry().register(InstantCodec.instance);
        this.cluster.getConfiguration().getCodecRegistry().register(LocalDateCodec.instance);
        this.cluster.getConfiguration().getCodecRegistry().register(new LocalDateTypeCodec());
    }

    @ConditionalOnProperty(value = "yolt.accounts-and-transactions.scheduling.enabled", havingValue = "true", matchIfMissing = true)
    @Configuration
    @EnableScheduling
    public static class SchedulingConfiguration {
    }

    @Bean
    public HealthIndicator datascienceKeyspaceCassaHealth(
            @Value("${spring.data.cassandra.ds-keyspace-name}") String dsKeyspace,
            DataScienceCassandraSession cassandraSession
    ) {
        return new CassandraHealthIndicator(dsKeyspace, cassandraSession.getSession());
    }

    /**
     * Copied from LBC.
     */
    @RequiredArgsConstructor
    static class CassandraHealthIndicator extends AbstractHealthIndicator {
        private final String keyspaceName;
        private final Session session;

        /**
         * Query Cassandra to access the model mutation table, and if that succeeds mark Cassandra as up;
         * If that fails exception thrown will mark Cassandra as down.
         * <p>
         * At the time this class was made (spring-boot 1.4) spring-data did not support the newer 3.x branch of Cassandra
         * yet. Spring Data was dropped and a copy of the health indicator included here. Should we move back to Spring Data
         * Cassandra then we can drop this class.
         * <p>
         * Recently we experienced an issue where the service couldn't access keyspaces due to lack of permissions.
         * This didn't set the Cassandra connection as unhealthy because the service could still access the
         * resource (system.local) we use for the health check. So instead we use the key space name used by the pod and
         * check for the well known model mutation table.
         */
        @Override
        protected void doHealthCheck(Health.Builder builder) {
            Select select = QueryBuilder.select("hcpk")
                    .from(keyspaceName, "modelmutation")
                    // Does not exist. Makes query efficient.
                    .where(QueryBuilder.eq("hcpk", "cassandra-health-indicator-check"))
                    .limit(1);

            select.setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM);
            ResultSet results = session.execute(select);

            // Will throw if table was not accessible
            results.all();

            builder.up().withDetail("modelmutation", "accessible");
        }
    }

    @Bean
    public Clock clock() {
        return Clock.systemUTC();
    }

    @Bean
    public AccountIdProvider<UUID> accountIdProvider() {
        return new RandomAccountIdProvider();
    }

    @Bean
    public TransactionIdProvider<UUID> transactionIdProvider() {
        return new TransactionIdProvider.RandomTransactionIdProvider();
    }

    @Bean
    public TimedAspect timedAspect(MeterRegistry registry) {
        return new TimedAspect(registry);
    }

    @Bean
    BaggageField clientIdField() {
        return BaggageField.create("client-id");
    }

    @Bean
    BaggageField userIdField() {
        return BaggageField.create("user-id");
    }

    @Bean
    CurrentTraceContext.ScopeDecorator mdcScopeDecorator() {
        return MDCScopeDecorator.newBuilder()
                .clear()
                .add(CorrelationScopeConfig.SingleCorrelationField.newBuilder(clientIdField())
                        .flushOnUpdate()
                        .build())
                .add(CorrelationScopeConfig.SingleCorrelationField.newBuilder(userIdField())
                        .flushOnUpdate()
                        .build())
                .build();
    }
}
