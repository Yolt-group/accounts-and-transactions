package com.yolt.accountsandtransactions.datascience;

import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.mapping.Mapper;
import com.yolt.accountsandtransactions.BaseIntegrationTest;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Date;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

public class DsAccountsCurrentRepositoryTest extends BaseIntegrationTest {

    private static final String EXTENDED_ACCOUNT_JSON = "{\"resourceId\":\"1234\",\"currency\":\"GBP\",\"name\":\"my " +
            "account\",\"product\":\"my product\",\"status\":\"active\",\"bic\":\"1900578\",\"details\":\"hey ho\"}";
    private static final Date LAST_UPDATED = Date.from(LocalDateTime.now().toInstant(ZoneOffset.UTC));

    @Autowired
    private DataScienceCassandraSession dataScienceCassandraSession;

    @Autowired
    private DsAccountsCurrentRepository dsAccountsCurrentRepository;

    private Mapper<DsAccountCurrent> cassandraMapper;
    @Value("${spring.data.cassandra.ds-keyspace-name}")
    private String dsKeyspace;

    @Override
    protected void setup() {
        cassandraMapper = mappingManager.mapper(DsAccountCurrent.class, dsKeyspace);
    }

    /**
     * Asserts that all the fields set in {@link DsAccountCurrent} are stored in the cassandra table.
     * <p/>
     * If this test fails, there is probably a mapping missing in {@link DsAccountsCurrentRepository#save(DsAccountCurrent)}
     */
    @Test
    public void testAllFieldsUpdatedWhenSaving() {

        UUID userId = UUID.randomUUID();
        UUID accountId = UUID.randomUUID();

        // use the AllArgsConstructor to enforce compile time checks on missing values
        DsAccountCurrent unsavedDsAccountCurrent = new DsAccountCurrent(
                userId,
                accountId,
                UUID.randomUUID(), // userSiteId
                UUID.randomUUID(), // siteId
                "externalAccountId",
                "externalSiteId",
                "name",
                "accountType",
                "GBP",
                BigDecimal.TEN, // currentBalance
                BigDecimal.TEN, // availableBalance,
                Date.from(Instant.now(Clock.systemUTC())), //lastUpdatedTime
                "status",
                "statusDetail",
                "provider",
                ByteBuffer.wrap("Hello World".getBytes()), //extendedAccount
                false, //hidden,
                "linkedAccount",
                "bankSpecific",
                "accountHolderName",
                "accountScheme",
                "accountIdentification",
                "accountSecondaryIdentification",
                "accountMaskedIdentification",
                false // closed
        );

        // save account
        dsAccountsCurrentRepository.save(unsavedDsAccountCurrent);

        ResultSet resultSet = dataScienceCassandraSession.getSession()
                .execute(String.format("SELECT * FROM %s WHERE user_id = %s and account_id = %s", DsAccountCurrent.TABLE_NAME, userId, accountId));
        Row row = resultSet.one();

        assertThat(row.getColumnDefinitions()).hasSize(25);
        for (ColumnDefinitions.Definition columnDefinition : row.getColumnDefinitions()) {
            assertThat(row.isNull(columnDefinition.getName())).isFalse();
        }
    }

    @Test
    public void testAccountSaveAndGet() {
        DsAccountCurrent dsAccountCurrent = currentAccount(false);
        dsAccountsCurrentRepository.save(dsAccountCurrent);

        DsAccountCurrent result = cassandraMapper.get(dsAccountCurrent.getUserId(), dsAccountCurrent.getAccountId());

        assertThat(result).isEqualToComparingFieldByField(dsAccountCurrent);
    }

    @Test
    public void testExtendedAccountSaveAndGet() {
        DsAccountCurrent dsAccountCurrent = currentAccount(true);
        dsAccountsCurrentRepository.save(dsAccountCurrent);

        DsAccountCurrent result = cassandraMapper.get(dsAccountCurrent.getUserId(), dsAccountCurrent.getAccountId());

        assertThat(result.getExtendedAccountAsString()).isEqualTo(EXTENDED_ACCOUNT_JSON);
    }

    @Test
    public void testNullValuesUpdate() {
        DsAccountCurrent dsAccountCurrent = currentAccount(true);
        dsAccountsCurrentRepository.save(dsAccountCurrent);

        DsAccountCurrent result = cassandraMapper.get(dsAccountCurrent.getUserId(), dsAccountCurrent.getAccountId());
        assertThat(result).isEqualToComparingFieldByField(dsAccountCurrent);

        DsAccountCurrent updatedAccount = DsAccountCurrent.builder()
                .userId(dsAccountCurrent.getUserId())
                .accountId(dsAccountCurrent.getAccountId())
                .userSiteId(null)
                .siteId(null)
                .externalAccountId(null)
                .externalSiteId(null)
                .name(null)
                .accountType(null)
                .currencyCode(null)
                .currentBalance(null)
                .availableBalance(null)
                .lastUpdatedTime(null)
                .status(null)
                .statusDetail(null)
                .provider(null)
                .extendedAccount(null)
                .hidden(null)
                .build();
        dsAccountsCurrentRepository.save(updatedAccount);

        result = cassandraMapper.get(dsAccountCurrent.getUserId(), dsAccountCurrent.getAccountId());
        assertThat(result).isEqualToIgnoringGivenFields(dsAccountCurrent, "currentBalance", "availableBalance", "extendedAccount");
        assertThat(result.getCurrentBalance()).isNull();
        assertThat(result.getAvailableBalance()).isNull();
        assertThat(result.getExtendedAccount()).isNull();
    }

    private static DsAccountCurrent currentAccount(boolean withExtendedData) {
        return DsAccountCurrent.builder()
                .userId(new UUID(0, 1))
                .accountId(UUID.randomUUID())
                .userSiteId(UUID.randomUUID())
                .siteId(UUID.randomUUID())
                .externalAccountId("1")
                .externalSiteId("1")
                .name("mega account")
                .accountType("Mega account")
                .currencyCode("GBP")
                .currentBalance(new BigDecimal("100"))
                .availableBalance(new BigDecimal("95"))
                .lastUpdatedTime(LAST_UPDATED)
                .status("PROCESSING_TRANSACTIONS_FINISHED")
                .statusDetail("0_SUCCESS")
                .provider("test_provider")
                .linkedAccount("ABC-1")
                .extendedAccount(withExtendedData ? StandardCharsets.UTF_8.encode(EXTENDED_ACCOUNT_JSON) : null)
                .hidden(true)
                .build();
    }

    @Test
    void save() {
    }
}
