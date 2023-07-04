package com.yolt.accountsandtransactions;

import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.mapping.MappingManager;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.yolt.accountsandtransactions.accounts.Account;
import com.yolt.accountsandtransactions.accounts.AccountRepository;
import com.yolt.accountsandtransactions.datascience.DSCreditCardCurrent;
import com.yolt.accountsandtransactions.datascience.DsAccountCurrent;
import com.yolt.accountsandtransactions.datascience.DsTransaction;
import com.yolt.accountsandtransactions.inputprocessing.AccountFromProviders;
import com.yolt.accountsandtransactions.inputprocessing.AccountsAndTransactionsRequestDTO;
import lombok.extern.slf4j.Slf4j;
import nl.ing.lovebird.cassandra.test.EnableExternalCassandraTestDatabase;
import nl.ing.lovebird.kafka.test.EnableExternalKafkaTestCluster;
import nl.ing.lovebird.postgres.test.EnableExternalPostgresTestDatabase;
import org.junit.jupiter.api.BeforeEach;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.cloud.contract.wiremock.AutoConfigureWireMock;
import org.springframework.test.context.ActiveProfiles;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static com.yolt.accountsandtransactions.TestUtils.toAccount;

@Slf4j
@SpringBootTest(classes = {AccountsAndTransactionsApplication.class, TestConfiguration.class})
@ActiveProfiles("test")
@AutoConfigureWireMock(port = 0)
@AutoConfigureMockMvc
@EnableExternalKafkaTestCluster
@EnableExternalCassandraTestDatabase
@EnableExternalPostgresTestDatabase
public abstract class BaseIntegrationTest {
    protected MappingManager mappingManager;

    @Autowired
    protected Session session;

    @Autowired
    protected ObjectMapper objectMapper;

    @Autowired
    protected AccountRepository accountRepository;

    @Autowired
    protected MappingAccountIdProvider mappingAccountIdProvider;

    @Value("${spring.data.cassandra.ds-keyspace-name}")
    protected String dsKeyspace;


    @BeforeEach
    public void initialSetup() {
        mappingManager = new MappingManager(session);

        session.execute(QueryBuilder.truncate(dsKeyspace, DsTransaction.TABLE_NAME));
        session.execute(QueryBuilder.truncate(dsKeyspace, DsAccountCurrent.TABLE_NAME));
        session.execute(QueryBuilder.truncate(dsKeyspace, DSCreditCardCurrent.TABLE_NAME));

        setup();
    }

    protected void setup() {
        mappingAccountIdProvider.clear();
    }

    protected void stubExistingAccounts(UUID userId, final AccountsAndTransactionsRequestDTO accountsAndTransactionsRequestDTO, UUID... internalAccountIds) {

        List<Account> accounts = new ArrayList<>();
        for (int i = 0; i < accountsAndTransactionsRequestDTO.getIngestionAccounts().size(); i++) {

            AccountFromProviders ingestionAccount = accountsAndTransactionsRequestDTO.getIngestionAccounts().get(i);
            UUID internalAccountId = internalAccountIds[i];

            mappingAccountIdProvider.addMapping(ingestionAccount.getName(), internalAccountId);

            Account account = toAccount(ingestionAccount).toBuilder()
                    .userId(userId)
                    .id(internalAccountId)
                    .build();
            accounts.add(account);
        }

        accountRepository.saveBatch(accounts, 100);
    }

    protected void stubNewAccounts(AccountsAndTransactionsRequestDTO accountsAndTransactionsRequestDTO, UUID... newAccountIds) {

        for (int i = 0; i < accountsAndTransactionsRequestDTO.getIngestionAccounts().size(); i++) {
            mappingAccountIdProvider.addMapping(accountsAndTransactionsRequestDTO.getIngestionAccounts().get(i).getName(), newAccountIds[i]);
        }
    }
}
