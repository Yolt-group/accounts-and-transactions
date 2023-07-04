package com.yolt.accountsandtransactions.datascience;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.yolt.accountsandtransactions.TestBuilders;
import com.yolt.accountsandtransactions.inputprocessing.TransactionReconciliationResultMetrics;
import com.yolt.accountsandtransactions.transactions.Transaction;
import nl.ing.lovebird.providerdomain.ProviderTransactionDTO;
import org.junit.jupiter.api.Test;
import org.springframework.http.converter.json.Jackson2ObjectMapperBuilder;

import java.time.Clock;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static com.yolt.accountsandtransactions.datascience.TransactionSyncService.reconcileUpstreamTransactionsWithPersisted;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * This tests uses real-world (anonymized) data from a Danskebank user.
 */
public class TransactionReconciliationWithDanskeDataTest {

    @Test
    public void given_aTransactionsIdChangesAcrossRefreshes_when_reconcileTransactions_then_weCanReconcileBasedOnAttributes() throws Throwable {
        ObjectMapper objectMapper = createObjectMapper();
        List<ProviderTransactionDTO> first = new ArrayList<>(objectMapper.readValue(getClass().getResourceAsStream("/transactionmatching/danske/2020-03-30T17_03.anon.json").readAllBytes(), new TypeReference<List<ProviderTransactionDTO>>() {
        }));
        List<ProviderTransactionDTO> second = new ArrayList<>(objectMapper.readValue(getClass().getResourceAsStream("/transactionmatching/danske/2020-03-31T02_47.anon.json").readAllBytes(), new TypeReference<List<ProviderTransactionDTO>>() {
        }));

        List<Transaction> firstAsDs = first.stream().map(TestBuilders::createTransaction).collect(Collectors.toList());

        var result = reconcileUpstreamTransactionsWithPersisted(firstAsDs, second, "BARCLAYS", Clock.systemUTC());

        assertThat(result.getMetrics()).isEqualToComparingFieldByField(TransactionReconciliationResultMetrics.builder()
                .provider("BARCLAYS")
                .upstreamTotal(228)
                .upstreamNew(2)
                .upstreamUnchanged(182)
                .storedTotal(226)
                .storedMatchedByExternalId(182)
                .storedMatchedByAttributesUnique(44)
                .build());
    }

    private static ObjectMapper createObjectMapper() {
        Jackson2ObjectMapperBuilder jacksonObjectMapperBuilder = new Jackson2ObjectMapperBuilder();
        jacksonObjectMapperBuilder.featuresToDisable(
                // Prevent user data snippets ending up in the logs on JsonParseExceptions
                JsonParser.Feature.INCLUDE_SOURCE_IN_LOCATION,
                // Format Dates instead of returning a long
                SerializationFeature.WRITE_DATES_AS_TIMESTAMPS,
                // Tolerate new fields
                DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES,
                // Do not normalize time zone to UTC
                DeserializationFeature.ADJUST_DATES_TO_CONTEXT_TIME_ZONE);
        ObjectMapper res = jacksonObjectMapperBuilder.build();
        res.findAndRegisterModules();
        return res;
    }

}
