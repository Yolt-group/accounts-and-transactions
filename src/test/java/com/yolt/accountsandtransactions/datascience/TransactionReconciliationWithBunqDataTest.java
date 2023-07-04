package com.yolt.accountsandtransactions.datascience;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.yolt.accountsandtransactions.TestBuilders;
import com.yolt.accountsandtransactions.inputprocessing.TransactionInsertionStrategy;
import com.yolt.accountsandtransactions.inputprocessing.TransactionReconciliationResultMetrics;
import com.yolt.accountsandtransactions.transactions.Transaction;
import lombok.SneakyThrows;
import nl.ing.lovebird.extendeddata.common.CurrencyCode;
import nl.ing.lovebird.providerdomain.ProviderTransactionDTO;
import org.junit.jupiter.api.Test;
import org.springframework.http.converter.json.Jackson2ObjectMapperBuilder;

import java.math.BigDecimal;
import java.time.Clock;
import java.time.Instant;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;

import static com.yolt.accountsandtransactions.datascience.TransactionSyncService.reconcileUpstreamTransactionsWithPersisted;
import static com.yolt.accountsandtransactions.datascience.TransactionSyncService.retrieveStoredTransactionsInSameTimeWindow;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * This tests uses real-world (anonymized) data from a Bunq user.
 */
public class TransactionReconciliationWithBunqDataTest {

    @Test
    public void given_aTransactionsIdChangesAcrossRefreshes_when_reconcileTransactions_then_weCanReconcileBasedOnAttributes() throws Throwable {
        ObjectMapper objectMapper = createObjectMapper();
        byte[] upstreamBytes = getClass().getResourceAsStream("/transactionmatching/bunq/2020-05-15T06_52_46.047_00_00.json").readAllBytes();
        List<ProviderTransactionDTO> upstream = objectMapper.readValue(upstreamBytes, new TypeReference<>() {
        });
        String provider = "doesNotMatter";
        List<Transaction> allstored = readStoredTrxs();
        List<Transaction> stored = retrieveStoredTransactionsInSameTimeWindow(provider, upstream, (d) -> allstored);
        TransactionInsertionStrategy.Instruction result = reconcileUpstreamTransactionsWithPersisted(stored, upstream, "BUNQ", Clock.systemUTC());

        assertThat(result.getMetrics()).isEqualToComparingFieldByField(TransactionReconciliationResultMetrics.builder()
                .provider("BUNQ")
                .upstreamTotal(7)
                .storedTotal(148)
                .storedMatchedByExternalId(7)
                .storedBookedNotMatched(141)
                .build());
    }

    @SneakyThrows
    private List<Transaction> readStoredTrxs() {
        String rows = new String(getClass().getResourceAsStream("/transactionmatching/bunq/dbrows.txt").readAllBytes());
        List<Transaction> res = new ArrayList<>();
        for (String row : rows.split("\n")) {
            String[] parts = row.split(",");
            String pending = parts[0].trim();
            String date = parts[1].trim();
            String transaction_id = parts[2].trim();
            String amount = parts[3].trim();
            String currency = parts[4].trim();
            String description = parts[5].trim();
            String external_id = parts[6].trim();
            String transaction_date = parts[7].trim();
            String transaction_timestamp = parts[8].trim();
            String transaction_type = parts[9].trim();
            res.add(
                    TestBuilders.createTransactionTemplate().toBuilder()
                            .id(transaction_id)
                            .date(LocalDate.parse(date))
//                            .pending() // ??
                            .externalId(external_id)
                            .amount(new BigDecimal(amount))
                            .currency(CurrencyCode.valueOf(currency))
                            .timestamp("null" .equals(transaction_timestamp)
                                    ? null
                                    : Instant.parse(transaction_timestamp.replace(" ", "T").replace("+0000", "Z")))
                            .build()
            );
        }
        return res;
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
