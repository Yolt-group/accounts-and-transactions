package com.yolt.accountsandtransactions.inputprocessing.matching;

import com.yolt.accountsandtransactions.inputprocessing.matching.AttributeTransactionMatcher.MatchResult;
import com.yolt.accountsandtransactions.transactions.Transaction;
import nl.ing.lovebird.providerdomain.ProviderTransactionDTO;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.ZonedDateTime;
import java.util.List;

import static com.yolt.accountsandtransactions.TestBuilders.createTransactionWithId;
import static com.yolt.accountsandtransactions.TestBuilders.transactionWithExternalIdAmountAndDate;
import static com.yolt.accountsandtransactions.inputprocessing.matching.AttributeTransactionMatcher.Unmatched.Reason.DUPLICATE;
import static com.yolt.accountsandtransactions.inputprocessing.matching.AttributeTransactionMatcher.Unmatched.Reason.PEERLESS;
import static com.yolt.accountsandtransactions.inputprocessing.matching.AttributeTransactionMatcher.Unmatched.unprocessed;
import static com.yolt.accountsandtransactions.inputprocessing.matching.GeneralizedTransaction.toProviderGeneralized;
import static com.yolt.accountsandtransactions.inputprocessing.matching.GeneralizedTransaction.toStoredGeneralized;
import static java.util.Collections.emptyList;
import static org.assertj.core.api.Assertions.assertThat;

class EqualityAttributeTransactionMatcherTest {

    @Test
    void shouldReturnEmptyMatchResultOnEmptyInput() {
        var matcher = new EqualityAttributeTransactionMatcher("test", List.of(
                AttributeSelectors.EXTERNAL_ID,
                AttributeSelectors.AMOUNT_IN_CENTS
        ));

        var matchResults = matcher.match("BANK",
                new AttributeTransactionMatcher.MatchResult(emptyList(), emptyList(), emptyList(), emptyList()));

        assertThat(matchResults.matched).isEmpty();
        assertThat(matchResults.unmatchedUpstream).isEmpty();
        assertThat(matchResults.unmatchedStored).isEmpty();
    }

    @Test
    void shouldReturnAllUnmatchedUpstreamOnEmptyStored() {
        var matcher = new EqualityAttributeTransactionMatcher("test", List.of(
                AttributeSelectors.EXTERNAL_ID,
                AttributeSelectors.AMOUNT_IN_CENTS
        ));

        var now = ZonedDateTime.now();

        List<ProviderTransactionDTO> upstreamTransactions = List.of(
                transactionWithExternalIdAmountAndDate("a", new BigDecimal("100"), now),
                transactionWithExternalIdAmountAndDate("b", new BigDecimal("200"), now),
                transactionWithExternalIdAmountAndDate("c", new BigDecimal("300"), now),
                transactionWithExternalIdAmountAndDate("d", new BigDecimal("400"), now));

        var matchResults = matcher.match("BANK",
                new AttributeTransactionMatcher.MatchResult(emptyList(), emptyList(), unprocessed(toProviderGeneralized(upstreamTransactions)), emptyList()));

        assertThat(matchResults.matched).isEmpty();
        assertThat(matchResults.unmatchedUpstream).hasSize(4);
        assertThat(matchResults.unmatchedStored).isEmpty();
    }

    @Test
    void shouldReturnAllUnmatchedStoredOnEmptyUpstream() {
        var matcher = new EqualityAttributeTransactionMatcher("test", List.of(
                AttributeSelectors.EXTERNAL_ID,
                AttributeSelectors.AMOUNT_IN_CENTS
        ));

        var now = ZonedDateTime.now();

        List<Transaction> storedTransactions = List.of(
                createTransactionWithId("1", transactionWithExternalIdAmountAndDate("a", new BigDecimal("100.00"), now)),
                createTransactionWithId("2", transactionWithExternalIdAmountAndDate("b", new BigDecimal("200.00"), now)),
                createTransactionWithId("3", transactionWithExternalIdAmountAndDate("c", new BigDecimal("300.00"), now)),
                createTransactionWithId("4", transactionWithExternalIdAmountAndDate("d", new BigDecimal("400.00"), now)));

        var matchResults = matcher.match("BANK",
                new MatchResult(emptyList(), emptyList(), emptyList(), unprocessed(toStoredGeneralized(storedTransactions))));

        assertThat(matchResults.matched).isEmpty();
        assertThat(matchResults.unmatchedUpstream).isEmpty();
        assertThat(matchResults.unmatchedStored).hasSize(4);
        assertThat(matchResults.unmatchedStored)
                .allSatisfy(unmatched -> assertThat(unmatched.reason).isEqualTo(PEERLESS));
    }

    @Test
    void shouldReturnAllMatchedWhenPerfectMatchOnAttribute() {
        var matcher = new EqualityAttributeTransactionMatcher("test", List.of(
                AttributeSelectors.EXTERNAL_ID,
                AttributeSelectors.AMOUNT_IN_CENTS
        ));

        var now = ZonedDateTime.now();

        List<ProviderTransactionDTO> upstreamTransactions = List.of(
                transactionWithExternalIdAmountAndDate("a", new BigDecimal("100"), now),
                transactionWithExternalIdAmountAndDate("b", new BigDecimal("200"), now),
                transactionWithExternalIdAmountAndDate("c", new BigDecimal("-300"), now),
                transactionWithExternalIdAmountAndDate("d", new BigDecimal("-400"), now));

        List<Transaction> storedTransactions = List.of(
                createTransactionWithId("1", transactionWithExternalIdAmountAndDate("a", new BigDecimal("100.00"), now)),
                createTransactionWithId("2", transactionWithExternalIdAmountAndDate("b", new BigDecimal("200.00"), now)),
                createTransactionWithId("3", transactionWithExternalIdAmountAndDate("c", new BigDecimal("-300.00"), now)),
                createTransactionWithId("4", transactionWithExternalIdAmountAndDate("d", new BigDecimal("-400.00"), now)));

        var matchResults = matcher.match("BANK",
                new MatchResult(
                        emptyList(),
                        emptyList(),
                        unprocessed(toProviderGeneralized(upstreamTransactions)),
                        unprocessed(toStoredGeneralized(storedTransactions)))
        );

        assertThat(matchResults.matched).hasSize(4);
        assertThat(matchResults.unmatchedUpstream).isEmpty();
        assertThat(matchResults.unmatchedStored).isEmpty();
    }

    @Test
    void shouldReturnAllUnmatchedWhenExternalIdDoesNotMatch() {
        var matcher = new EqualityAttributeTransactionMatcher("test", List.of(
                AttributeSelectors.EXTERNAL_ID,
                AttributeSelectors.AMOUNT_IN_CENTS
        ));

        var now = ZonedDateTime.now();

        List<ProviderTransactionDTO> upstreamTransactions = List.of(
                transactionWithExternalIdAmountAndDate("a", new BigDecimal("100"), now),
                transactionWithExternalIdAmountAndDate("b", new BigDecimal("200"), now),
                transactionWithExternalIdAmountAndDate("c", new BigDecimal("-300"), now),
                transactionWithExternalIdAmountAndDate("d", new BigDecimal("-400"), now));

        List<Transaction> storedTransactions = List.of(
                createTransactionWithId("1", transactionWithExternalIdAmountAndDate("e", new BigDecimal("100.00"), now)),
                createTransactionWithId("2", transactionWithExternalIdAmountAndDate("f", new BigDecimal("200.00"), now)),
                createTransactionWithId("3", transactionWithExternalIdAmountAndDate("g", new BigDecimal("-300.00"), now)),
                createTransactionWithId("4", transactionWithExternalIdAmountAndDate("h", new BigDecimal("-400.00"), now)));

        var matchResults = matcher.match("BANK",
                new MatchResult(
                        emptyList(),
                        emptyList(),
                        unprocessed(toProviderGeneralized(upstreamTransactions)),
                        unprocessed(toStoredGeneralized(storedTransactions)))
        );

        assertThat(matchResults.matched).hasSize(0);

        assertThat(matchResults.unmatchedUpstream).hasSize(4);
        assertThat(matchResults.unmatchedUpstream)
                .allSatisfy(unmatched -> assertThat(unmatched.reason).isEqualTo(PEERLESS));

        assertThat(matchResults.unmatchedStored).hasSize(4);
        assertThat(matchResults.unmatchedStored)
                .allSatisfy(unmatched -> assertThat(unmatched.reason).isEqualTo(PEERLESS));
    }

    @Test
    void shouldReturnAllUnmatchedWhenAmountDoesNotMatch() {
        var matcher = new EqualityAttributeTransactionMatcher("test", List.of(
                AttributeSelectors.EXTERNAL_ID,
                AttributeSelectors.AMOUNT_IN_CENTS
        ));

        var now = ZonedDateTime.now();

        List<ProviderTransactionDTO> upstreamTransactions = List.of(
                transactionWithExternalIdAmountAndDate("a", new BigDecimal("100"), now),
                transactionWithExternalIdAmountAndDate("b", new BigDecimal("200"), now),
                transactionWithExternalIdAmountAndDate("c", new BigDecimal("300"), now),
                transactionWithExternalIdAmountAndDate("d", new BigDecimal("400"), now));

        List<Transaction> storedTransactions = List.of(
                createTransactionWithId("1", transactionWithExternalIdAmountAndDate("a", new BigDecimal("500.00"), now)),
                createTransactionWithId("2", transactionWithExternalIdAmountAndDate("b", new BigDecimal("600.00"), now)),
                createTransactionWithId("3", transactionWithExternalIdAmountAndDate("c", new BigDecimal("700.00"), now)),
                createTransactionWithId("4", transactionWithExternalIdAmountAndDate("d", new BigDecimal("800.00"), now)));

        var matchResults = matcher.match("BANK",
                new MatchResult(
                        emptyList(),
                        emptyList(),
                        unprocessed(toProviderGeneralized(upstreamTransactions)),
                        unprocessed(toStoredGeneralized(storedTransactions)))
        );

        assertThat(matchResults.matched).hasSize(0);

        assertThat(matchResults.unmatchedUpstream).hasSize(4);
        assertThat(matchResults.unmatchedUpstream)
                .allSatisfy(unmatched -> assertThat(unmatched.reason).isEqualTo(PEERLESS));

        assertThat(matchResults.unmatchedStored).hasSize(4);
        assertThat(matchResults.unmatchedStored)
                .allSatisfy(unmatched -> assertThat(unmatched.reason).isEqualTo(PEERLESS));
    }

    @Test
    void shouldReturnAllUnmatchedWhenDuplicate() {
        var matcher = new EqualityAttributeTransactionMatcher("test", List.of(
                AttributeSelectors.EXTERNAL_ID,
                AttributeSelectors.AMOUNT_IN_CENTS
        ));

        var now = ZonedDateTime.now();

        List<ProviderTransactionDTO> upstreamTransactions = List.of(
                transactionWithExternalIdAmountAndDate("a", new BigDecimal("100"), now),
                transactionWithExternalIdAmountAndDate("b", new BigDecimal("200"), now));

        List<Transaction> storedTransactions = List.of(
                createTransactionWithId("1", transactionWithExternalIdAmountAndDate("a", new BigDecimal("100.00"), now)),
                createTransactionWithId("2", transactionWithExternalIdAmountAndDate("a", new BigDecimal("100.00"), now)));

        var matchResults = matcher.match("BANK",
                new MatchResult(
                        emptyList(),
                        emptyList(),
                        unprocessed(toProviderGeneralized(upstreamTransactions)),
                        unprocessed(toStoredGeneralized(storedTransactions)))
        );

        assertThat(matchResults.matched).hasSize(0);

        assertThat(matchResults.unmatchedUpstream).hasSize(2);
        assertThat(matchResults.unmatchedUpstream)
                .allSatisfy(unmatched -> assertThat(unmatched.reason).isEqualTo(PEERLESS));

        assertThat(matchResults.unmatchedStored).hasSize(2);
        assertThat(matchResults.unmatchedStored)
                .allSatisfy(unmatched -> assertThat(unmatched.reason).isEqualTo(DUPLICATE));
    }

    @Test
    void minSelector() {

        var matcher = new EqualityAttributeTransactionMatcher("test", List.of(
                AttributeSelectors.EXTERNAL_ID,
                AttributeSelectors.AMOUNT_IN_CENTS
        ));

        var modified = matcher.withoutSelector("noExternalId", AttributeSelectors.EXTERNAL_ID);
        assertThat(modified.getSelectors())
                .containsExactly(AttributeSelectors.AMOUNT_IN_CENTS);

    }
}