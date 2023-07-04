package com.yolt.accountsandtransactions.inputprocessing.matching;

import com.yolt.accountsandtransactions.transactions.Transaction;
import nl.ing.lovebird.extendeddata.transaction.ExtendedTransactionDTO;
import nl.ing.lovebird.providerdomain.ProviderTransactionDTO;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.ZonedDateTime;
import java.util.List;

import static com.yolt.accountsandtransactions.TestBuilders.createTransactionWithId;
import static com.yolt.accountsandtransactions.TestBuilders.transactionWithExternalIdAmountAndDate;
import static com.yolt.accountsandtransactions.inputprocessing.matching.AttributeSelectors.*;
import static com.yolt.accountsandtransactions.inputprocessing.matching.AttributeTransactionMatcher.Unmatched.Reason.REJECTED;
import static com.yolt.accountsandtransactions.inputprocessing.matching.AttributeTransactionMatcher.Unmatched.unprocessed;
import static com.yolt.accountsandtransactions.inputprocessing.matching.GeneralizedTransaction.toProviderGeneralized;
import static com.yolt.accountsandtransactions.inputprocessing.matching.GeneralizedTransaction.toStoredGeneralized;
import static java.util.Collections.emptyList;
import static org.assertj.core.api.Assertions.assertThat;

class AttributeSelectorTest {

    @Test
    void shouldRejectTransactionIfSelectorAttributeValueIsNull() {
        var matcher = new EqualityAttributeTransactionMatcher("test", List.of(
                AttributeSelectors.EXTERNAL_ID,
                AttributeSelectors.AMOUNT_IN_CENTS,
                // require booking-date not null
                notNull(AttributeSelectors.BOOKING_DATE)));

        var now = ZonedDateTime.now();

        List<ProviderTransactionDTO> upstreamTransactions = List.of(
                transactionWithExternalIdAmountAndDate("a", new BigDecimal("100"), now)
                        .toBuilder()
                        .extendedTransaction(ExtendedTransactionDTO.builder()
                                .bookingDate(null)
                                .build())
                        .build(),
                transactionWithExternalIdAmountAndDate("b", new BigDecimal("-200"), now)
                        .toBuilder()
                        .extendedTransaction(ExtendedTransactionDTO.builder()
                                .bookingDate(null)
                                .build())
                        .build());

        List<Transaction> storedTransactions = List.of(
                createTransactionWithId("1", transactionWithExternalIdAmountAndDate("a", new BigDecimal("100.00"), now))
                        .toBuilder()
                        .bookingDate(null)
                        .build(),
                createTransactionWithId("2", transactionWithExternalIdAmountAndDate("b", new BigDecimal("-200.00"), now))
                        .toBuilder()
                        .bookingDate(null)
                        .build());

        var matchResults = matcher.match("BANK",
                new AttributeTransactionMatcher.MatchResult(
                        emptyList(),
                        emptyList(),
                        unprocessed(toProviderGeneralized(upstreamTransactions)),
                        unprocessed(toStoredGeneralized(storedTransactions)))
        );

        assertThat(matchResults.matched).hasSize(0);

        // assert that the transactions are rejected in the upstream
        assertThat(matchResults.unmatchedUpstream).hasSize(2);
        assertThat(matchResults.unmatchedUpstream)
                .allSatisfy(unmatched -> assertThat(unmatched.reason).isEqualTo(REJECTED));

        // assert that the transactions are rejected in the storage
        assertThat(matchResults.unmatchedStored).hasSize(2);
        assertThat(matchResults.unmatchedStored)
                .allSatisfy(unmatched -> assertThat(unmatched.reason).isEqualTo(REJECTED));
    }

    @Test
    void shouldRejectTransactionIfSelectorAttributeValueIsBlank() {
        var matcher = new EqualityAttributeTransactionMatcher("test", List.of(
                AttributeSelectors.EXTERNAL_ID,
                AttributeSelectors.AMOUNT_IN_CENTS,
                // require description not blank (null or empty)
                notBlank(AttributeSelectors.DESCRIPTION)));

        var now = ZonedDateTime.now();

        List<ProviderTransactionDTO> upstreamTransactions = List.of(
                transactionWithExternalIdAmountAndDate("a", new BigDecimal("100"), now)
                        .toBuilder()
                        .description("")
                        .build(),
                transactionWithExternalIdAmountAndDate("b", new BigDecimal("-200"), now)
                        .toBuilder()
                        .description(null)
                        .build());

        List<Transaction> storedTransactions = List.of(
                createTransactionWithId("1", transactionWithExternalIdAmountAndDate("a", new BigDecimal("100.00"), now))
                        .toBuilder()
                        .description("")
                        .build(),
                createTransactionWithId("2", transactionWithExternalIdAmountAndDate("b", new BigDecimal("-200.00"), now))
                        .toBuilder()
                        .description(null)
                        .build());

        var matchResults = matcher.match("BANK",
                new AttributeTransactionMatcher.MatchResult(
                        emptyList(),
                        emptyList(),
                        unprocessed(toProviderGeneralized(upstreamTransactions)),
                        unprocessed(toStoredGeneralized(storedTransactions)))
        );

        assertThat(matchResults.matched).hasSize(0);

        // assert that the transactions are rejected in the upstream
        assertThat(matchResults.unmatchedUpstream).hasSize(2);
        assertThat(matchResults.unmatchedUpstream)
                .allSatisfy(unmatched -> assertThat(unmatched.reason).isEqualTo(REJECTED));

        // assert that the transactions are rejected in the storage
        assertThat(matchResults.unmatchedStored).hasSize(2);
        assertThat(matchResults.unmatchedStored)
                .allSatisfy(unmatched -> assertThat(unmatched.reason).isEqualTo(REJECTED));
    }


    @Test
    void shouldRejectTransactionIfSelectorAttributeValueIsPlaceholderValue() {
        var matcher = new EqualityAttributeTransactionMatcher("test", List.of(
                notPlaceholder(AttributeSelectors.EXTERNAL_ID),
                AttributeSelectors.AMOUNT_IN_CENTS
        ));

        var now = ZonedDateTime.now();

        List<ProviderTransactionDTO> upstreamTransactions = List.of(
                transactionWithExternalIdAmountAndDate("Not Provided", new BigDecimal("100"), now),
                transactionWithExternalIdAmountAndDate("Not Provided", new BigDecimal("-200"), now));

        List<Transaction> storedTransactions = List.of(
                createTransactionWithId("1", transactionWithExternalIdAmountAndDate("Not Provided", new BigDecimal("100.00"), now)),
                createTransactionWithId("2", transactionWithExternalIdAmountAndDate("Not Provided", new BigDecimal("-200.00"), now)));

        var matchResults = matcher.match("BANK",
                new AttributeTransactionMatcher.MatchResult(
                        emptyList(),
                        emptyList(),
                        unprocessed(toProviderGeneralized(upstreamTransactions)),
                        unprocessed(toStoredGeneralized(storedTransactions)))
        );

        assertThat(matchResults.matched).hasSize(0);

        // assert that the transactions are rejected in the upstream
        assertThat(matchResults.unmatchedUpstream).hasSize(2);
        assertThat(matchResults.unmatchedUpstream)
                .allSatisfy(unmatched -> assertThat(unmatched.reason).isEqualTo(REJECTED));

        // assert that the transactions are rejected in the storage
        assertThat(matchResults.unmatchedStored).hasSize(2);
        assertThat(matchResults.unmatchedStored)
                .allSatisfy(unmatched -> assertThat(unmatched.reason).isEqualTo(REJECTED));
    }

}