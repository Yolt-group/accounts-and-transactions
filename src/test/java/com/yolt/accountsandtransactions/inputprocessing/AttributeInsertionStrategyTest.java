package com.yolt.accountsandtransactions.inputprocessing;

import com.yolt.accountsandtransactions.inputprocessing.TransactionInsertionStrategy.Mode;
import com.yolt.accountsandtransactions.inputprocessing.matching.AttributeSelectors;
import com.yolt.accountsandtransactions.inputprocessing.matching.AttributeTransactionMatcher;
import com.yolt.accountsandtransactions.inputprocessing.matching.EqualityAttributeTransactionMatcher;
import com.yolt.accountsandtransactions.inputprocessing.matching.ProviderConfiguration;
import com.yolt.accountsandtransactions.transactions.Transaction;
import com.yolt.accountsandtransactions.transactions.TransactionRepository;
import lombok.NonNull;
import nl.ing.lovebird.clienttokens.ClientUserToken;
import nl.ing.lovebird.clienttokens.test.TestJwtClaims;
import nl.ing.lovebird.extendeddata.common.CurrencyCode;
import nl.ing.lovebird.extendeddata.transaction.ExtendedTransactionDTO;
import nl.ing.lovebird.extendeddata.transaction.TransactionStatus;
import nl.ing.lovebird.providerdomain.ProviderTransactionDTO;
import nl.ing.lovebird.providerdomain.ProviderTransactionType;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.math.BigDecimal;
import java.time.*;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.yolt.accountsandtransactions.Predef.concat;
import static com.yolt.accountsandtransactions.TestBuilders.createTransactionWithId;
import static com.yolt.accountsandtransactions.TestBuilders.transactionWithExternalIdAmountAndDate;
import static com.yolt.accountsandtransactions.inputprocessing.AttributeInsertionStrategy.reconcileTransactionsOnAttributes;
import static com.yolt.accountsandtransactions.inputprocessing.matching.AttributeSelectors.notNull;
import static com.yolt.accountsandtransactions.inputprocessing.matching.AttributeTransactionMatcher.Unmatched.Reason.*;
import static com.yolt.accountsandtransactions.inputprocessing.matching.GeneralizedTransaction.toProviderGeneralized;
import static com.yolt.accountsandtransactions.inputprocessing.matching.GeneralizedTransaction.toStoredGeneralized;
import static com.yolt.accountsandtransactions.inputprocessing.matching.Matchers.BOOKING_DATE_AMOUNT_STRICT;
import static com.yolt.accountsandtransactions.inputprocessing.matching.Matchers.EXTERNAL_ID_AMOUNT_STRICT;
import static java.util.Collections.emptyList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class AttributeInsertionStrategyTest {

    private final static TransactionIdProvider<UUID> TRANSACTION_ID_PROVIDER = providerTransaction -> UUID.randomUUID();
    private final static ClientUserToken clientUserToken = new ClientUserToken(null, TestJwtClaims.createClientUserClaims("junit", UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID()));

    @Test
    void selfTest() {
        List<Transaction> transactions = List.of(
                createTransactionWithId("1", transactionWithExternalIdAmountAndDate("a", new BigDecimal("100.00"), Instant.EPOCH.atZone(ZoneOffset.UTC))),
                createTransactionWithId("2", transactionWithExternalIdAmountAndDate("b", new BigDecimal("200.00"), Instant.EPOCH.atZone(ZoneOffset.UTC))),
                createTransactionWithId("3", transactionWithExternalIdAmountAndDate("c", new BigDecimal("300.00"), Instant.EPOCH.atZone(ZoneOffset.UTC))),
                createTransactionWithId("4", transactionWithExternalIdAmountAndDate("d", new BigDecimal("400.00"), Instant.EPOCH.atZone(ZoneOffset.UTC))),
                createTransactionWithId("5", transactionWithExternalIdAmountAndDate("e", new BigDecimal("500.00"), Instant.EPOCH.atZone(ZoneOffset.UTC)))
        );

        var matchResult = reconcileTransactionsOnAttributes(
                ProviderConfiguration.builder()
                        .provider("TEST")
                        .syncWindowSelector(new UnboundedSyncWindowSelector())
                        .matchers(List.of(EXTERNAL_ID_AMOUNT_STRICT, BOOKING_DATE_AMOUNT_STRICT))
                        .build(),
                toProviderGeneralized(asProviderTransaction(transactions)),
                toStoredGeneralized(transactions));

        var lastMatchResult = matchResult.last().orElseThrow(AssertionError::new);

        assertThat(lastMatchResult.matched).hasSize(5);
        assertThat(lastMatchResult.multiMatched).hasSize(0);
        assertThat(lastMatchResult.unmatchedUpstream).hasSize(0);
        assertThat(lastMatchResult.unmatchedStored).hasSize(0);
    }

    @Test
    void given_existingExternalIds_when_reconciling_then_transactionsAreMatchedAndUpdatedAndNothingWillBeDeleted() {
        List<Transaction> storedTransactions = List.of(
                createTransactionWithId("1", transactionWithExternalIdAmountAndDate("a", new BigDecimal("100.00"), Instant.EPOCH.atZone(ZoneOffset.UTC))),
                createTransactionWithId("2", transactionWithExternalIdAmountAndDate("b", new BigDecimal("200.00"), Instant.EPOCH.atZone(ZoneOffset.UTC)))
        );

        var newUpstreamTransactions = List.of(
                transactionWithExternalIdAmountAndDate("c", new BigDecimal("300.00"), Instant.EPOCH.atZone(ZoneOffset.UTC)),
                transactionWithExternalIdAmountAndDate("d", new BigDecimal("400.00"), Instant.EPOCH.atZone(ZoneOffset.UTC)),
                transactionWithExternalIdAmountAndDate("e", new BigDecimal("500.00"), Instant.EPOCH.atZone(ZoneOffset.UTC)));

        var updatedUpstreamTransactions = List.of(
                transactionWithExternalIdAmountAndDate("a", new BigDecimal("100.00"), Instant.EPOCH.atZone(ZoneOffset.UTC)),
                transactionWithExternalIdAmountAndDate("b", new BigDecimal("200.00"), Instant.EPOCH.atZone(ZoneOffset.UTC))
        );

        var matchResult = reconcileTransactionsOnAttributes(
                ProviderConfiguration.builder()
                        .provider("TEST")
                        .syncWindowSelector(new UnboundedSyncWindowSelector())
                        .matchers(List.of(EXTERNAL_ID_AMOUNT_STRICT, BOOKING_DATE_AMOUNT_STRICT))
                        .build(),
                toProviderGeneralized(concat(newUpstreamTransactions, updatedUpstreamTransactions)),
                toStoredGeneralized(storedTransactions));

        var lastMatchResult = matchResult.last().orElseThrow(AssertionError::new);

        assertThat(lastMatchResult.matched).hasSize(2);
        assertThat(lastMatchResult.unmatchedUpstream).hasSize(3);
        assertThat(lastMatchResult.unmatchedStored).hasSize(0);
    }

    @ParameterizedTest
    @MethodSource("provideEqualAmounts")
    void given_scaledAmount_then_shouldMatch(BigDecimal upstreamAmount, BigDecimal storedAmount) {

        List<Transaction> transactionWithoutScale = List.of(
                createTransactionWithId("1", transactionWithExternalIdAmountAndDate("a", upstreamAmount, Instant.EPOCH.atZone(ZoneOffset.UTC))));

        var transactionWithScale = List.of(
                transactionWithExternalIdAmountAndDate("a", storedAmount, Instant.EPOCH.atZone(ZoneOffset.UTC)));

        var matchResult = reconcileTransactionsOnAttributes(
                ProviderConfiguration.builder()
                        .provider("TEST")
                        .syncWindowSelector(new UnboundedSyncWindowSelector())
                        .matchers(List.of(EXTERNAL_ID_AMOUNT_STRICT, BOOKING_DATE_AMOUNT_STRICT))
                        .build(),
                toProviderGeneralized(transactionWithScale),
                toStoredGeneralized(transactionWithoutScale));

        var lastMatchResult = matchResult.last().orElseThrow(AssertionError::new);

        assertThat(lastMatchResult.matched).hasSize(1);
        assertThat(lastMatchResult.unmatchedUpstream).hasSize(0);
        assertThat(lastMatchResult.unmatchedUpstream).hasSize(0);
    }

    private static Stream<Arguments> provideEqualAmounts() {
        return Stream.of(
                Arguments.of(new BigDecimal("100"), new BigDecimal("100")),
                Arguments.of(new BigDecimal("-100"), new BigDecimal("-100")),
                Arguments.of(new BigDecimal("100.1"), new BigDecimal("100.10")),
                Arguments.of(new BigDecimal("100.01"), new BigDecimal("100.010")),
                Arguments.of(new BigDecimal("100.001"), new BigDecimal("100.0010")),
                Arguments.of(new BigDecimal("-100.1"), new BigDecimal("-100.10")),
                Arguments.of(new BigDecimal("-100.01"), new BigDecimal("-100.010")),
                Arguments.of(new BigDecimal("-100.001"), new BigDecimal("-100.0010")));
    }

    @Test
    void given_anUpstreamAndStoredTransaction_when_theAmountSignIsDifferent_then_noMatch() {
        List<Transaction> transactionWithoutScale = List.of(
                createTransactionWithId("1", transactionWithExternalIdAmountAndDate("a", new BigDecimal("-100"), Instant.EPOCH.atZone(ZoneOffset.UTC))));

        var transactionWithScale = List.of(
                transactionWithExternalIdAmountAndDate("a", new BigDecimal("100"), Instant.EPOCH.atZone(ZoneOffset.UTC)));

        var matchResult = reconcileTransactionsOnAttributes(
                ProviderConfiguration.builder()
                        .provider("TEST")
                        .syncWindowSelector(new UnboundedSyncWindowSelector())
                        .matchers(List.of(EXTERNAL_ID_AMOUNT_STRICT))
                        .build(),
                toProviderGeneralized(transactionWithScale),
                toStoredGeneralized(transactionWithoutScale));

        var lastMatchResult = matchResult.last().orElseThrow(AssertionError::new);

        assertThat(lastMatchResult.matched).hasSize(0);
        assertThat(lastMatchResult.unmatchedUpstream).hasSize(1);
        assertThat(lastMatchResult.unmatchedUpstream).hasSize(1);
    }

    public static List<ProviderTransactionDTO> asProviderTransaction(final List<Transaction> transactions) {
        return transactions.stream()
                .map(transaction -> ProviderTransactionDTO.builder()
                        .dateTime(transaction.getTimestamp().atZone(ZoneOffset.UTC))
                        .type(transaction.getAmount().signum() == -1 ? ProviderTransactionType.DEBIT : ProviderTransactionType.CREDIT)
                        .status(transaction.getStatus())
                        .amount(transaction.getAmount())
                        .extendedTransaction(ExtendedTransactionDTO.builder()
                                .bookingDate(Optional.ofNullable(transaction.getBookingDate())
                                        .map(localDate -> localDate.atStartOfDay(ZoneId.of("UTC")))
                                        .orElse(null))
                                .build())
                        .externalId(transaction.getExternalId())
                        .description(transaction.getDescription())
                        .build())
                .collect(Collectors.toList());
    }

    @Test
    void given_upstreamAndStoredAreEqual_then_allInstructionsAreEmpty() {

        var upstreamTransactions = List.of(
                transactionWithExternalIdAmountAndDate("a", new BigDecimal("100.00"), Instant.EPOCH.atZone(ZoneOffset.UTC)),
                transactionWithExternalIdAmountAndDate("b", new BigDecimal("200.00"), Instant.EPOCH.atZone(ZoneOffset.UTC)),
                transactionWithExternalIdAmountAndDate("c", new BigDecimal("300.00"), Instant.EPOCH.atZone(ZoneOffset.UTC)),
                transactionWithExternalIdAmountAndDate("d", new BigDecimal("400.00"), Instant.EPOCH.atZone(ZoneOffset.UTC)),
                transactionWithExternalIdAmountAndDate("e", new BigDecimal("500.00"), Instant.EPOCH.atZone(ZoneOffset.UTC))
        );

        var storedTransactions = upstreamTransactions.stream()
                .map(providerTransactionDTO -> createTransactionWithId(UUID.randomUUID().toString(), providerTransactionDTO))
                .collect(Collectors.toList());

        var sut = createAttributeInsertionStrategy(storedTransactions, List.of(EXTERNAL_ID_AMOUNT_STRICT));
        var instruction = sut.determineTransactionPersistenceInstruction(upstreamTransactions, clientUserToken, UUID.randomUUID(), "Dummy", CurrencyCode.EUR);

        assertThat(instruction.getTransactionsToDelete()).isEmpty();
        assertThat(instruction.getTransactionsToUpdate()).isEmpty();
        assertThat(instruction.getTransactionsToInsert()).isEmpty();
        assertThat(instruction.getTransactionsToIgnore()).isEmpty();
        assertThat(instruction.getOldestTransactionChangeDate()).isEmpty();
    }

    @Test
    void given_aNewUpstreamTransaction_when_thereAreNoStoredTransactions_then_fillTypeIsRegular() {

        var now = ZonedDateTime.now();

        List<ProviderTransactionDTO> upstreamTransactions = List.of(
                transactionWithExternalIdAmountAndDate("a", new BigDecimal("100.00"), now));

        var sut = createAttributeInsertionStrategy(emptyList(), List.of(EXTERNAL_ID_AMOUNT_STRICT));
        var instruction = sut.determineTransactionPersistenceInstruction(upstreamTransactions, clientUserToken, UUID.randomUUID(), "Dummy", CurrencyCode.EUR);

        assertThat(instruction.getTransactionsToInsert()).hasSize(1);
        assertThat(instruction.getTransactionsToInsert())
                .allMatch(providerTransactionWithId -> providerTransactionWithId.getFillType() == Transaction.FillType.REGULAR);
        assertThat(instruction.getOldestTransactionChangeDate()).contains(now.toLocalDate());

        assertThat(instruction.getTransactionsToDelete()).isEmpty();
        assertThat(instruction.getTransactionsToUpdate()).isEmpty();
        assertThat(instruction.getTransactionsToIgnore()).isEmpty();
    }

    @Test
    void given_aNewUpstreamTransaction_when_theUpstreamTransactionIsOlderThen7DaysGivenTheMostRecentlyStoredTransaction_then_fillTypeIsBackFilled() {

        var now = ZonedDateTime.now();

        List<ProviderTransactionDTO> upstreamTransactions = List.of(
                transactionWithExternalIdAmountAndDate("a", new BigDecimal("200.00"), now),
                transactionWithExternalIdAmountAndDate("b", new BigDecimal("400.00"), now.minus(7, ChronoUnit.DAYS)));

        List<Transaction> storedTransactions = List.of(
                createTransactionWithId("1", transactionWithExternalIdAmountAndDate("a", new BigDecimal("200.00"), now)));

        var sut = createAttributeInsertionStrategy(storedTransactions, List.of(EXTERNAL_ID_AMOUNT_STRICT));
        var instruction = sut.determineTransactionPersistenceInstruction(upstreamTransactions, clientUserToken, UUID.randomUUID(), "Dummy", CurrencyCode.EUR);

        assertThat(instruction.getTransactionsToInsert()).hasSize(1);
        assertThat(instruction.getTransactionsToInsert())
                .allMatch(providerTransactionWithId -> providerTransactionWithId.getFillType() == Transaction.FillType.BACKFILLED);
        assertThat(instruction.getOldestTransactionChangeDate()).isEmpty();

        assertThat(instruction.getTransactionsToDelete()).isEmpty();
        assertThat(instruction.getTransactionsToUpdate()).isEmpty();
        assertThat(instruction.getTransactionsToIgnore()).isEmpty();
    }

    @Test
    void given_aNewUpstreamTransaction_when_theUpstreamTransactionIsNewerThen7DaysGivenTheMostRecentlyStoredTransaction_then_fillTypeIsRegular() {

        var now = ZonedDateTime.now();

        List<ProviderTransactionDTO> upstreamTransactions = List.of(
                transactionWithExternalIdAmountAndDate("a", new BigDecimal("200.00"), now),
                transactionWithExternalIdAmountAndDate("b", new BigDecimal("400.00"), now.minus(6, ChronoUnit.DAYS)));

        List<Transaction> storedTransactions = List.of(
                createTransactionWithId("1", transactionWithExternalIdAmountAndDate("a", new BigDecimal("200.00"), now)));

        var sut = createAttributeInsertionStrategy(storedTransactions, List.of(EXTERNAL_ID_AMOUNT_STRICT));
        var instruction = sut.determineTransactionPersistenceInstruction(upstreamTransactions, clientUserToken, UUID.randomUUID(), "Dummy", CurrencyCode.EUR);

        assertThat(instruction.getTransactionsToInsert()).hasSize(1);
        assertThat(instruction.getTransactionsToInsert())
                .allMatch(providerTransactionWithId -> providerTransactionWithId.getFillType() == Transaction.FillType.REGULAR);
        assertThat(instruction.getOldestTransactionChangeDate()).contains(now.minus(6, ChronoUnit.DAYS).toLocalDate());

        assertThat(instruction.getTransactionsToDelete()).isEmpty();
        assertThat(instruction.getTransactionsToUpdate()).isEmpty();
        assertThat(instruction.getTransactionsToIgnore()).isEmpty();
    }

    @Test
    void give_upstreamTransactions_when_noStoredTransactions_then_InsertAll() {

        var now = ZonedDateTime.parse("2019-04-01T16:24:11.252+02:00[Europe/Amsterdam]");

        List<ProviderTransactionDTO> upstreamTransactions = List.of(
                transactionWithExternalIdAmountAndDate("a", new BigDecimal("100.00"), now.minusWeeks(1)),
                transactionWithExternalIdAmountAndDate("b", new BigDecimal("100.00"), now.minusWeeks(2)),
                transactionWithExternalIdAmountAndDate("c", new BigDecimal("100.00"), now.minusWeeks(3))
        );

        var sut = createAttributeInsertionStrategy(emptyList(), List.of(EXTERNAL_ID_AMOUNT_STRICT));
        var instruction = sut.determineTransactionPersistenceInstruction(upstreamTransactions, clientUserToken, UUID.randomUUID(), "Dummy", CurrencyCode.EUR);

        assertThat(instruction.getTransactionsToInsert()).hasSize(3);
        assertThat(instruction.getTransactionsToInsert())
                .allMatch(providerTransactionWithId -> providerTransactionWithId.getFillType() == Transaction.FillType.REGULAR);
        assertThat(instruction.getOldestTransactionChangeDate()).contains(LocalDate.parse("2019-03-11"));

        assertThat(instruction.getTransactionsToDelete()).isEmpty();
        assertThat(instruction.getTransactionsToUpdate()).isEmpty();
        assertThat(instruction.getTransactionsToIgnore()).isEmpty();
    }


    @Test
    void given_rejectedOrDuplicateMatchesInUpstream_when_matching_then_throwIllegalStateException() {

        var now = ZonedDateTime.now();
        List<ProviderTransactionDTO> upstreamTransactions = List.of(

                //match
                transactionWithExternalIdAmountAndDate("a", new BigDecimal("200.00"), now),

                // duplicate
                transactionWithExternalIdAmountAndDate("a", new BigDecimal("100.00"), now),
                transactionWithExternalIdAmountAndDate("a", new BigDecimal("100.00"), now),

                // rejected
                transactionWithExternalIdAmountAndDate(null, new BigDecimal("100.00"), now),
                transactionWithExternalIdAmountAndDate("", new BigDecimal("100.00"), now),

                // new
                transactionWithExternalIdAmountAndDate("new", new BigDecimal("-100.00"), now)
        );

        List<Transaction> storedTransactions = List.of(
                createTransactionWithId("1", transactionWithExternalIdAmountAndDate("a", new BigDecimal("200.00"), now)));

        var sut = createAttributeInsertionStrategy(storedTransactions, List.of(EXTERNAL_ID_AMOUNT_STRICT));

        assertThatThrownBy(() -> sut.determineTransactionPersistenceInstruction(upstreamTransactions, clientUserToken, UUID.randomUUID(), "Dummy", CurrencyCode.EUR))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageStartingWith("""
                        Mode: TEST
                        Fatal: Found 2 rejected and/or 2 duplicate transaction(s) in the upstream (%s).
                        Processing of this transaction set has been interrupted and all transactions will be dropped.
                        A modification to the attribute matching is required. Until the rejected/ duplicate
                        transactions are removed, this account can no longer be updated as the matching would
                        constantly fail.
                        """.formatted("DUMMY"));
    }

    @Test
    void given_DuplicateMatchesInUpstream_when_matching_then_throwIllegalStateException() {

        var now = ZonedDateTime.parse("2019-04-01T16:24:11.252+02:00[Europe/Amsterdam]");

        List<ProviderTransactionDTO> upstreamTransactions = List.of(
                // match
                transactionWithExternalIdAmountAndDate("a", new BigDecimal("200.00"), now.minus(6, ChronoUnit.DAYS)),

                // duplicate
                transactionWithExternalIdAmountAndDate("a", new BigDecimal("100.00"), now.minus(6, ChronoUnit.DAYS)),
                transactionWithExternalIdAmountAndDate("a", new BigDecimal("100.00"), now.minus(6, ChronoUnit.DAYS)),

                // rejected
                transactionWithExternalIdAmountAndDate(null, new BigDecimal("100.00"), now.minus(6, ChronoUnit.DAYS)),
                transactionWithExternalIdAmountAndDate("", new BigDecimal("100.00"), now.minus(6, ChronoUnit.DAYS)),

                // new
                transactionWithExternalIdAmountAndDate("new", new BigDecimal("-100.00"), now.minus(6, ChronoUnit.DAYS))
        );

        List<Transaction> storedTransactions = List.of(
                createTransactionWithId("1", transactionWithExternalIdAmountAndDate("a", new BigDecimal("200.00"), now)));

        var sut = createAttributeInsertionStrategy(storedTransactions, List.of(EXTERNAL_ID_AMOUNT_STRICT));

        assertThatThrownBy(() -> sut.determineTransactionPersistenceInstruction(upstreamTransactions, clientUserToken, UUID.randomUUID(), "Dummy", CurrencyCode.EUR))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageStartingWith("""
                        Mode: TEST
                        Fatal: Found 2 rejected and/or 2 duplicate transaction(s) in the upstream (DUMMY).
                        Processing of this transaction set has been interrupted and all transactions will be dropped.
                        A modification to the attribute matching is required. Until the rejected/ duplicate
                        transactions are removed, this account can no longer be updated as the matching would
                        constantly fail.
                                                
                        Rejected transactions:
                        |transaction-id                      |external-id                     |date      |booking-date|timestamp               |reason      |matcher                         |n-attr|
                        |n/a                                 |n/a                             |2019-03-26|2019-03-26|2019-03-26T15:24:11.252Z|REJECTED    |ExternalIdAmountStrict          |2     |
                        |n/a                                 |                                |2019-03-26|2019-03-26|2019-03-26T15:24:11.252Z|REJECTED    |ExternalIdAmountStrict          |2     |
                                                
                        Duplicate transactions:
                        |transaction-id                      |external-id                     |date      |booking-date|timestamp               |reason      |matcher                         |n-attr|
                        |n/a                                 |a                               |2019-03-26|2019-03-26|2019-03-26T15:24:11.252Z|DUPLICATE   |ExternalIdAmountStrict          |2     |
                        |n/a                                 |a                               |2019-03-26|2019-03-26|2019-03-26T15:24:11.252Z|DUPLICATE   |ExternalIdAmountStrict          |2     |
                                                
                        Consult the match report for detailed information about the matching output.
                        """);
    }

    @Test
    void given_DuplicateMatchesInStorage_when_matching_then_throwIllegalStateException() {
        var now = ZonedDateTime.parse("2019-04-01T16:24:11.252+02:00[Europe/Amsterdam]");

        List<ProviderTransactionDTO> upstreamTransactions = List.of(
                transactionWithExternalIdAmountAndDate("a", new BigDecimal("100.00"), now));

        List<Transaction> storedDuplicates = List.of(
                createTransactionWithId("1", transactionWithExternalIdAmountAndDate("a", new BigDecimal("100.00"), now)),
                createTransactionWithId("1", transactionWithExternalIdAmountAndDate("a", new BigDecimal("100.00"), now)));

        var sut = createAttributeInsertionStrategy(storedDuplicates, List.of(EXTERNAL_ID_AMOUNT_STRICT));

        assertThatThrownBy(() -> sut.determineTransactionPersistenceInstruction(upstreamTransactions, clientUserToken, UUID.randomUUID(), "Dummy", CurrencyCode.EUR))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageStartingWith("""
                        Mode: TEST
                        Provider: DUMMY
                        Fatal: Found 2 un-matched (DUPLICATE) stored transactions
                        Processing of this transaction set has been interrupted and all upstream transactions will be dropped.
                          
                        Unmatched transactions:
                        |transaction-id                      |external-id                     |date      |booking-date|timestamp               |reason      |matcher                         |n-attr|
                        |1                                   |a                               |2019-04-01|2019-04-01|2019-04-01T14:24:11.252Z|DUPLICATE   |ExternalIdAmountStrict          |2     |
                        |1                                   |a                               |2019-04-01|2019-04-01|2019-04-01T14:24:11.252Z|DUPLICATE   |ExternalIdAmountStrict          |2     |
                          
                        Consult the match report for detailed information about the matching output.
                        """);
    }

    @Test
    void given_unmatchedStoredTransaction_when_transactionNotCoincidesWithOldestUpstream_then_failWithIllegalStateException() {

        var now = ZonedDateTime.parse("2019-04-01T16:24:11.252+02:00[Europe/Amsterdam]");

        List<ProviderTransactionDTO> upstreamTransactions = List.of(
                transactionWithExternalIdAmountAndDate("a", new BigDecimal("200.00"), now.minusDays(7))); // oldest

        List<Transaction> storedTransactions = List.of(
                createTransactionWithId("1", transactionWithExternalIdAmountAndDate("b", new BigDecimal("400.00"), now)));

        var sut = createAttributeInsertionStrategy(storedTransactions, List.of(EXTERNAL_ID_AMOUNT_STRICT));

        assertThatThrownBy(() -> sut.determineTransactionPersistenceInstruction(upstreamTransactions, clientUserToken, UUID.randomUUID(), "Dummy", CurrencyCode.EUR)).isInstanceOf(IllegalStateException.class)
                .hasMessageStartingWith("""
                        Mode: TEST
                        Provider: DUMMY
                        Fatal: Found 1 un-matched (PEERLESS) stored transactions
                        Processing of this transaction set has been interrupted and all upstream transactions will be dropped.
                        """);
    }

    @Test
    void given_unmatchedStoredTransaction_when_transactionCoincidesWithOldestUpstream_then_Insert() {

        var now = ZonedDateTime.now();

        List<ProviderTransactionDTO> upstreamTransactions = List.of(
                transactionWithExternalIdAmountAndDate("a", new BigDecimal("200.00"), now.minusDays(7)));

        List<Transaction> storedTransactions = List.of(
                createTransactionWithId("1", transactionWithExternalIdAmountAndDate("b", new BigDecimal("400.00"), now.minusDays(7))));

        var sut = createAttributeInsertionStrategy(storedTransactions, List.of(EXTERNAL_ID_AMOUNT_STRICT));
        var instruction = sut.determineTransactionPersistenceInstruction(upstreamTransactions, clientUserToken, UUID.randomUUID(), "Dummy", CurrencyCode.EUR);

        assertThat(instruction.getTransactionsToInsert()).hasSize(1);
        assertThat(instruction.getTransactionsToInsert())
                .allMatch(providerTransactionWithId -> providerTransactionWithId.getFillType() == Transaction.FillType.REGULAR);
        assertThat(instruction.getOldestTransactionChangeDate()).contains(now.minusDays(7).toLocalDate());

        assertThat(instruction.getTransactionsToDelete()).isEmpty();
        assertThat(instruction.getTransactionsToUpdate()).isEmpty();
        assertThat(instruction.getTransactionsToIgnore()).isEmpty();
    }

    @Test
    @Disabled
    void given_PeerlessInUpstreamAndDuplicatesInStored_whenMultiMatched_thenInsertNothing() {
        var now = ZonedDateTime.now();

        List<ProviderTransactionDTO> upstreamTransactions = List.of(
                transactionWithExternalIdAmountAndDate("a", new BigDecimal("200.00"), now));

        List<Transaction> storedTransactions = List.of(
                createTransactionWithId("1", transactionWithExternalIdAmountAndDate("a", new BigDecimal("200.00"), now)),
                createTransactionWithId("2", transactionWithExternalIdAmountAndDate("a", new BigDecimal("200.00"), now)));

        var sut = createAttributeInsertionStrategy(storedTransactions, List.of(EXTERNAL_ID_AMOUNT_STRICT));
        var instruction = sut.determineTransactionPersistenceInstruction(upstreamTransactions, clientUserToken, UUID.randomUUID(), "Dummy", CurrencyCode.EUR);

        assertThat(instruction.getTransactionsToInsert()).isEmpty();
        assertThat(instruction.getTransactionsToDelete()).isEmpty();
        assertThat(instruction.getTransactionsToUpdate()).isEmpty();
        assertThat(instruction.getTransactionsToIgnore()).isEmpty();
    }

    @Test
    void shouldRemovePendingFromDatabase() {

        var now = ZonedDateTime.now();

        List<ProviderTransactionDTO> upstreamTransactions = List.of(
                transactionWithExternalIdAmountAndDate("123", new BigDecimal("200.00"), now).toBuilder()
                        .status(TransactionStatus.BOOKED)
                        .build());

        List<Transaction> storedTransactions = List.of(
                createTransactionWithId("1", transactionWithExternalIdAmountAndDate("123", new BigDecimal("200.00"), now)).toBuilder()
                        .status(TransactionStatus.BOOKED)
                        .build(),
                createTransactionWithId("2", transactionWithExternalIdAmountAndDate("456", new BigDecimal("200.00"), now)).toBuilder()
                        .status(TransactionStatus.PENDING)
                        .build());

        var repository = mock(TransactionRepository.class);
        when(repository.getTransactionsInAccountFromDate(any(), any(), any()))
                .thenReturn(new ArrayList<>(storedTransactions));

        var sut = createAttributeInsertionStrategy(storedTransactions, List.of(EXTERNAL_ID_AMOUNT_STRICT));
        var instruction = sut.determineTransactionPersistenceInstruction(upstreamTransactions, clientUserToken, UUID.randomUUID(), "Dummy", CurrencyCode.EUR);

        assertThat(instruction.getTransactionsToDelete()).hasSize(1);
        assertThat(instruction.getTransactionsToDelete()).allSatisfy(transaction -> assertThat(transaction.getStatus()).isEqualTo(TransactionStatus.PENDING));
        assertThat(instruction.getTransactionsToInsert()).isEmpty();
        assertThat(instruction.getTransactionsToIgnore()).isEmpty();
        assertThat(instruction.getTransactionsToUpdate()).isEmpty();
        assertThat(instruction.getOldestTransactionChangeDate()).isEmpty();
    }

    @Test
    void testMatcherShortCircuitIfNoUpstreamOrStored() {
        var configuration = ProviderConfiguration.builder()
                .provider("Dummy")
                .syncWindowSelector(new UnboundedSyncWindowSelector())
                .matchers(List.of(EXTERNAL_ID_AMOUNT_STRICT, BOOKING_DATE_AMOUNT_STRICT))
                .build();

        var matchResults = reconcileTransactionsOnAttributes(configuration, emptyList(), emptyList());
        assertThat(matchResults.results).hasSize(1); // initial only.
    }

    @Test
    void testMatcherShortCircuitIfNoneUnmatched() {

        var now = ZonedDateTime.now();
        var configuration = ProviderConfiguration.builder()
                .provider("Dummy")
                .syncWindowSelector(new UnboundedSyncWindowSelector())
                .matchers(List.of(EXTERNAL_ID_AMOUNT_STRICT, BOOKING_DATE_AMOUNT_STRICT))
                .build();

        var upstreamTransactions = List.of(
                transactionWithExternalIdAmountAndDate("123", new BigDecimal("200.00"), now).toBuilder()
                        .status(TransactionStatus.BOOKED)
                        .build());
        var storedTransactions = List.of(
                createTransactionWithId("1", transactionWithExternalIdAmountAndDate("123", new BigDecimal("200.00"), now)).toBuilder()
                        .status(TransactionStatus.BOOKED)
                        .build());

        var matchResults = reconcileTransactionsOnAttributes(
                configuration,
                toProviderGeneralized(upstreamTransactions),
                toStoredGeneralized(storedTransactions));

        assertThat(matchResults.results).hasSize(2); // INITIAL + EXTERNAL_ID_AMOUNT_STRICT
    }

    @Test
    void testMatcherShortCircuitIfAllMatchersUnmatched() {

        var now = ZonedDateTime.now();
        var configuration = ProviderConfiguration.builder()
                .provider("Dummy")
                .syncWindowSelector(new UnboundedSyncWindowSelector())
                .matchers(List.of(EXTERNAL_ID_AMOUNT_STRICT, BOOKING_DATE_AMOUNT_STRICT))
                .build();

        var upstreamTransactions = List.of(
                transactionWithExternalIdAmountAndDate("123", new BigDecimal("200.00"), now).toBuilder()
                        .status(TransactionStatus.BOOKED)
                        .build());
        var storedTransactions = List.of(
                createTransactionWithId("1", transactionWithExternalIdAmountAndDate("456", new BigDecimal("400.00"), now)).toBuilder()
                        .status(TransactionStatus.BOOKED)
                        .build());

        var matchResults = reconcileTransactionsOnAttributes(
                configuration,
                toProviderGeneralized(upstreamTransactions),
                toStoredGeneralized(storedTransactions));

        assertThat(matchResults.results).hasSize(3); // INITIAL + EXTERNAL_ID_AMOUNT_STRICT + BOOKING_DATE_AMOUNT_STRICT
    }

    @Test
    void shouldGivePrecedenceOverRejectedWhenPreviousResultWasUnprocessed() {

        var now = ZonedDateTime.now();
        var configuration = ProviderConfiguration.builder()
                .provider("Dummy")
                .syncWindowSelector(new UnboundedSyncWindowSelector())
                .matchers(List.of(EXTERNAL_ID_AMOUNT_STRICT, BOOKING_DATE_AMOUNT_STRICT))
                .build();

        List<ProviderTransactionDTO> upstreamTransactions = List.of(
                transactionWithExternalIdAmountAndDate(null, new BigDecimal("100"), now),
                transactionWithExternalIdAmountAndDate(null, new BigDecimal("100"), now));

        var matchResults = reconcileTransactionsOnAttributes(
                configuration,
                toProviderGeneralized(upstreamTransactions),
                emptyList());

        assertThat(matchResults.results.get(0).unmatchedUpstream)
                .allSatisfy(unmatched -> assertThat(unmatched.reason).isEqualTo(UNPROCESSED));
        assertThat(matchResults.results.get(1).unmatchedUpstream)
                .allSatisfy(unmatched -> assertThat(unmatched.reason).isEqualTo(REJECTED));
    }

    @Test
    void shouldGivePrecedenceOverDuplicateWhenPreviousResultWasRejected() {

        var now = ZonedDateTime.now();
        var configuration = ProviderConfiguration.builder()
                .provider("Dummy")
                .syncWindowSelector(new UnboundedSyncWindowSelector())
                .matchers(List.of(
                        new EqualityAttributeTransactionMatcher("NotNullExternalIdOnly", List.of(
                                notNull(AttributeSelectors.EXTERNAL_ID) // rejects all absent external-id
                        )),
                        new EqualityAttributeTransactionMatcher("AmountInCentsOnly", List.of(
                                AttributeSelectors.AMOUNT_IN_CENTS // matches on amounts in cents uniquely
                        ))
                ))
                .build();

        List<ProviderTransactionDTO> upstreamTransactions = List.of(
                transactionWithExternalIdAmountAndDate(null, new BigDecimal("100"), now),
                transactionWithExternalIdAmountAndDate(null, new BigDecimal("100"), now));

        var matchResults = reconcileTransactionsOnAttributes(
                configuration,
                toProviderGeneralized(upstreamTransactions),
                emptyList());

        assertThat(matchResults.results.get(0).unmatchedUpstream)
                .allSatisfy(unmatched -> assertThat(unmatched.reason).isEqualTo(UNPROCESSED));
        assertThat(matchResults.results.get(1).unmatchedUpstream)
                .allSatisfy(unmatched -> assertThat(unmatched.reason).isEqualTo(REJECTED));
        assertThat(matchResults.results.get(2).unmatchedUpstream)
                .allSatisfy(unmatched -> assertThat(unmatched.reason).isEqualTo(DUPLICATE));
    }

    @Test
    void shouldGivePrecedenceOverPreviousPeerlessOnDuplicate() {

        var now = ZonedDateTime.now();
        var configuration = ProviderConfiguration.builder()
                .provider("Dummy")
                .syncWindowSelector(new UnboundedSyncWindowSelector())
                .matchers(List.of(
                        new EqualityAttributeTransactionMatcher("NotNullExternalIdOnly", List.of(
                                notNull(AttributeSelectors.EXTERNAL_ID)
                        )),
                        new EqualityAttributeTransactionMatcher("AmountInCentsOnly", List.of(
                                AttributeSelectors.AMOUNT_IN_CENTS
                        ))
                ))
                .build();

        List<ProviderTransactionDTO> upstreamTransactions = List.of(
                transactionWithExternalIdAmountAndDate("a", new BigDecimal("100"), now),
                transactionWithExternalIdAmountAndDate("b", new BigDecimal("100"), now));

        var matchResults = reconcileTransactionsOnAttributes(
                configuration,
                toProviderGeneralized(upstreamTransactions),
                emptyList());

        assertThat(matchResults.results.get(0).unmatchedUpstream)
                .allSatisfy(unmatched -> assertThat(unmatched.reason).isEqualTo(UNPROCESSED)); // initial
        assertThat(matchResults.results.get(1).unmatchedUpstream)
                .allSatisfy(unmatched -> assertThat(unmatched.reason).isEqualTo(PEERLESS)); // NotNullExternalIdOnly
        assertThat(matchResults.results.get(2).unmatchedUpstream)
                .allSatisfy(unmatched -> assertThat(unmatched.reason).isEqualTo(PEERLESS)); // AmountInCentsOnly
    }

    @Test
    void shouldGivePrecedenceOverPreviousPeerlessOnRejected() {

        var now = ZonedDateTime.now();
        var configuration = ProviderConfiguration.builder()
                .provider("Dummy")
                .syncWindowSelector(new UnboundedSyncWindowSelector())
                .matchers(List.of(
                        new EqualityAttributeTransactionMatcher("NotNullExternalIdOnly", List.of(
                                notNull(AttributeSelectors.EXTERNAL_ID)
                        )),
                        new EqualityAttributeTransactionMatcher("NotNullDescription", List.of(
                                notNull(AttributeSelectors.DESCRIPTION)
                        ))
                ))
                .build();

        List<ProviderTransactionDTO> upstreamTransactions = List.of(
                transactionWithExternalIdAmountAndDate("a", new BigDecimal("100"), now).toBuilder()
                        .description(null) // causes rejection
                        .build(),
                transactionWithExternalIdAmountAndDate("b", new BigDecimal("100"), now).toBuilder()
                        .description(null) // causes rejection
                        .build());

        var matchResults = reconcileTransactionsOnAttributes(
                configuration,
                toProviderGeneralized(upstreamTransactions),
                emptyList());

        assertThat(matchResults.results.get(0).unmatchedUpstream)
                .allSatisfy(unmatched -> assertThat(unmatched.reason).isEqualTo(UNPROCESSED)); // initial
        assertThat(matchResults.results.get(1).unmatchedUpstream)
                .allSatisfy(unmatched -> assertThat(unmatched.reason).isEqualTo(PEERLESS)); // NotNullExternalIdOnly
        assertThat(matchResults.results.get(2).unmatchedUpstream)
                .allSatisfy(unmatched -> assertThat(unmatched.reason).isEqualTo(PEERLESS)); // NotNullDescription
    }

    @Test
    void shouldHaveSameNumberOfTransactionsInEveryStage() {

        var now = ZonedDateTime.now();
        var configuration = ProviderConfiguration.builder()
                .provider("Dummy")
                .syncWindowSelector(new UnboundedSyncWindowSelector())
                .matchers(List.of(
                        new EqualityAttributeTransactionMatcher("NotNullExternalIdOnly", List.of(
                                notNull(AttributeSelectors.EXTERNAL_ID)
                        )),
                        new EqualityAttributeTransactionMatcher("NotNullAmount", List.of(
                                notNull(AttributeSelectors.AMOUNT_IN_CENTS)
                        )),
                        new EqualityAttributeTransactionMatcher("NotNullDescription", List.of(
                                notNull(AttributeSelectors.DESCRIPTION)
                        ))
                ))
                .build();

        var upstreamTransactions = List.of(
                transactionWithExternalIdAmountAndDate("a", new BigDecimal("100.00"), now),
                transactionWithExternalIdAmountAndDate("b", new BigDecimal("200.00"), now),
                transactionWithExternalIdAmountAndDate(null, new BigDecimal("300.00"), now),
                transactionWithExternalIdAmountAndDate(null, new BigDecimal("300.00"), now)
                        .toBuilder()
                        .description("300")
                        .build());

        var storedTransactions = List.of(
                createTransactionWithId("1", transactionWithExternalIdAmountAndDate("a", new BigDecimal("100.00"), now)),
                createTransactionWithId("2", transactionWithExternalIdAmountAndDate("b", new BigDecimal("200.00"), now)),
                createTransactionWithId("3", transactionWithExternalIdAmountAndDate(null, new BigDecimal("300.00"), now)),
                createTransactionWithId("3", transactionWithExternalIdAmountAndDate(null, new BigDecimal("300.00"), now))
                        .toBuilder()
                        .description("300")
                        .build()
        );

        var matchResults = reconcileTransactionsOnAttributes(
                configuration,
                toProviderGeneralized(upstreamTransactions),
                toStoredGeneralized(storedTransactions));

        assertThat(matchResults.results).hasSize(4);
        assertThat(matchResults.last().orElseThrow().extractTransactions().size()).isEqualTo(8);
    }

    @ParameterizedTest
    @ValueSource(strings = {"2011-02-02", "2011-02-03", "2011-02-04"})
    void isDateBetweenExclusive(String date) {
        var isBetween = AttributeInsertionStrategy
                .isDateBetweenExclusive(LocalDate.parse("2011-02-01"), LocalDate.parse("2011-02-05")).test(new ThrowingGeneralizedTransaction() {
                    @Override
                    public @NonNull LocalDate getDate() {
                        return LocalDate.parse(date);
                    }
                });
        assertThat(isBetween).isTrue();
    }

    @ParameterizedTest
    @ValueSource(strings = {"2011-02-01", "2011-02-05"})
    void isNotDateBetweenExclusive(String date) {
        var isBetween = AttributeInsertionStrategy
                .isDateBetweenExclusive(LocalDate.parse("2011-02-01"), LocalDate.parse("2011-02-05")).test(new ThrowingGeneralizedTransaction() {
                    @Override
                    public @NonNull LocalDate getDate() {
                        return LocalDate.parse(date);
                    }
                });
        assertThat(isBetween).isFalse();
    }

    @Test
    void isDateEqual() {
        var isEqual = AttributeInsertionStrategy
                .isDateEqual(LocalDate.parse("2011-02-01")).test(new ThrowingGeneralizedTransaction() {
                    @Override
                    public @NonNull LocalDate getDate() {
                        return LocalDate.parse("2011-02-01");
                    }
                });
        assertThat(isEqual).isTrue();
    }

    @Test
    void isNotDateEqual() {
        var isEqual = AttributeInsertionStrategy
                .isDateEqual(LocalDate.parse("2011-02-01")).test(new ThrowingGeneralizedTransaction() {
                    @Override
                    public @NonNull LocalDate getDate() {
                        return LocalDate.parse("2011-02-15");
                    }
                });
        assertThat(isEqual).isFalse();
    }

    @Test
    void calculateOldestTransactionDate() {
        var oldest = AttributeInsertionStrategy.calculateOldestTransactionDate(List.of(
                new ThrowingGeneralizedTransaction() {
                    @Override
                    public @NonNull LocalDate getDate() {
                        return LocalDate.parse("2011-02-01");
                    }
                },
                new ThrowingGeneralizedTransaction() {
                    @Override
                    public @NonNull LocalDate getDate() {
                        return LocalDate.parse("2011-02-02");
                    }
                },
                new ThrowingGeneralizedTransaction() {
                    @Override
                    public @NonNull LocalDate getDate() {
                        return LocalDate.parse("2011-02-03");
                    }
                },
                new ThrowingGeneralizedTransaction() {
                    @Override
                    public @NonNull LocalDate getDate() {
                        return LocalDate.parse("2011-02-04");
                    }
                }
        ));
        assertThat(oldest).contains(LocalDate.parse("2011-02-01"));
    }

    @Test
    void calculateMostRecentTransactionDate() {
        var mostRecent = AttributeInsertionStrategy.calculateMostRecentTransactionDate(List.of(
                new ThrowingGeneralizedTransaction() {
                    @Override
                    public @NonNull LocalDate getDate() {
                        return LocalDate.parse("2011-02-01");
                    }
                },
                new ThrowingGeneralizedTransaction() {
                    @Override
                    public @NonNull LocalDate getDate() {
                        return LocalDate.parse("2011-02-02");
                    }
                },
                new ThrowingGeneralizedTransaction() {
                    @Override
                    public @NonNull LocalDate getDate() {
                        return LocalDate.parse("2011-02-03");
                    }
                },
                new ThrowingGeneralizedTransaction() {
                    @Override
                    public @NonNull LocalDate getDate() {
                        return LocalDate.parse("2011-02-04");
                    }
                }
        ));
        assertThat(mostRecent).contains(LocalDate.parse("2011-02-04"));
    }

    public static AttributeInsertionStrategy createAttributeInsertionStrategy(
            final @NonNull List<Transaction> storedTransactions,
            final @NonNull List<? extends AttributeTransactionMatcher> matchers) {

        return new AttributeInsertionStrategy(
                Mode.TEST,
                (userId, accountId, earliestDate) -> storedTransactions,
                TRANSACTION_ID_PROVIDER,
                ProviderConfiguration.builder()
                        .provider("DUMMY")
                        .syncWindowSelector(new UnboundedSyncWindowSelector())
                        .matchers(matchers)
                        .build());
    }
}
