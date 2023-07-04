package com.yolt.accountsandtransactions.datascience;

import com.yolt.accountsandtransactions.ApplicationConfiguration;
import com.yolt.accountsandtransactions.transactions.Transaction;
import com.yolt.accountsandtransactions.transactions.TransactionService.TransactionPrimaryKey;
import nl.ing.lovebird.extendeddata.transaction.TransactionStatus;
import nl.ing.lovebird.providerdomain.ProviderTransactionDTO;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoField;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.yolt.accountsandtransactions.TestBuilders.createTransactionTemplate;
import static com.yolt.accountsandtransactions.datascience.TransactionSyncService.retrieveStoredTransactionsInSameTimeWindow;
import static java.util.UUID.randomUUID;
import static org.assertj.core.api.Assertions.assertThat;

public class RetrieveStoredTransactionsInSameTimeWindowTest {

    static ZonedDateTime _1970_01_01_00h00_UTC = Instant.EPOCH.atZone(ZoneOffset.UTC);
    static ZonedDateTime _1970_01_01_23h00_UTC = _1970_01_01_00h00_UTC.plus(23, ChronoUnit.HOURS);
    static ZonedDateTime _1970_01_02_00h00_UTC = _1970_01_01_00h00_UTC.plus(1, ChronoUnit.DAYS);
    static ZonedDateTime _1970_01_03_00h00_UTC = _1970_01_02_00h00_UTC.plus(1, ChronoUnit.DAYS);
    static ZonedDateTime _1970_01_01_23h01_PLUS0100 = _1970_01_01_23h00_UTC.plusMinutes(1).withZoneSameInstant(ZoneOffset.ofHours(1));
    static ZonedDateTime _1970_01_02_00h01_MIN0100 = _1970_01_02_00h00_UTC.plusMinutes(1).withZoneSameInstant(ZoneOffset.ofHours(-1));

    static List<ZonedDateTime> times = List.of(
            /* 0 */ _1970_01_01_00h00_UTC,
            /* 1 */ _1970_01_01_23h00_UTC,
            /* 2 */ _1970_01_01_23h01_PLUS0100,
            /* 3 */ _1970_01_02_00h00_UTC,
            /* 4 */ _1970_01_02_00h01_MIN0100,
            /* 5 */ _1970_01_03_00h00_UTC
    );

    static List<Transaction> stored = times.stream()
            .map(RetrieveStoredTransactionsInSameTimeWindowTest::createBookedTransaction)
            .map(RetrieveStoredTransactionsInSameTimeWindowTest::createTransaction)
            .collect(Collectors.toUnmodifiableList());

    // Assign id's to the transactions for easier assertions
    static {
        int i = 0;
        for (Transaction trx : stored) {
            trx.setId(i++ + "");
        }
    }

    private static List<Transaction> filterStored(LocalDate input) {
        return stored.stream()
                .filter(t -> !t.getDate().isBefore(input))
                .collect(Collectors.toList());
    }

    /**
     * Some checks that were useful while constructing this testsuite.
     */
    @Test
    public void sanityChecks() {
        // checking that all the 'stored' transactions have a distinct timestamp, the tests rely on this.
        assertThat(times.stream().distinct().count()).isEqualTo(stored.size());
        // check that times is sorted in the time dimension, the tests rely on this.
        assertThat(times.stream()
                .sorted(ZonedDateTime::compareTo)
                .collect(Collectors.toList())
        ).containsSubsequence(times);
    }

    @Test
    public void given_timesNearMidnight_when_convertingToLocalDate_then_dayBoundariesCanBeCrossed() {
        // UTC, the conversion to a LocalDate is straightforward here, the time component is just truncated
        assertThat(_1970_01_01_23h00_UTC.toLocalDate()).isEqualTo(LocalDate.parse("1970-01-01"));
        assertThat(_1970_01_02_00h00_UTC.toLocalDate()).isEqualTo(LocalDate.parse("1970-01-02"));

        // When converting this ZDT to a LocalDate the date jumps a day into the past due to the -0100 offset and it being midnight ...
        assertThat(_1970_01_02_00h01_MIN0100.toLocalDate()).isEqualTo(LocalDate.parse("1970-01-01"));
        // ... converting the same **instant** in time (in UTC) gives us another day.
        assertThat(_1970_01_02_00h01_MIN0100.withZoneSameInstant(ZoneOffset.UTC).toLocalDate()).isEqualTo(LocalDate.parse("1970-01-02"));

        // When converting this ZDT to a LocalDate the date jumps a day into the future due to the +0100 offset and it being close to midnight...
        assertThat(_1970_01_01_23h01_PLUS0100.toLocalDate()).isEqualTo("1970-01-02");
        // ... converting the same **instant** in time (in UTC) gives us another day.
        assertThat(_1970_01_01_23h01_PLUS0100.withZoneSameInstant(ZoneOffset.UTC).toLocalDate()).isEqualTo(LocalDate.parse("1970-01-01"));
    }

    @Test
    public void given_transactionsWithTimestampsThatCrossDateBoundariesWhenConvertingToLocalDate_when_queryingForLocalDate_then_transactionsAreLost() {
        // the logical and normal cases:
        assertThat(filterStored(LocalDate.parse("1970-01-01")).stream().map(Transaction::getId).collect(Collectors.toList())).containsExactly("0", "1", "2", "3", "4", "5");
        assertThat(filterStored(LocalDate.parse("1970-01-03")).stream().map(Transaction::getId).collect(Collectors.toList())).containsExactly("5");
        // the weird case, transaction #4 disappears
        assertThat(filterStored(LocalDate.parse("1970-01-02")).stream().map(Transaction::getId).collect(Collectors.toList())).containsExactly("2", "3", "5");
    }

    /**
     * The resulting transactions lists returned here should correspond to the tests in:
     * {@link #given_transactionsWithTimestampsThatCrossDateBoundariesWhenConvertingToLocalDate_when_queryingForLocalDate_then_transactionsAreLost}
     */
    @Test
    public void given_storedTransactionsWithoutTimestamps_when_retrieveStoredTransactionsInSameTimeWindow_then_brokenLegacyBehaviourIsObserved() {
        Function<LocalDate, List<Transaction>> storedTransactionsWithoutTransactionTimestamp = d -> stored.stream()
                .filter(t -> !t.getDate().isBefore(d))
                .map(tx -> tx.toBuilder().timestamp(null).build())
                .collect(Collectors.toList());
        String provider = "doesNotMatter";

        // Legacy behaviour gives us the correct result in these cases:
        assertThat(retrieveStoredTransactionsInSameTimeWindow(provider, List.of(createBookedTransaction(times.get(0))), storedTransactionsWithoutTransactionTimestamp).stream().map(Transaction::getId).collect(Collectors.toList()))
                .containsExactly("0", "1", "2", "3", "4", "5");
        assertThat(retrieveStoredTransactionsInSameTimeWindow(provider, List.of(createBookedTransaction(times.get(5))), storedTransactionsWithoutTransactionTimestamp).stream().map(Transaction::getId).collect(Collectors.toList()))
                .containsExactly("5");

        // Legacy behaviour incorrectly returns transaction 0.
        assertThat(retrieveStoredTransactionsInSameTimeWindow(provider, List.of(createBookedTransaction(times.get(1))), storedTransactionsWithoutTransactionTimestamp).stream().map(Transaction::getId).collect(Collectors.toList()))
                .containsExactly("0", "1", "2", "3", "4", "5");

        // Legacy behaviour incorrectly doesn't return transaction 4.
        assertThat(retrieveStoredTransactionsInSameTimeWindow(provider, List.of(createBookedTransaction(times.get(2))), storedTransactionsWithoutTransactionTimestamp).stream().map(Transaction::getId).collect(Collectors.toList()))
                .containsExactly("2", "3", "5");
        assertThat(retrieveStoredTransactionsInSameTimeWindow(provider, List.of(createBookedTransaction(times.get(3))), storedTransactionsWithoutTransactionTimestamp).stream().map(Transaction::getId).collect(Collectors.toList()))
                .containsExactly("2", "3", "5");

        // Legacy behaviour incorrectly returns 0, 1, 2, and 3.
        assertThat(retrieveStoredTransactionsInSameTimeWindow(provider, List.of(createBookedTransaction(times.get(4))), storedTransactionsWithoutTransactionTimestamp).stream().map(Transaction::getId).collect(Collectors.toList()))
                .containsExactly("0", "1", "2", "3", "4", "5");
    }

    /**
     * This is a test to catch a somewhat subtle bug.  When saving {@link Instant} values to Cassandra the time-component is
     * truncated to milliseconds.  See the codec we use {@link com.datastax.driver.extras.codecs.jdk8.InstantCodec}, loaded by
     * {@link ApplicationConfiguration#registerCodecs()} for more details.
     * <p>
     * This test checks that whenever we compare a transaction with a timestamp that has been truncated to milliseconds from the database ({@code roundedToMs}) to
     * an incoming transaction with an in-memory timestamp with its microseconds still intact, that this component is irrelevant.
     * <p>
     * I.e. test that the comparison happens without taking into account microseconds (or milliseconds for that matter).
     */
    @Test
    public void given_timestampsWithDifferentPrecision_when_retrieveStoredTransactionsInSameTimeWindow_then_milliSecondsDontAffectComparison() {
        Instant now = Instant.EPOCH.with(ChronoField.MICRO_OF_SECOND, 123456L);
        ZonedDateTime preciseTime = ZonedDateTime.from(now.atZone(ZoneOffset.UTC));
        ZonedDateTime roundedToMs = ZonedDateTime.from(now.atZone(ZoneOffset.UTC).truncatedTo(ChronoUnit.MILLIS));
        String provider = "doesNotMatter";
        assertThat(retrieveStoredTransactionsInSameTimeWindow(provider, List.of(createBookedTransaction(preciseTime)), (d) -> List.of(createTransaction(createBookedTransaction(roundedToMs)))).stream().map(Transaction::getId).collect(Collectors.toList()))
                .isNotEmpty();
    }

    /**
     * This tests the "new" behaviour that only looks at timestamps of stored transactions and ignores the date.
     * <p>
     * The list {@link #stored} of transactions was carefully constructed to make the legacy behaviour fail, see this test for a demonstration:
     * {@link #given_storedTransactionsWithoutTimestamps_when_retrieveStoredTransactionsInSameTimeWindow_then_brokenLegacyBehaviourIsObserved()}
     * <p>
     * Because {@link #stored} is sorted by time, the function that we're testing {@link TransactionSyncService#retrieveStoredTransactionsInSameTimeWindow} should return,
     * for every item in {@link #stored} indexed by i, return all transactions with index >= i.
     */
    @Test
    public void given_storedTransactionsWithTimestamps_when_retrieveStoredTransactionsInSameTimeWindow_then_correctResultsAreObtainedDespiteMisleadingLocalDatesInStoredTransactions() {
        String provider = "doesNotMatter";
        for (int i = 0; i < stored.size(); i++) {
            // We use only 1 upstream transaction, that's all that is needed because retrieveStoredTransactionsInSameTimeWindow looks at the earliest timestamp.
            List<ProviderTransactionDTO> upstreamTransactions = List.of(createBookedTransaction(times.get(i)));
            // Expect every item with index >= i in stored to be returned.
            List<Transaction> expectedStoredTransactions = stored.subList(i, stored.size());
            assertThat(retrieveStoredTransactionsInSameTimeWindow(provider, upstreamTransactions, RetrieveStoredTransactionsInSameTimeWindowTest::filterStored))
                    .containsExactlyElementsOf(expectedStoredTransactions);
        }
    }

    private static ProviderTransactionDTO createBookedTransaction(ZonedDateTime dateTime) {
        return ProviderTransactionDTO.builder()
                .status(TransactionStatus.BOOKED)
                .dateTime(dateTime)
                .build();
    }

    private static Transaction createTransaction(ProviderTransactionDTO pt) {
        var allFieldsRandomTransaction = createTransactionTemplate(
                new TransactionPrimaryKey(randomUUID(), randomUUID(), pt.getDateTime().toLocalDate(), randomUUID().toString(), pt.getStatus()));

        return allFieldsRandomTransaction.toBuilder()
                .date(pt.getDateTime().toLocalDate())
                .timestamp(pt.getDateTime().toInstant())
                .build();
    }
}