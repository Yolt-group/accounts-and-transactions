package com.yolt.accountsandtransactions.inputprocessing;

import com.yolt.accountsandtransactions.inputprocessing.matching.GeneralizedTransaction;
import com.yolt.accountsandtransactions.transactions.Transaction;
import nl.ing.lovebird.extendeddata.transaction.ExtendedTransactionDTO;
import nl.ing.lovebird.providerdomain.ProviderTransactionDTO;
import org.junit.jupiter.api.Test;

import java.time.*;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static java.util.Collections.emptyList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

class BookingDateSyncWindowSelectorTest {

    private static final LocalDate TODAY = LocalDate.now();

    private final BookingDateSyncWindowSelector systemUnderTest = new BookingDateSyncWindowSelector();

    @Test
    public void shouldSelectUnboundedWindowWhenUpstreamBookingDatesUnreliable() {
        final var upstream = List.of(
                upstream(delta(0)),
                upstream(delta(-1))
        );
        final var stored = List.of(
                stored(delta(-1)),
                // No booking date
                GeneralizedTransaction.StoredGeneralizedTransaction.builder()
                        .stored(Transaction.builder().build())
                        .build()
        );

        final var result = systemUnderTest.selectWindow(upstream, stored);

        assertUnbounded(result);
    }

    @Test
    public void shouldSelectUnboundedWindowWhenStoredBookingDatesUnreliable() {
        final var upstream = List.of(
                upstream(delta(0)),
                // No booking date
                GeneralizedTransaction.ProviderGeneralizedTransaction.builder()
                        .provider(ProviderTransactionDTO.builder()
                                .extendedTransaction(ExtendedTransactionDTO.builder().build())
                                .build())
                        .build()
        );
        final var stored = List.of(
                stored(delta(-1)),
                stored(delta(-2))
        );

        final var result = systemUnderTest.selectWindow(upstream, stored);

        assertUnbounded(result);
    }

    @Test
    public void shouldNotSelectWindowWhenEmptyUpstream() {
        final var stored = stored(
                delta(-1),
                delta(-2),
                delta(-5),
                delta(-7)
        );

        final var result = catchThrowable(() -> systemUnderTest.selectWindow(emptyList(), stored));

        assertThat(result).isInstanceOf(IllegalStateException.class);
    }

    @Test
    public void shouldSelectUnboundedWindowWhenEmptyStored() {
        final var upstream = upstream(
                delta(-1),
                delta(-2),
                delta(-5),
                delta(-7)
        );

        final var result = systemUnderTest.selectWindow(upstream, emptyList());

        assertUnbounded(result);
    }

    @Test
    public void shouldNotSelectWindowWhenEmptyUpstreamAndEmptyStored() {
        final var result = catchThrowable(() -> systemUnderTest.selectWindow(emptyList(), emptyList()));

        assertThat(result).isInstanceOf(IllegalStateException.class);
    }

    @Test
    public void shouldSelectEarliestOverlappingFullDayWhenMultipleFullDaysOverlap() {
        final var upstream = upstream(
                delta(0),
                delta(-1),
                delta(-1),
                delta(-2),
                delta(-5)
        );
        final var stored = stored(
                delta(-1),
                delta(-2),
                delta(-5),
                delta(-5),
                delta(-7)
        );

        final var result = systemUnderTest.selectWindow(upstream, stored);

        assertBounded(result, delta(-4), true);
    }

    @Test
    public void shouldSelectOverlappingFullDayWhenSingleFullDayOverlap() {
        final var upstream = upstream(
                delta(0),
                delta(-1),
                delta(-1),
                delta(-3)
        );
        final var stored = stored(
                delta(-1),
                delta(-3)
        );

        final var result = systemUnderTest.selectWindow(upstream, stored);

        assertBounded(result, delta(-2), true);
    }

    @Test
    public void shouldSelectLatestPartialDayWhenTwoPartialDaysOverlap() {
        final var upstream = upstream(
                delta(0),
                delta(-1),
                delta(-1),
                delta(-2)
        );
        final var stored = stored(
                delta(-1),
                delta(-2),
                delta(-2),
                delta(-5)
        );

        final var result = systemUnderTest.selectWindow(upstream, stored);

        assertBounded(result, delta(-1), true);
    }

    @Test
    public void shouldSelectFullUpstreamWhenOnePartialDayOverlap() {
        final var upstream = upstream(
                delta(0),
                delta(-1)
        );
        final var stored = stored(
                delta(-1),
                delta(-1),
                delta(-2),
                delta(-5),
                delta(-5)
        );

        final var result = systemUnderTest.selectWindow(upstream, stored);

        assertBounded(result, delta(-1), false);
    }

    @Test
    public void shouldSelectFullUpstreamWhenOnePartialDayOverlapAndStoredWindowIsSingleDay() {
        final var upstream = upstream(
                delta(0),
                delta(-1),
                delta(-1)
        );
        final var stored = List.of(stored(delta(-1)));

        final var result = systemUnderTest.selectWindow(upstream, stored);

        assertBounded(result, delta(-1), false);
    }

    @Test
    public void shouldSelectFullUpstreamWhenNoOverlap() {
        final var upstream = upstream(
                delta(0),
                delta(-1),
                delta(-1)
        );
        final var stored = stored(
                delta(-2),
                delta(-5),
                delta(-5)
        );

        final var result = systemUnderTest.selectWindow(upstream, stored);

        assertBounded(result, delta(-1), false);
    }

    @Test
    public void shouldSelectEarliestOverlappingFullDayWhenUpstreamBehindStored() {
        final var upstream = upstream(
                delta(-1),
                delta(-2),
                delta(-5)
        );
        final var stored = stored(
                delta(0),
                delta(-1),
                delta(-1),
                delta(-2),
                delta(-5)
        );

        final var result = systemUnderTest.selectWindow(upstream, stored);

        assertBounded(result, delta(-4), true);
    }

    @Test
    public void shouldSelectUnboundedWhenUpstreamBeforeStored() {
        final var upstream = upstream(
                delta(0),
                delta(-1),
                delta(-1),
                delta(-2),
                delta(-5)
        );
        final var stored = stored(
                delta(-1),
                delta(-1),
                delta(-2)
        );

        final var result = systemUnderTest.selectWindow(upstream, stored);

        assertUnbounded(result);
    }

    @Test
    public void shouldSelectUnboundedWhenUpstreamBeforeStoredAndOneFullDayOverlap() {
        final var upstream = upstream(
                delta(0),
                delta(-1),
                delta(-1),
                delta(-3),
                delta(-5)
        );
        final var stored = stored(
                delta(-1),
                delta(-1),
                delta(-3)
        );

        final var result = systemUnderTest.selectWindow(upstream, stored);

        assertUnbounded(result);
    }

    @Test
    public void shouldSelectUnboundedWindowWhenUpstreamBeforeStoredAndOnePartialDayOverlap() {
        final var upstream = upstream(
                delta(0),
                delta(-1),
                delta(-1),
                delta(-2),
                delta(-5)
        );
        final var stored = stored(
                delta(-1),
                delta(-1)
        );

        final var result = systemUnderTest.selectWindow(upstream, stored);

        assertUnbounded(result);
    }

    @Test
    public void shouldSelectEarliestOverlappingFullDayWhenUpstreamStartsStored() {
        final var upstream = upstream(
                delta(-2),
                delta(-3)
        );
        final var stored = stored(
                delta(-1),
                delta(-2),
                delta(-3)
        );

        final var result = systemUnderTest.selectWindow(upstream, stored);

        assertBounded(result, delta(-2), true);
    }

    @Test
    public void shouldSelectFullUpstreamWhenSingleUpstreamEqualToStored() {
        final var upstream = List.of(upstream(delta(-1)));
        final var stored = List.of(stored(delta(-1)));

        final var result = systemUnderTest.selectWindow(upstream, stored);

        assertBounded(result, delta(-1), false);
    }

    @Test
    public void shouldSelectFullUpstreamWhenSingleUpstreamAfterStored() {
        final var upstream = List.of(upstream(delta(-1)));
        final var stored = List.of(stored(delta(-2)));

        final var result = systemUnderTest.selectWindow(upstream, stored);

        assertBounded(result, delta(-1), false);
    }

    @Test
    public void shouldSelectUnboundedWhenSingleUpstreamAtStartOfStored() {
        final var upstream = List.of(upstream(
                delta(-2)
        ));
        final var stored = stored(
                delta(-1),
                delta(-2)
        );

        final var result = systemUnderTest.selectWindow(upstream, stored);

        assertUnbounded(result);
    }

    @Test
    public void shouldSelectEarliestUpstreamWhenSingleUpstreamInStored() {
        final var upstream = List.of(upstream(
                delta(-2)
        ));
        final var stored = stored(
                delta(-1),
                delta(-2),
                delta(-3)
        );

        final var result = systemUnderTest.selectWindow(upstream, stored);

        assertBounded(result, delta(-2), false);
    }

    @Test
    public void shouldSelectUnboundedWhenSingleUpstreamAtEndOfStored() {
        final var upstream = List.of(upstream(
                delta(-1)
        ));
        final var stored = stored(
                delta(-1),
                delta(-2)
        );

        final var result = systemUnderTest.selectWindow(upstream, stored);

        assertBounded(result, delta(-1), false);
    }

    @Test
    public void shouldTruncateLowerBound() {
        final var transactions = stored(
                delta(-1),
                delta(-2),
                delta(-5),
                delta(-5),
                delta(-6),
                delta(-10)
        );

        final var window = BookingDateSyncWindowSelector.BookingDateSyncWindow.bounded(delta(-5), false);
        final var result = window.truncate(transactions);

        assertThat(result).flatExtracting(GeneralizedTransaction.StoredGeneralizedTransaction::getBookingDate).containsExactly(
                delta(-1),
                delta(-2),
                delta(-5),
                delta(-5)
        );
    }

    private static void assertBounded(BookingDateSyncWindowSelector.BookingDateSyncWindow result, LocalDate delta, boolean completeBookingDay) {
        assertThat(result.isBounded()).isTrue();
        assertThat(result.isUnbounded()).isFalse();
        assertThat(result.getLowerBound()).contains(delta);
        assertThat(result.isLowerBoundCompleteBookingDay()).isEqualTo(completeBookingDay);
    }

    private static void assertUnbounded(BookingDateSyncWindowSelector.BookingDateSyncWindow result) {
        assertThat(result.isBounded()).isFalse();
        assertThat(result.isUnbounded()).isTrue();
        assertThat(result.getLowerBound()).isEmpty();
        assertThat(result.isLowerBoundCompleteBookingDay()).isFalse();
    }

    /**
     * Date with an offset from the "base" day used in this test. Negative days indicate a number of days in the past.
     *
     * @param days the number of days difference from the base day (may be negative)
     * @return the resulting day
     */
    private static LocalDate delta(int days) {
        return TODAY.plusDays(days);
    }

    /**
     * Creates a list of generalized provider transactions with the given booking dates.
     *
     * @param bookingDates the dates for the transactions
     * @return list of transactions with the given dates
     */
    private static List<GeneralizedTransaction.ProviderGeneralizedTransaction> upstream(LocalDate... bookingDates) {
        return Arrays.stream(bookingDates)
                .map(BookingDateSyncWindowSelectorTest::upstream)
                .collect(Collectors.toUnmodifiableList());
    }

    /**
     * Creates a list of generalized stored transactions with the given booking dates.
     *
     * @param bookingDates the dates for the transactions
     * @return list of transactions with the given dates
     */
    private static List<GeneralizedTransaction.StoredGeneralizedTransaction> stored(LocalDate... bookingDates) {
        return Arrays.stream(bookingDates)
                .map(BookingDateSyncWindowSelectorTest::stored)
                .collect(Collectors.toUnmodifiableList());
    }

    /**
     * Creates a generalized provider transaction with the given booking date.
     *
     * @param bookingDate the date for the transactions
     * @return transaction with the given date
     */
    private static GeneralizedTransaction.ProviderGeneralizedTransaction upstream(LocalDate bookingDate) {
        return GeneralizedTransaction.ProviderGeneralizedTransaction.builder()
                .provider(ProviderTransactionDTO.builder()
                        .extendedTransaction(ExtendedTransactionDTO.builder()
                                .bookingDate(ZonedDateTime.of(bookingDate, LocalTime.MIDNIGHT, ZoneOffset.UTC))
                                .build())
                        .build())
                .build();
    }

    /**
     * Creates a generalized stored transaction with the given booking date.
     *
     * @param bookingDate the date for the transactions
     * @return transaction with the given date
     */
    private static GeneralizedTransaction.StoredGeneralizedTransaction stored(LocalDate bookingDate) {
        return GeneralizedTransaction.StoredGeneralizedTransaction.builder()
                .stored(Transaction.builder()
                        .bookingDate(bookingDate)
                        .build())
                .build();
    }
}
