package com.yolt.accountsandtransactions.inputprocessing;

import com.yolt.accountsandtransactions.inputprocessing.matching.GeneralizedTransaction;
import lombok.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.ObjectUtils;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

import java.time.LocalDate;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

@Slf4j
public class BookingDateSyncWindowSelector implements SyncWindowSelector {

    /**
     * Selects a lower-bound date (inclusive) to limit the matching window for both the upstream and stored transactions
     * to. This date will be chosen such that unmatched stored transactions at the lower end of the window are avoided.
     * In essence, we try to match up the stored transactions with the upstream transactions.
     * <p>
     * At minimum, the earliest upstream date is selected. Whenever the earliest upstream date is not before the
     * earliest stored date, and both lists cover at least the same two days, the day following the earliest upstream
     * date is selected to avoid processing a partially fetched day. Upstream transactions earlier than the earliest
     * stored transaction are allowed, as they are currently missing from the database. In this case the selected window
     * is unbounded.
     * <p>
     * This method does **NOT** guarantee that the upstream and stored transactions match. There may still be
     * transactions missing from either list on days we don't expect them to. This indicates a problem upstream or with
     * our storage. This is the responsibility of the matcher to pick up and report.
     *
     * @param upstream the generalized upstream transactions from providers
     * @param stored   the generalized stored transactions from the database
     * @return the lower limit (inclusive) for the matching window, or empty when a date could not be chosen
     */
    @Override
    public BookingDateSyncWindow selectWindow(List<GeneralizedTransaction.ProviderGeneralizedTransaction> upstream, List<GeneralizedTransaction.StoredGeneralizedTransaction> stored) {
        if (upstream.isEmpty()) {
            log.debug("Upstream empty, processing not possible");
            throw new IllegalStateException("Unable to select sync window, upstream is empty");
        } else if (stored.isEmpty()) {
            log.debug("Stored empty, unbounded window selected");
            return BookingDateSyncWindow.unbounded();
        }

        final var maybeUpstreamRange = bookingDateInterval(upstream);
        final var maybeStoredRange = bookingDateInterval(stored);

        if (maybeUpstreamRange.isEmpty()) {
            log.warn("Unreliable booking dates in upstream transactions, unbounded window selected");
            return BookingDateSyncWindow.unbounded();
        } else if (maybeStoredRange.isEmpty()) {
            log.warn("Unreliable booking dates in stored transactions, unbounded window selected");
            return BookingDateSyncWindow.unbounded();
        }

        final var upstreamRange = maybeUpstreamRange.get();
        final var storedRange = maybeStoredRange.get();

        final var intervalRelation = upstreamRange.relationTo(storedRange);

        return switch (intervalRelation) {
            case PRECEDES, MEETS, OVERLAPS -> {
                log.warn("Insufficient refresh window: upstream {} stored, unbounded selected", intervalRelation);
                yield BookingDateSyncWindow.unbounded();
            }
            case STARTS, DURING -> {
                if (upstreamRange.getDaysInclusive() > 1) {
                    log.warn("Insufficient refresh window: upstream {} stored, earliest upstream full day selected", intervalRelation);
                    yield BookingDateSyncWindow.bounded(upstreamRange.start.plusDays(1), true);
                } else {
                    log.warn("Insufficient refresh window: upstream {} stored, earliest upstream day selected", intervalRelation);
                    yield BookingDateSyncWindow.bounded(upstreamRange.start, false);
                }
            }
            case CONTAINS, FINISHED_BY -> {
                log.info("Potentially insufficient storage window: upstream {} stored, unbounded selected", intervalRelation);
                yield BookingDateSyncWindow.unbounded();
            }
            case PRECEDED_BY, MET_BY -> {
                log.info("Potentially insufficient refresh window: upstream {} stored, full upstream selected", intervalRelation);
                yield BookingDateSyncWindow.bounded(upstreamRange.start, false);
            }
            case OVERLAPPED_BY, STARTED_BY, FINISHES, EQUAL -> {
                // getDaysInclusive is checked just to be sure, should always be true based on the implementation of determining the relation
                if (upstreamRange.start.isBefore(storedRange.end) && upstreamRange.getDaysInclusive() > 1) {
                    log.debug("Sufficient storage and refresh windows: upstream {} stored, earliest upstream full day selected", intervalRelation);
                    yield BookingDateSyncWindow.bounded(upstreamRange.start.plusDays(1), true);
                } else {
                    log.info("Potentially insufficient storage window: upstream {} stored, full upstream selected", intervalRelation);
                    yield BookingDateSyncWindow.bounded(upstreamRange.start, false);
                }
            }
        };
    }

    private static Optional<BookingDateInterval> bookingDateInterval(List<? extends GeneralizedTransaction> transactions) {
        return transactions.stream()
                .map(GeneralizedTransaction::getBookingDate)
                .reduce(new BookingDateIntervalSummary(), BookingDateIntervalSummary::accumulate, BookingDateIntervalSummary::combine)
                .toInterval();
    }

    @RequiredArgsConstructor(access = AccessLevel.PRIVATE)
    public static class BookingDateSyncWindow implements SyncWindow {

        private static final BookingDateSyncWindow UNBOUNDED = new BookingDateSyncWindow(null, false);

        @Nullable
        private final LocalDate lowerBound;

        private final boolean lowerBoundCompleteBookingDay;

        /**
         * @return lower bound booking date (inclusive) of this window, or empty when unbounded
         */
        public Optional<LocalDate> getLowerBound() {
            return Optional.ofNullable(lowerBound);
        }

        @Override
        public boolean isUnbounded() {
            return lowerBound == null;
        }

        /**
         * Returns whether the lower bound date represents a complete booking day, i.e. a day for which we are
         * processing all transactions that we think exist.
         * <p>
         * Always returns {@code false} when the window is unbounded.
         * <p>
         * Whenever the method returns {@code false}, there is the possibility that we end up with a gap in the database
         * or have unmatched transactions on the selected lower bound booking date.
         *
         * @return {@code true} when the lower bound day represents a complete booking day or the most recent day
         */
        public boolean isLowerBoundCompleteBookingDay() {
            return lowerBoundCompleteBookingDay;
        }

        @Override
        public <T extends GeneralizedTransaction> List<T> truncate(List<T> transactions) {
            if (lowerBound == null) {
                return transactions;
            }

            return transactions.stream()
                    .filter(it -> Optional.ofNullable(it.getBookingDate())
                            .map(bookingDate -> !bookingDate.isBefore(lowerBound))
                            .orElseThrow())
                    .collect(Collectors.toList());
        }

        /**
         * Creates a new bounded window with the provided lower bound (inclusive).
         *
         * @param lowerBound lower bound (inclusive)
         * @return bounded window
         */
        public static BookingDateSyncWindow bounded(@NonNull LocalDate lowerBound, boolean fullBookingDay) {
            return new BookingDateSyncWindow(lowerBound, fullBookingDay);
        }

        /**
         * Returns the singleton unbounded window.
         *
         * @return unbounded window
         */
        public static BookingDateSyncWindow unbounded() {
            return UNBOUNDED;
        }
    }

    /**
     * Summary for reducing a stream of transactions with the goal of building an immutable {@link BookingDateInterval}.
     * <p>
     * Inspired by {@link java.util.IntSummaryStatistics}.
     */
    @NoArgsConstructor
    private static class BookingDateIntervalSummary {
        private LocalDate start;
        private LocalDate end;
        private long count;
        private long countNonNull;

        public BookingDateIntervalSummary accumulate(LocalDate value) {
            if (value != null) {
                start = ObjectUtils.min(start, value);
                end = ObjectUtils.max(end, value);
                countNonNull++;
            }
            count++;
            return this;
        }

        public BookingDateIntervalSummary combine(BookingDateIntervalSummary other) {
            start = ObjectUtils.min(start, other.start);
            end = ObjectUtils.max(end, other.end);
            count += other.count;
            countNonNull += other.countNonNull;
            return this;
        }

        public Optional<BookingDateInterval> toInterval() {
            if (count != countNonNull || start == null || end == null) {
                return Optional.empty();
            } else {
                return Optional.of(new BookingDateInterval(start, end));
            }
        }
    }

    /**
     * Interval between two booking dates for a list of transactions.
     */
    private static class BookingDateInterval {
        public final LocalDate start;
        public final LocalDate end;

        public BookingDateInterval(@NonNull LocalDate start, @NonNull LocalDate end) {
            Assert.isTrue(!start.isAfter(end), "End must not be before start");

            this.start = start;
            this.end = end;
        }

        public long getDaysInclusive() {
            return ChronoUnit.DAYS.between(start, end) + 1;
        }

        /**
         * Returns the relation of this interval to the other interval.
         *
         * @param other interval to check the relation with
         * @return relation from this interval to the other interval
         */
        public IntervalRelation relationTo(BookingDateInterval other) {
            return IntervalRelation.between(this, other);
        }
    }

    /**
     * Relation of two intervals.
     * <p>
     * See: https://en.wikipedia.org/wiki/Allen%27s_interval_algebra
     */
    private enum IntervalRelation {
        /**
         * <pre>
         * X  (...]|
         *   ------|------
         * Y       |[...)
         * </pre>
         */
        PRECEDES,

        /**
         * <pre>
         * X       |[...)
         *   ------|------
         * Y  (...]|
         * </pre>
         */
        PRECEDED_BY,

        /**
         * <pre>
         * X   (...]
         *   ------|------
         * Y       [...)
         * </pre>
         */
        MEETS,

        /**
         * <pre>
         * X       [...)
         *   ------|------
         * Y   (...]
         * </pre>
         */
        MET_BY,

        /**
         * <pre>
         * X    [..|]
         *   ------|------
         * Y      [|..]
         * </pre>
         */
        OVERLAPS,

        /**
         * <pre>
         * X      [|..)
         *   ------|------
         * Y    (..|]
         * </pre>
         */
        OVERLAPPED_BY,

        /**
         * <pre>
         * X     [..]
         *   ----|--------
         * Y     [...)
         * </pre>
         */
        STARTS,

        /**
         * <pre>
         * X     [...)
         *   ----|--------
         * Y     [..]
         * </pre>
         */
        STARTED_BY,

        /**
         * <pre>
         * X    [..|..]
         *   ------|------
         * Y   (...|...)
         * </pre>
         */
        DURING,

        /**
         * <pre>
         * X   (...|...)
         *   ------|------
         * Y    [..|..]
         * </pre>
         */
        CONTAINS,

        /**
         * <pre>
         * X      [..]
         *   --------|------
         * Y     (...]
         * </pre>
         */
        FINISHES,

        /**
         * <pre>
         * X     (...]
         *   --------|------
         * Y      [..]
         * </pre>
         */
        FINISHED_BY,

        /**
         * <pre>
         * X   [...|...]
         *   ------|------
         * Y   [...|...]
         * </pre>
         */
        EQUAL;

        /**
         * Returns the interval relation from {@code x} to {@code y}.
         * <p>
         * The result should be read as "X (result) Y", for example, "X precedes Y".
         *
         * @param x first interval
         * @param y second interval
         * @return interval relation from {@code x} to {@code y}
         */
        public static IntervalRelation between(BookingDateInterval x, BookingDateInterval y) {
            if (x.start.isEqual(y.start) && x.end.isEqual(y.end)) {
                return IntervalRelation.EQUAL;
            } else if (x.end.isBefore(y.start)) {
                return IntervalRelation.PRECEDES;
            } else if (x.start.isAfter(y.end)) {
                return IntervalRelation.PRECEDED_BY;
            } else if (x.end.isEqual(y.start)) {
                return IntervalRelation.MEETS;
            } else if (x.start.isEqual(y.end)) {
                return IntervalRelation.MET_BY;
            } else if (x.start.isBefore(y.start) && x.end.isBefore(y.end)) {
                return IntervalRelation.OVERLAPS;
            } else if (x.start.isAfter(y.start) && x.end.isAfter(y.end)) {
                return IntervalRelation.OVERLAPPED_BY;
            } else if (x.start.isEqual(y.start) && x.end.isBefore(y.end)) {
                return IntervalRelation.STARTS;
            } else if (x.start.isEqual(y.start) && x.end.isAfter(y.end)) {
                return IntervalRelation.STARTED_BY;
            } else if (x.start.isAfter(y.start) && x.end.isBefore(y.end)) {
                return IntervalRelation.DURING;
            } else if (x.start.isBefore(y.start) && x.end.isAfter(y.end)) {
                return IntervalRelation.CONTAINS;
            } else if (x.start.isAfter(y.start) && x.end.isEqual(y.end)) {
                return IntervalRelation.FINISHES;
            } else if (x.start.isBefore(y.start) && x.end.isEqual(y.end)) {
                return IntervalRelation.FINISHED_BY;
            } else {
                // Should be unreachable
                throw new IllegalStateException("Intervals do not satisfy any relation");
            }
        }
    }
}
