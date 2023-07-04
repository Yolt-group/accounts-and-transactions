package com.yolt.accountsandtransactions.datetime;

import com.fasterxml.jackson.annotation.JsonValue;
import com.yolt.accountsandtransactions.datetime.exception.DateIntervalParseException;
import lombok.EqualsAndHashCode;
import lombok.Value;

import java.time.LocalDate;
import java.time.Period;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * ISO 8601 Time interval. End inclusive.
 * <p>
 * See https://en.wikipedia.org/wiki/ISO_8601#Time_intervals
 * <p>
 * Supported formats:
 * <pre>
 * {@code
 * <start>/<end>
 * <start>/<duration>
 * <duration>/<end>
 * }
 * </pre>
 */
@Value
@EqualsAndHashCode
public class DateInterval {

    private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ISO_LOCAL_DATE;

    LocalDate start;
    LocalDate end;

    public DateInterval(final String value) {
        DateInterval parsed = parse(value);
        this.start = parsed.start;
        this.end = parsed.end;
    }

    public DateInterval(final LocalDate start, final LocalDate end) {
        Objects.requireNonNull(start);
        Objects.requireNonNull(end);

        if (end.isBefore(start)) {
            throw new IllegalArgumentException(String.format("Start date %s is after end date %s", start, end));
        }

        this.start = start;
        this.end = end;
    }

    public static DateInterval of(final LocalDate start, final Period period) {
        Objects.requireNonNull(start);
        Objects.requireNonNull(period);

        return new DateInterval(start, start.plus(period));
    }

    public static DateInterval parse(final String value) {
        Objects.requireNonNull(value);

        int solidus = value.indexOf('/');
        if (solidus < 0) {
            throw new DateIntervalParseException(String.format(Locale.ROOT, "Input '%s' not in the format <date/period>/<date/period>", value));
        }

        boolean firstIsPeriod = value.charAt(0) == 'p' || value.charAt(0) == 'P';
        boolean secondIsPeriod = value.charAt(solidus + 1) == 'p' || value.charAt(solidus + 1) == 'P';

        LocalDate start;
        LocalDate end;

        if (firstIsPeriod && secondIsPeriod) {
            throw new DateIntervalParseException("Both sides of solidus can't be Periods");
        }

        CharSequence first = value.subSequence(0, solidus);
        CharSequence second = value.subSequence(solidus + 1, value.length());

        if (firstIsPeriod) {
            end = parseTime(second);
            Period period = parsePeriod(first);
            start = end.minus(period);
        } else if (secondIsPeriod) {
            start = parseTime(first);
            Period period = parsePeriod(second);
            end = start.plus(period);
        } else {
            start = parseTime(first);
            end = parseTime(second);
        }

        return new DateInterval(start, end);
    }

    private static LocalDate parseTime(CharSequence value) {
        try {
            return LocalDate.parse(value, DATE_FORMATTER);
        } catch (DateTimeParseException e) {
            throw new DateIntervalParseException(e);
        }
    }

    private static Period parsePeriod(CharSequence value) {
        try {
            return Period.parse(value);
        } catch (DateTimeParseException e) {
            throw new DateIntervalParseException(e);
        }
    }

    public boolean contains(final LocalDate date) {
        return !date.isBefore(start) && !date.isAfter(end);
    }

    public boolean overlaps(final DateInterval otherInterval) {
        Objects.requireNonNull(otherInterval, "'otherInterval' must not be null");
        // (this.start <= otherInterval.end) and (this.end >= otherInterval.start)
        return !this.start.isAfter(otherInterval.end) && !this.end.isBefore(otherInterval.start);
    }

    public DateInterval withEnd(final LocalDate end) {
        return new DateInterval(this.start, end);
    }

    public int daysWithinInterval() {
        return (int) ChronoUnit.DAYS.between(this.getStart(), this.getEnd()) + 1;
    }

    public List<LocalDate> asListOfDays() {
        return Stream.iterate(this.getStart(), d -> d.plusDays(1))
                .limit(this.daysWithinInterval())
                .collect(Collectors.toList());
    }

    @Override
    @JsonValue  // This will be serialized JSON representation of this object
    public String toString() {
        return DATE_FORMATTER.format(start) + "/" + DATE_FORMATTER.format(end);
    }

    public String getStartFormatted() {
        return DATE_FORMATTER.format(start);
    }

    public String getEndFormatted() {
        return DATE_FORMATTER.format(end);
    }
}
