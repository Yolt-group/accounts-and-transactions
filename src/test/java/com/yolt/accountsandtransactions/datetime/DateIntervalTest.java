package com.yolt.accountsandtransactions.datetime;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import com.yolt.accountsandtransactions.datetime.exception.DateIntervalParseException;
import org.junit.jupiter.api.Test;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertThrows;

class DateIntervalTest {

    private static void assertDateInterval(DateInterval in, String start, String end) {
        assertThat(in.getStart()).isEqualTo(LocalDate.parse(start, DateTimeFormatter.ISO_LOCAL_DATE));
        assertThat(in.getEnd()).isEqualTo(LocalDate.parse(end, DateTimeFormatter.ISO_LOCAL_DATE));
    }

    @Test
    void shouldModifyEnd() {
        DateInterval dateInterval = new DateInterval(LocalDate.parse("2019-01-01"), LocalDate.parse("2019-01-24"));
        assertThat(dateInterval.withEnd(LocalDate.parse("2019-02-03")))
                .isEqualTo(new DateInterval(LocalDate.parse("2019-01-01"), LocalDate.parse("2019-02-03")));
    }

    @Test
    void shouldReturn31DaysForMonthlyInterval() {

        DateInterval dateInterval = new DateInterval(LocalDate.parse("2019-01-01"), LocalDate.parse("2019-01-31"));
        assertThat(dateInterval.daysWithinInterval()).isEqualTo(31);
    }

    @Test
    void shouldReturnOrderedListOfDates() {
        DateInterval dateInterval = new DateInterval(LocalDate.parse("2019-01-01"), LocalDate.parse("2019-01-07"));
        assertThat(dateInterval.asListOfDays()).containsExactly(
                LocalDate.parse("2019-01-01"),
                LocalDate.parse("2019-01-02"),
                LocalDate.parse("2019-01-03"),
                LocalDate.parse("2019-01-04"),
                LocalDate.parse("2019-01-05"),
                LocalDate.parse("2019-01-06"),
                LocalDate.parse("2019-01-07")
        );
    }

    @Test
    void shouldParseValidIntervals() {
        assertDateInterval(DateInterval.parse("2016-01-01/2016-08-17"), "2016-01-01", "2016-08-17");
        assertDateInterval(DateInterval.parse("2016-01-01/P1M"), "2016-01-01", "2016-02-01");
        assertDateInterval(DateInterval.parse("P1M/2016-01-01"), "2015-12-01", "2016-01-01");
    }

    @Test
    void shouldAcceptSameStartAndEndDate() {
        assertThatCode(() -> {
            final LocalDate date = LocalDate.of(2019, 12, 15);
            new DateInterval(date, date);
        }).doesNotThrowAnyException();
    }

    @Test
    void shouldRejectInvalidSolidus() {
        DateIntervalParseException exception = assertThrows(
                DateIntervalParseException.class,
                () -> DateInterval.parse("2016-01-01"));
        assertThat(exception).hasMessage("Input '2016-01-01' not in the format <date/period>/<date/period>");
    }

    @Test
    void shouldRejectIllegalDate() {
        DateIntervalParseException exception = assertThrows(
                DateIntervalParseException.class,
                () -> DateInterval.parse("2016-99-01/2016-08-17"));
        assertThat(exception).hasMessage("Text '2016-99-01' could not be parsed: Invalid value for MonthOfYear (valid values 1 - 12): 99");
    }

    @Test
    void shouldRejectIllegalPeriod() {
        DateIntervalParseException exception = assertThrows(
                DateIntervalParseException.class,
                () -> DateInterval.parse("2016-01-01/P1M1"));
        assertThat(exception).hasMessage("Text cannot be parsed to a Period");
    }

    @Test
    void shouldRejectEndBeforeStart() {
        assertThatIllegalArgumentException()
                .as("Start date 2016-04-01 is after end date 2016-03-31")
                .isThrownBy(() -> DateInterval.parse("2016-04-01/2016-03-31"));
    }

    @Test
    void shouldSerializeAsString() throws Exception {
        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.registerModule(new JavaTimeModule());

        DateInterval example = DateInterval.parse("2016-01-01/2016-08-17");
        assertThat(objectMapper.writeValueAsString(example)).isEqualTo("\"2016-01-01/2016-08-17\"");
    }

    @Test
    void shouldDeserializeFromString() throws Exception {
        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.registerModule(new JavaTimeModule());

        assertThat(objectMapper.readValue("{\"dateInterval\":\"2016-01-01/2016-08-17\"}", DateIntervalTest.TestClass.class))
                .isEqualTo(new DateIntervalTest.TestClass(DateInterval.parse("2016-01-01/2016-08-17")));
        assertThat(objectMapper.readValue("{\"dateInterval\":null}", DateIntervalTest.TestClass.class))
                .isEqualTo(new DateIntervalTest.TestClass(null));
    }

    @Test
    void shouldDetermineContainment() {
        DateInterval di = DateInterval.parse("2019-05-01/2019-06-30");
        assertAll(
                () -> assertThat(di.contains(LocalDate.of(2019, 4, 30))).isFalse(),
                () -> assertThat(di.contains(LocalDate.of(2019, 5, 1))).isTrue(),
                () -> assertThat(di.contains(LocalDate.of(2019, 5, 31))).isTrue(),
                () -> assertThat(di.contains(LocalDate.of(2019, 6, 1))).isTrue(),
                () -> assertThat(di.contains(LocalDate.of(2019, 6, 30))).isTrue(),
                () -> assertThat(di.contains(LocalDate.of(2019, 7, 1))).isFalse()
        );
    }

    @Test
    void shouldDetermineOverlap() {
        DateInterval di = DateInterval.parse("2019-05-01/2019-05-31");

        assertAll(
                // Same intervals
                () -> assertThat(di.overlaps(di)).isTrue(),
                // Half overlaps
                () -> assertThat(di.overlaps(DateInterval.parse("2019-04-15/2019-05-15"))).isTrue(),
                () -> assertThat(di.overlaps(DateInterval.parse("2019-05-15/2019-06-15"))).isTrue(),
                // First day overlap
                () -> assertThat(di.overlaps(DateInterval.parse("2019-04-01/2019-05-01"))).isTrue(),
                // Last day overlap
                () -> assertThat(di.overlaps(DateInterval.parse("2019-05-31/2019-06-30"))).isTrue(),
                // Adjacent but not overlapping
                () -> assertThat(di.overlaps(DateInterval.parse("2019-04-01/2019-04-30"))).isFalse(),
                () -> assertThat(di.overlaps(DateInterval.parse("2019-06-01/2019-06-30"))).isFalse(),
                // Not overlapping
                () -> assertThat(di.overlaps(DateInterval.parse("2019-03-01/2019-03-31"))).isFalse(),
                () -> assertThat(di.overlaps(DateInterval.parse("2019-07-01/2019-07-31"))).isFalse()
        );
    }

    @Getter
    @AllArgsConstructor
    @EqualsAndHashCode
    private static class TestClass {
        private DateInterval dateInterval;
    }
}
