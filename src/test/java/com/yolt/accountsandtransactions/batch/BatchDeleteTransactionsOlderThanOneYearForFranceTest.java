package com.yolt.accountsandtransactions.batch;

import com.yolt.accountsandtransactions.transactions.Transaction;
import org.junit.jupiter.api.Test;

import java.time.LocalDate;
import java.time.Month;
import java.time.ZoneId;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

class BatchDeleteTransactionsOlderThanOneYearForFranceTest {

    BatchDeleteTransactionsOlderThanOneYearForFrance batch = new BatchDeleteTransactionsOlderThanOneYearForFrance("", null, null, null);

    LocalDate referenceDate = LocalDate.now(ZoneId.of("Europe/Paris"))
            .atStartOfDay()
            .minusDays(365)
            .toLocalDate();

    @Test
    public void testSafeguards() {
        // Fail to run on production environments other than yfb-prd
        assertThatCode(() -> {
            var b = new BatchDeleteTransactionsOlderThanOneYearForFrance("yfb-ext-prd", null, null, null);
            b.run(false);
        }).hasMessageStartingWith("refusing to run on environment \"yfb-ext-prd\"");

        assertThatCode(() -> {
            var b = new BatchDeleteTransactionsOlderThanOneYearForFrance("app-prd", null, null, null);
            b.run(false);
        }).hasMessageStartingWith("refusing to run on environment \"app-prd\"");

        // Permit a run on yfb-prd
        assertThatCode(() -> {
            var b = new BatchDeleteTransactionsOlderThanOneYearForFrance("yfb-prd", null, null, null);
            b.run(false);
        }).doesNotThrowAnyException();
    }

    @Test
    public void testIsOlderThanReferenceDate() {
        assertThat(batch.isOlderThanReferenceDate(Transaction.builder()
                .date(LocalDate.of(2020, Month.OCTOBER, 1))
                .build(), referenceDate)
        ).isTrue();
        assertThat(batch.isOlderThanReferenceDate(Transaction.builder()
                .date(referenceDate.plusDays(1))
                .build(), referenceDate)
        ).isFalse();
    }

}