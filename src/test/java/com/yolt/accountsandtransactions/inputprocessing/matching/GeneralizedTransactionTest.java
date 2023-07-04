package com.yolt.accountsandtransactions.inputprocessing.matching;

import com.yolt.accountsandtransactions.inputprocessing.matching.GeneralizedTransaction.ProviderGeneralizedTransaction;
import com.yolt.accountsandtransactions.inputprocessing.matching.GeneralizedTransaction.StoredGeneralizedTransaction;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.ZoneOffset;

import static com.yolt.accountsandtransactions.TestBuilders.createTransactionWithId;
import static com.yolt.accountsandtransactions.TestBuilders.transactionWithExternalIdAmountAndDate;
import static org.assertj.core.api.Assertions.assertThat;

class GeneralizedTransactionTest {

    @Test
    void equatableTransactionShouldEqual() {
        var now = Instant.EPOCH.atZone(ZoneOffset.UTC);
        var providerTransaction = transactionWithExternalIdAmountAndDate("a", new BigDecimal("-100.1"), now);
        var storedTransaction = createTransactionWithId("1", transactionWithExternalIdAmountAndDate("a", new BigDecimal("-100.10"), now));

        var storedGeneralizedTransaction = new StoredGeneralizedTransaction(storedTransaction);
        var providerGeneralizedTransaction = new ProviderGeneralizedTransaction(providerTransaction);

        assertThat(providerGeneralizedTransaction.getExternalId()).isEqualTo(storedGeneralizedTransaction.getExternalId());
        assertThat(providerGeneralizedTransaction.getAmountInCents()).isEqualTo(storedGeneralizedTransaction.getAmountInCents());
        assertThat(providerGeneralizedTransaction.getDate()).isEqualTo(storedGeneralizedTransaction.getDate());
        assertThat(providerGeneralizedTransaction.getStatus()).isEqualTo(storedGeneralizedTransaction.getStatus());
        assertThat(providerGeneralizedTransaction.getBookingDate()).isEqualTo(storedGeneralizedTransaction.getBookingDate());
        assertThat(providerGeneralizedTransaction.getTimestamp()).isEqualTo(storedGeneralizedTransaction.getTimestamp());
        assertThat(providerGeneralizedTransaction.getDebtorName()).isEqualTo(storedGeneralizedTransaction.getDebtorName());
        assertThat(providerGeneralizedTransaction.getDebtorAccountNr()).isEqualTo(storedGeneralizedTransaction.getDebtorAccountNr());
        assertThat(providerGeneralizedTransaction.getCreditorName()).isEqualTo(storedGeneralizedTransaction.getCreditorName());
        assertThat(providerGeneralizedTransaction.getCreditorAccountNr()).isEqualTo(storedGeneralizedTransaction.getCreditorAccountNr());
        assertThat(providerGeneralizedTransaction.getDescription()).isEqualTo(storedGeneralizedTransaction.getDescription());


    }

}