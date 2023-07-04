package com.yolt.accountsandtransactions.transactions;

import com.yolt.accountsandtransactions.TestBuilders;
import nl.jqno.equalsverifier.EqualsVerifier;
import nl.jqno.equalsverifier.Warning;
import org.junit.jupiter.api.Test;

import java.time.LocalDate;

import static org.assertj.core.api.Assertions.assertThat;

class TransactionTest {

    /**
     * Note:
     * <p>
     * The {@link Transaction} implements a custom equals method to compare two transactions for equivalence.
     * The equals method is used in the {@link com.yolt.accountsandtransactions.datascience.TransactionSyncService} to
     * determine if two transactions (one from the provider and one locally) are different in terms of properties.
     * <p>
     * If new fields gets added to the Transaction class, think about the equals method and if the new field should be used
     * when de-duplicating existing transactions.
     */
    @Test
    public void verifyEqualsContractIncludes() {

        EqualsVerifier.configure()
                .forClass(Transaction.class)
                .suppress(Warning.NONFINAL_FIELDS)
                .withOnlyTheseFields(
                        "userId",
                        "accountId",
                        "date",
                        "id",
                        "timestamp",
                        "timeZone",
                        "bookingDate",
                        "valueDate",
                        "externalId",
                        "status",
                        "amount",
                        "currency",
                        "description",
                        "endToEndId",
                        "creditorName",
                        "creditorIban",
                        "creditorBban",
                        "creditorMaskedPan",
                        "creditorPan",
                        "creditorSortCodeAccountNumber",
                        "debtorName",
                        "debtorIban",
                        "debtorBban",
                        "debtorMaskedPan",
                        "debtorPan",
                        "debtorSortCodeAccountNumber",
                        "exchangeRateCurrencyFrom",
                        "exchangeRateCurrencyTo",
                        "exchangeRateRate",
                        "originalAmountAmount",
                        "originalAmountCurrency",
                        "bankTransactionCode",
                        "purposeCode",
                        "bankSpecific",
                        "originalCategory",
                        "originalMerchantName",
                        "remittanceInformationStructured",
                        "remittanceInformationUnstructured"
                )
                .verify();
    }

    @Test
    public void verifyEqualsContractExcludes() {

        EqualsVerifier.configure()
                .forClass(Transaction.class)
                .suppress(Warning.NONFINAL_FIELDS)
                .withIgnoredFields(
                        "enrichmentCategory",
                        "enrichmentMerchantName",
                        "enrichmentCycleId",
                        "enrichmentLabels",
                        "lastUpdatedTime",
                        "createdAt",
                        "fillType"
                )
                .verify();
    }

    @Test
    public void verifyEqual() {
        Transaction transaction = TestBuilders.createTransactionTemplate();

        assertThat(transaction).isEqualTo(transaction);
    }

    @Test
    public void verifyNotEqual() {
        Transaction transaction = TestBuilders.createTransactionTemplate();

        assertThat(transaction).isNotEqualTo(
                transaction.toBuilder()
                        .date(LocalDate.now())
                        .build());
    }
}