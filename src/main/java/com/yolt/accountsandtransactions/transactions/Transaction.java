package com.yolt.accountsandtransactions.transactions;

import com.datastax.driver.mapping.annotations.*;
import lombok.*;
import nl.ing.lovebird.cassandra.codec.LocalDateTypeCodec;
import nl.ing.lovebird.extendeddata.common.CurrencyCode;
import nl.ing.lovebird.extendeddata.transaction.TransactionStatus;
import nl.ing.lovebird.providerdomain.YoltCategory;
import org.springframework.lang.Nullable;

import javax.validation.constraints.NotNull;
import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

@AllArgsConstructor
@NoArgsConstructor
@Table(name = "transactions")
@Data
@Builder(toBuilder = true)
public final class Transaction {

    @NotNull
    @PartitionKey
    @Column(name = "user_id")
    private UUID userId;
    @NotNull
    @ClusteringColumn(0)
    @Column(name = "account_id")
    private UUID accountId;
    @NotNull
    @ClusteringColumn(1)
    @Column(name = "date", codec = LocalDateTypeCodec.class)
    private LocalDate date;
    @NotNull
    @ClusteringColumn(2)
    @Column(name = "id")
    private String id;

    /**
     * When did the transaction take place?
     */
    @Column(name = "transaction_timestamp")
    private Instant timestamp;

    @Column(name = "time_zone")
    private String timeZone;

    @Column(name = "booking_date", codec = LocalDateTypeCodec.class)
    private LocalDate bookingDate; // optional
    @Column(name = "value_date", codec = LocalDateTypeCodec.class)
    private LocalDate valueDate; // optional

    @Column(name = "external_id")
    private String externalId;
    @NotNull
    @Column(name = "status")
    private TransactionStatus status;
    @NotNull
    @Column(name = "amount")
    private BigDecimal amount;
    @NotNull
    @Column(name = "currency")
    private CurrencyCode currency;
    @NotNull
    @Column(name = "description")
    private String description;
    @Column(name = "end_to_end_id")
    private String endToEndId;
    @Column(name = "creditor_name")
    private String creditorName;
    @Column(name = "creditor_iban")
    private String creditorIban;
    @Column(name = "creditor_bban")
    private String creditorBban;
    @Column(name = "creditor_masked_pan")
    private String creditorMaskedPan;
    @Column(name = "creditor_pan")
    private String creditorPan;
    @Column(name = "creditor_sort_code_account_number")
    private String creditorSortCodeAccountNumber;
    @Column(name = "debtor_name")
    private String debtorName;
    @Column(name = "debtor_iban")
    private String debtorIban;
    @Column(name = "debtor_bban")
    private String debtorBban;
    @Column(name = "debtor_masked_pan")
    private String debtorMaskedPan;
    @Column(name = "debtor_pan")
    private String debtorPan;
    @Column(name = "debtor_sort_code_account_number")
    private String debtorSortCodeAccountNumber;
    @Column(name = "exchange_rate_currency_from")
    private CurrencyCode exchangeRateCurrencyFrom;
    @Column(name = "exchange_rate_currency_to")
    private CurrencyCode exchangeRateCurrencyTo;
    @Column(name = "exchange_rate_currency_rate")
    private BigDecimal exchangeRateRate;
    @Column(name = "original_amount_amount")
    private BigDecimal originalAmountAmount;
    @Column(name = "original_amount_currency")
    private CurrencyCode originalAmountCurrency;

    @Column(name = "bank_transaction_code")
    private String bankTransactionCode;
    @Column(name = "purpose_code")
    private String purposeCode;
    @Column(name = "bank_specific")
    private Map<String, String> bankSpecific;


    /**
     * This field is not to be exposed, but is necessary for data science.
     * This serves as input for the enrichment.category.
     * This is the original category provided by providers/scraper/the bank.
     */
    @Column(name = "original_category")
    private YoltCategory originalCategory;
    /**
     * This field is not to be exposed, but is necessary for data science.
     * This serves as input for the enrichment.category.
     * This is the original category provided by providers/scraper/the bank.
     */
    @Column(name = "original_merchant_name")
    private String originalMerchantName;

    @Column(name = "remittance_information_structured")
    private String remittanceInformationStructured;

    @Column(name = "remittance_information_unstructured")
    private String remittanceInformationUnstructured;

    // ------------------- EXCLUDED FROM EQUALS/HASHCODE

    /**
     * These 4 enrichment fields are deprecated as they are stored separately in
     * {@link com.yolt.accountsandtransactions.transactions.enrichments.TransactionEnrichments}
     */
    @Deprecated
    @Column(name = "enrichment_category")
    @EqualsAndHashCode.Exclude
    private String enrichmentCategory;

    @Deprecated
    @Column(name = "enrichment_merchant_name")
    @EqualsAndHashCode.Exclude
    private String enrichmentMerchantName;

    @Deprecated
    @Column(name = "enrichment_cycle_id")
    @EqualsAndHashCode.Exclude
    private UUID enrichmentCycleId;

    @Deprecated
    @Column(name = "enrichment_labels")
    @EqualsAndHashCode.Exclude
    private Set<String> enrichmentLabels;


    /**
     * When was the last time we updated this transaction?
     */
    @Column(name = "last_updated_time")
    @EqualsAndHashCode.Exclude
    private Instant lastUpdatedTime;

    /**
     * Non nullable getter overridden at {@link Transaction#getCreatedAtOrEPOCH()}
     */
    @Nullable
    @Column(name = "created_at")
    @EqualsAndHashCode.Exclude
    private Instant createdAt;

    /**
     * The fill type of the transaction.
     */
    @Nullable
    @Column(name = "fill_type")
    @EqualsAndHashCode.Exclude
    private FillType fillType;

    /**
     * Returns the date/time (as UTC instant) when this transaction was created within the YTS system.
     * <p/>
     * This value defaults to the EPOCH for transactions older than the 25th of March 2021
     */
    @Transient
    public @NonNull Instant getCreatedAtOrEPOCH() {
        return Optional.ofNullable(createdAt).orElse(Instant.EPOCH);
    }

    /**
     * Returns the fill type for this transaction (if any)
     * <p/>
     * This value defaults to the REGULAR for transactions older than the 5th of November 2021
     */
    @Transient
    public @NonNull FillType getFillTypeOrDefault() {
        return Optional.ofNullable(fillType).orElse(FillType.REGULAR);
    }

    public enum FillType {
        /**
         * The transaction was filled normally.
         */
        REGULAR,

        /**
         * The transaction was backfilled from history/ (re-)consent.
         */
        BACKFILLED
    }
}
