package com.yolt.accountsandtransactions.transactions;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.yolt.accountsandtransactions.accounts.AccountReferencesDTO;
import com.yolt.accountsandtransactions.transactions.cycles.CycleType;
import com.yolt.accountsandtransactions.transactions.cycles.TransactionCycleDTO;
import io.swagger.v3.oas.annotations.media.ArraySchema;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Builder;
import lombok.NonNull;
import lombok.Value;
import nl.ing.lovebird.extendeddata.common.CurrencyCode;
import nl.ing.lovebird.extendeddata.transaction.TransactionStatus;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZonedDateTime;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

@Builder
@Value
@JsonInclude(JsonInclude.Include.NON_NULL)
@Schema
public class TransactionDTO {
    @Schema(required = true, description = "The identifier assigned by Yolt.", example = "20197208837131089204224A")
    String id;
    @Schema(description = "The identifier of the transaction at the bank.  This field is not guaranteed to be present. It is also not guaranteed to be unique.", example = "14H8IY710471984729847")
    String externalId;
    @Schema(required = true, description = "The account identifier.", example = "b86dd7b9-4886-41a5-a4fe-9ac348db0427")
    UUID accountId;
    @Schema(required = true, description = "Status of the transaction.", example = "BOOKED")
    TransactionStatus status;
    @Schema(required = true, description = "The date of the transaction. This is the date that is shown to the user by the bank, and not necessarily value date, booking date, ..", example = "2019-11-26")
    LocalDate date;
    @Schema(required = true, description = "The timestamp of the transaction with the time-offset included.", example = "2019-11-26T12:00:00.00000+02:00")
    ZonedDateTime timestamp;
    @Schema(description = "The Date when an entry is posted to an account on the ASPSPs books.")
    LocalDate bookingDate;
    @Schema(description = "The Date at which assets become available to the account owner in case of a credit.")
    LocalDate valueDate;
    @Schema(required = true, description = "The amount of the transaction.", example = "-22.33")
    BigDecimal amount;
    @Schema(required = true, description = "The currency of the transaction.", example = "EUR")
    CurrencyCode currency;
    @Deprecated
    @Schema(required = true, description = "A description of the transaction for display purposes. " +
            "This value is deprecated in favour of #remittanceInformationStructured and #remittanceInformationUnstructured " +
            "and will be removed in the future.", example = "Mc Donalds Spaklerweg Amsterdam")
    String description;
    @Schema(description = "Unique end to end identity.", example = "90705030")
    String endToEndId;
    CreditorDTO creditor;
    DebtorDTO debtor;
    @Schema(description = "Bank transaction code.", example = "PMNT-RCDT-ESCT")
    String bankTransactionCode;
    @Schema(description = "Purpose code.", example = "AGRT")
    String purposeCode;
    ExchangeRateDTO exchangeRate;
    OriginalAmountDTO originalAmount;
    EnrichmentDTO enrichment;
    @Schema(description = "The last time this transaction was updated by Yolt.")
    Instant lastUpdatedTime;
    @Schema(type = "object", description = "Bank specific fields. These are special bank specific fields that will be propagated on request only.")
    Map<String, String> bankSpecific;
    @Schema(description = "A reference (in a structured format) issued by the counterparty used to establish a link between the payment of an invoice and the invoice instance.")
    String remittanceInformationStructured;
    @Schema(description = "A reference (in an unstructured format) issued by the counterparty used to establish a link between the payment of an invoice and the invoice instance.")
    String remittanceInformationUnstructured;

    @Deprecated(forRemoval = true)
    @NonNull
    @JsonFormat(pattern = "yyyy-MM-dd'T'HH:mm:ssZ", timezone = "UTC")
    @Schema(required = true, description = "The date/time (as UTC instant) when this transaction was created within the YTS system. This value defaults to the EPOCH for transactions older then the 25th of March 2021. Deprecated, this field will be removed in the near future.", example = "2021-03-26T09:30:00.000Z")
    Instant createdAt;

    @Value
    @JsonInclude(JsonInclude.Include.NON_NULL)
    @Schema(description = "The original amount of the transaction. Provided when a payment was initiated in a " +
            "different currency than the account currency")
    static class OriginalAmountDTO {
        @Schema(required = true, description = "The amount in the original currency", example = "-24.33")
        BigDecimal amount;
        @Schema(required = true, description = "The currency of the transaction.", example = "USD")
        CurrencyCode currency;
    }

    @Value
    @JsonInclude(JsonInclude.Include.NON_NULL)
    @Schema(description = "The creditor of the transaction.")
    static class CreditorDTO {
        @Schema(description = "The name.", example = "Marie")
        String name;
        AccountReferencesDTO accountReferences;
    }

    @Value
    @JsonInclude(JsonInclude.Include.NON_NULL)
    @Schema(description = "The debtor of the transaction.")
    static class DebtorDTO {
        @Schema(description = "The name.", example = "John")
        String name;
        AccountReferencesDTO accountReferences;
    }

    @Value
    @JsonInclude(JsonInclude.Include.NON_NULL)
    @Schema(description = "Fields provided by Yolt's data enrichment pipeline. This field will appear for clients that " +
            "have data enrichment enabled on their account.")
    public static class EnrichmentDTO {
        @Schema(description = "The category (private persons).")
        String category;

        @Schema(description = "The category (SME)")
        String categorySME;

        @Deprecated
        @Schema(description = "The merchant. " +
                "This value is deprecated in favour of #counterparty and exists solely for api compatibility.")
        MerchantDTO merchant;

        @Schema(description = "The counterparty.")
        CounterpartyDTO counterparty;

        @Schema(description = "The id of a transaction-cycle if any.")
        UUID cycleId;

        @Deprecated
        DeprecatedTransactionCycleDTO cycle = null;

        @ArraySchema(arraySchema = @Schema(description = "Extra labels detected by the model, for example 'salary' or 'refund'."))
        Set<String> labels;

        @Value
        @Deprecated
        @JsonInclude(JsonInclude.Include.NON_NULL)
        @Schema(description = "The merchant. " +
                "This type is deprecated in favour of CounterpartyDTO and exists solely for api compatibility.")
        public static class MerchantDTO {
            @Schema(description = "Name of the merchant.")
            public String name;
        }

        @Value
        @JsonInclude(JsonInclude.Include.NON_NULL)
        @Schema(description = "The counterparty")
        public static class CounterpartyDTO {
            @Schema(description = "Name of the counterparty.")
            public String name;
            @Schema(description = "Indicates if the counterparty is a known merchant.")
            public boolean knownMerchant;
        }
    }

    @Value
    @JsonInclude(JsonInclude.Include.NON_NULL)
    @Schema(description = "The exchange rate of the transaction.")
    static class ExchangeRateDTO {
        @Schema(description = "The currency in which the transaction was started.", example = "EUR")
        CurrencyCode currencyFrom;
        @Schema(description = "The currency of the target account.", example = "USD")
        CurrencyCode currencyTo;
        @Schema(description = "The conversion rate of currencyFrom -> currencyTo.", example = "1.1")
        BigDecimal rate;
    }

    @Schema(description = "This value is deprecated in favour of the #cycleId and exists solely for api compatibility. This value is always absent.", allOf = TransactionCycleDTO.class)
    public static class DeprecatedTransactionCycleDTO extends TransactionCycleDTO {
        public DeprecatedTransactionCycleDTO(@NonNull UUID cycleId, @NonNull CycleType cycleType, @NonNull BigDecimal amount,
                                             @NonNull String currency, @NonNull String period, @NonNull Optional<ModelParameters> detected,
                                             @NonNull Set<String> predictedOccurrences, @NonNull Optional<String> label, boolean subscription,
                                             @NonNull String counterparty, boolean expired) {
            super(cycleId, cycleType, amount, currency, period, detected, predictedOccurrences, label, subscription, counterparty, expired);
        }
    }
}
