package com.yolt.accountsandtransactions.offloading;

import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NonNull;
import nl.ing.lovebird.extendeddata.common.CurrencyCode;
import nl.ing.lovebird.extendeddata.transaction.TransactionStatus;
import nl.ing.lovebird.providerdomain.YoltCategory;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.util.Map;
import java.util.Set;
import java.util.UUID;


@Data
@Builder
@EqualsAndHashCode(callSuper = false)
class OffloadableTransaction extends OffloadablePayload {
    private final String offloadType = "transaction";

    @NonNull
    private UUID userId;
    @NonNull
    private UUID accountId;
    @NonNull
    private LocalDate date;
    @NonNull
    private String id;
    @NonNull
    private TransactionStatus status;
    @NonNull
    private BigDecimal amount;
    @NonNull
    private CurrencyCode currency;

    private Instant timestamp;
    private String timeZone;
    private LocalDate bookingDate;
    private LocalDate valueDate;
    private Instant lastUpdatedTime;
    private String externalId;
    private String endToEndId;
    private String creditorName;
    private String creditorIban;
    private String creditorBban;
    private String creditorMaskedPan;
    private String creditorPan;
    private String creditorSortCodeAccountNumber;
    private String debtorName;
    private String debtorIban;
    private String debtorBban;
    private String debtorMaskedPan;
    private String debtorPan;
    private String debtorSortCodeAccountNumber;
    private CurrencyCode exchangeRateCurrencyFrom;
    private CurrencyCode exchangeRateCurrencyTo;
    private BigDecimal exchangeRateRate;
    private BigDecimal originalAmountAmount;
    private CurrencyCode originalAmountCurrency;
    private String bankTransactionCode;
    private String purposeCode;
    private Map<String, String> bankSpecific;
    private YoltCategory originalCategory;
    private String originalMerchantName;
    private String remittanceInformationStructured;
    private String remittanceInformationUnstructured;
    private Instant createdAt;
}
