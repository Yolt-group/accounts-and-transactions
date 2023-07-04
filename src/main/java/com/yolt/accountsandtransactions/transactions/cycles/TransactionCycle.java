package com.yolt.accountsandtransactions.transactions.cycles;

import com.datastax.driver.mapping.annotations.ClusteringColumn;
import com.datastax.driver.mapping.annotations.Column;
import com.datastax.driver.mapping.annotations.PartitionKey;
import com.datastax.driver.mapping.annotations.Table;
import com.yolt.accountsandtransactions.inputprocessing.enrichments.api.cycles.DsTransactionCycle;
import lombok.*;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.Period;
import java.util.Currency;
import java.util.Set;
import java.util.UUID;

@AllArgsConstructor
@NoArgsConstructor
@Table(name = "transaction_cycles_v2")
@Data
@Builder(toBuilder = true)
public class TransactionCycle {

    @NonNull
    @PartitionKey
    @Column(name = "user_id")
    private UUID userId;

    @NonNull
    @ClusteringColumn
    @Column(name = "cycle_id")
    private UUID cycleId;

    @NonNull
    @Column(name = "cycle_type")
    private CycleType cycleType;

    @NonNull
    @Column(name = "amount")
    private BigDecimal amount;

    @NonNull
    @Column(name = "currency")
    private String currency;

    @NonNull
    @Column(name = "period")
    private String period;

    @Column(name = "model_amount")
    private BigDecimal modelAmount;

    @Column(name = "model_currency")
    private String modelCurrency;

    @Column(name = "model_period")
    private String modelPeriod;

    @Column(name = "predicted_occurrences")
    private Set<LocalDate> predictedOccurrences;

    @Column(name = "label")
    private String label;

    @Column(name = "subscription")
    private boolean subscription;

    @NonNull
    @Column(name = "counterparty")
    private String counterparty;

    /**
     * Indicates that this {@see TransactionCycleV2} is no longer detected.
     */
    @Column(name = "expired")
    private boolean expired;

    public static TransactionCycle fromDatascienceTransactionCycle(
            final @NonNull UUID userId,
            final @NonNull DsTransactionCycle dsTransactionCycle) {
        return TransactionCycle.builder()
                .userId(userId)
                .cycleId(dsTransactionCycle.getId())
                .cycleType(dsTransactionCycle.getAmount().signum() >= 0 ? CycleType.CREDITS : CycleType.DEBITS)
                .amount(dsTransactionCycle.getAmount())
                .currency(dsTransactionCycle.getCurrency().getCurrencyCode())
                .period(dsTransactionCycle.getPeriod().toString())
                .modelAmount(dsTransactionCycle.getModelParameters()
                        .map(DsTransactionCycle.ModelParameters::getAmount)
                        .orElse(null))
                .modelCurrency(dsTransactionCycle.getModelParameters()
                        .map(DsTransactionCycle.ModelParameters::getCurrency)
                        .map(Currency::getCurrencyCode)
                        .orElse(null))
                .modelPeriod(dsTransactionCycle.getModelParameters()
                        .map(DsTransactionCycle.ModelParameters::getPeriod)
                        .map(Period::toString)
                        .orElse(null))
                .counterparty(dsTransactionCycle.getCounterparty())
                .label(dsTransactionCycle.getLabel().orElse(null))
                .predictedOccurrences(dsTransactionCycle.getPredictedOccurrences())
                .subscription(dsTransactionCycle.isSubscription())
                .expired(false)
                .build();
    }
}
