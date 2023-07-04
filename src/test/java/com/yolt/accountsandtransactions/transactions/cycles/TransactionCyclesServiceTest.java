package com.yolt.accountsandtransactions.transactions.cycles;

import org.junit.jupiter.api.Test;

import java.time.LocalDate;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

import static com.yolt.accountsandtransactions.TestBuilders.bulkCreateTransactionCycles;
import static com.yolt.accountsandtransactions.transactions.cycles.TransactionCyclesService.calculateChangeSet;
import static java.math.BigDecimal.ONE;
import static java.math.BigDecimal.TEN;
import static java.util.Collections.emptyList;
import static java.util.UUID.randomUUID;
import static java.util.function.Function.identity;
import static org.assertj.core.api.Assertions.assertThat;

public class TransactionCyclesServiceTest {

    @Test
    public void testMapping() {
        var cycleId = randomUUID();
        var userId = randomUUID();

        var transactionCycleDTO = TransactionCycleDTO.fromTransactionCycle(TransactionCycle.builder()
                .userId(userId)
                .cycleId(cycleId)
                .amount(TEN)
                .counterparty("CounterParty")
                .currency("EUR")
                .cycleType(CycleType.CREDITS)
                .label("Avery")
                .period("DOT")
                .predictedOccurrences(Set.of(LocalDate.now().minusDays(1)))
                .subscription(true)
                .modelAmount(ONE)
                .modelCurrency("EUR")
                .modelPeriod("DOT")
                .expired(true)
                .build());

        assertThat(transactionCycleDTO.getCycleId()).isEqualTo(cycleId);
        assertThat(transactionCycleDTO.getCycleId()).isEqualTo(cycleId);
        assertThat(transactionCycleDTO.getAmount()).isEqualTo(TEN);
        assertThat(transactionCycleDTO.getCounterparty()).isEqualTo("CounterParty");
        assertThat(transactionCycleDTO.getCurrency()).isEqualTo("EUR");
        assertThat(transactionCycleDTO.getDetected()).contains(TransactionCycleDTO.ModelParameters.builder()
                .amount(ONE)
                .currency("EUR")
                .period("DOT")
                .build());
        assertThat(transactionCycleDTO.getCycleType()).isEqualTo(CycleType.CREDITS);
        assertThat(transactionCycleDTO.getLabel()).contains("Avery");
        assertThat(transactionCycleDTO.getPeriod()).isEqualTo("DOT");
        assertThat(transactionCycleDTO.getPredictedOccurrences()).isNotEmpty();
        assertThat(transactionCycleDTO.isSubscription()).isTrue();
        assertThat(transactionCycleDTO.isExpired()).isTrue();
    }

    @Test
    public void testMappingWithoutPredictedOccurrences() {
        var cycleId = randomUUID();
        var userId = randomUUID();

        var transactionCycleDTO = TransactionCycleDTO.fromTransactionCycle(TransactionCycle.builder()
                .userId(userId)
                .cycleId(cycleId)
                .amount(TEN)
                .counterparty("CounterParty")
                .currency("EUR")
                .cycleType(CycleType.CREDITS)
                .label("Avery")
                .period("DOT")
                .predictedOccurrences(null)
                .subscription(true)
                .modelAmount(ONE)
                .modelCurrency("EUR")
                .modelPeriod("DOT")
                .expired(true)
                .build());

        assertThat(transactionCycleDTO.getCycleId()).isEqualTo(cycleId);
        assertThat(transactionCycleDTO.getAmount()).isEqualTo(TEN);
        assertThat(transactionCycleDTO.getCounterparty()).isEqualTo("CounterParty");
        assertThat(transactionCycleDTO.getCurrency()).isEqualTo("EUR");
        assertThat(transactionCycleDTO.getDetected()).contains(TransactionCycleDTO.ModelParameters.builder()
                .amount(ONE)
                .currency("EUR")
                .period("DOT")
                .build());
        assertThat(transactionCycleDTO.getCycleType()).isEqualTo(CycleType.CREDITS);
        assertThat(transactionCycleDTO.getLabel()).isEqualTo(Optional.of("Avery"));
        assertThat(transactionCycleDTO.getPeriod()).isEqualTo("DOT");
        assertThat(transactionCycleDTO.getPredictedOccurrences()).isEmpty();
        assertThat(transactionCycleDTO.isSubscription()).isTrue();
        assertThat(transactionCycleDTO.isExpired()).isTrue();
    }

    @Test
    public void testMappingWithoutModelParameters() {
        var cycleId = randomUUID();
        var userId = randomUUID();

        var transactionCycleDTO = TransactionCycleDTO.fromTransactionCycle(TransactionCycle.builder()
                .userId(userId)
                .cycleId(cycleId)
                .amount(TEN)
                .counterparty("CounterParty")
                .currency("EUR")
                .cycleType(CycleType.CREDITS)
                .label("Avery")
                .period("DOT")
                .predictedOccurrences(null)
                .subscription(true)
                .expired(true)
                .build());

        assertThat(transactionCycleDTO.getCycleId()).isEqualTo(cycleId);
        assertThat(transactionCycleDTO.getAmount()).isEqualTo(TEN);
        assertThat(transactionCycleDTO.getCounterparty()).isEqualTo("CounterParty");
        assertThat(transactionCycleDTO.getCurrency()).isEqualTo("EUR");
        assertThat(transactionCycleDTO.getDetected()).isEmpty();
        assertThat(transactionCycleDTO.getCycleType()).isEqualTo(CycleType.CREDITS);
        assertThat(transactionCycleDTO.getLabel()).isEqualTo(Optional.of("Avery"));
        assertThat(transactionCycleDTO.getPeriod()).isEqualTo("DOT");
        assertThat(transactionCycleDTO.getPredictedOccurrences()).isEmpty();
        assertThat(transactionCycleDTO.isSubscription()).isTrue();
        assertThat(transactionCycleDTO.isExpired()).isTrue();
    }


    @Test
    void testFullyDistinctLocalAndUpstreamChangeSet() {
        var userId = UUID.randomUUID();

        // create two sets without any overlapping cycles
        List<TransactionCycle> local = bulkCreateTransactionCycles(10, () -> userId, (builder, i) -> builder, identity());
        List<TransactionCycle> upstream = bulkCreateTransactionCycles(10, () -> userId, (builder, i) -> builder, identity());

        TransactionCycleChangeSet transactionCycleChangeSet = calculateChangeSet(local, upstream);

        assertThat(transactionCycleChangeSet.addedAndUpdated()).hasSize(10);
        assertThat(transactionCycleChangeSet.addedAndUpdated()).containsAll(upstream);
        assertThat(transactionCycleChangeSet.deleted()).hasSize(10);
        assertThat(transactionCycleChangeSet.deleted()).containsAll(local);
    }

    @Test
    void testIdenticalLocalAndUpstreamChangeSet() {
        var userId = UUID.randomUUID();

        // create identical sets (100% overlap)
        List<TransactionCycle> local = bulkCreateTransactionCycles(10, () -> userId, (builder, i) -> builder.cycleId(new UUID(1, i)), identity());
        List<TransactionCycle> upstream = bulkCreateTransactionCycles(10, () -> userId, (builder, i) -> builder.cycleId(new UUID(1, i)), identity());

        TransactionCycleChangeSet transactionCycleChangeSet = calculateChangeSet(local, upstream);

        assertThat(transactionCycleChangeSet.addedAndUpdated()).hasSize(10);
        assertThat(transactionCycleChangeSet.addedAndUpdated()).containsAll(upstream);

        assertThat(transactionCycleChangeSet.deleted()).isEmpty();
    }

    @Test
    void testOnlyLocalNoUpstreamChangeSet() {
        var userId = UUID.randomUUID();

        List<TransactionCycle> local = bulkCreateTransactionCycles(10, () -> userId, (builder, i) -> builder.cycleId(new UUID(1, i)), identity());
        List<TransactionCycle> upstream = emptyList();

        TransactionCycleChangeSet transactionCycleChangeSet = calculateChangeSet(local, upstream);

        assertThat(transactionCycleChangeSet.addedAndUpdated()).hasSize(0);
        assertThat(transactionCycleChangeSet.deleted()).hasSize(10);
        assertThat(transactionCycleChangeSet.deleted()).containsAll(local);
    }

    @Test
    void testOnlyUpstreamNoLocalChangeSet() {
        var userId = UUID.randomUUID();

        List<TransactionCycle> local = emptyList();
        List<TransactionCycle> upstream = bulkCreateTransactionCycles(10, () -> userId, (builder, i) -> builder.cycleId(new UUID(1, i)), identity());

        TransactionCycleChangeSet transactionCycleChangeSet = calculateChangeSet(local, upstream);

        assertThat(transactionCycleChangeSet.deleted()).hasSize(0);
        assertThat(transactionCycleChangeSet.addedAndUpdated()).hasSize(10);
        assertThat(transactionCycleChangeSet.addedAndUpdated()).containsAll(upstream);
    }
}