package com.yolt.accountsandtransactions.transactions.cycles;

import com.yolt.accountsandtransactions.BaseIntegrationTest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.List;
import java.util.UUID;

import static com.yolt.accountsandtransactions.TestBuilders.*;
import static java.util.Collections.singleton;
import static java.util.UUID.randomUUID;
import static java.util.function.Function.identity;
import static org.assertj.core.api.Assertions.assertThat;

public class TransactionCyclesServiceIntegrationTest extends BaseIntegrationTest {

    @Autowired
    TransactionCyclesService transactionCyclesService;

    @BeforeEach
    void onStart() {
        // Add some noise (random user, 10 random cycle-ids)
        bulkCreateTransactionCycles(10, UUID::randomUUID,
                (builder, i) -> builder, cycle -> {
                    transactionCyclesService.saveBatch(List.of(cycle));
                    return cycle;
                });
    }

    @Test
    void testSaveAndFind() {
        var userId = randomUUID();
        var cycleId = randomUUID();

        createTransactionCycle(() -> userId,
                (builder, i) -> builder
                        .cycleId(cycleId)
                        .counterparty("Ajax"), cycle -> {
                    transactionCyclesService.saveBatch(List.of(cycle));
                    return cycle;
                });

        var result = transactionCyclesService.find(userId, cycleId);

        assertThat(result).isNotEmpty();
        var foundTransactionCycle = result.get();
        assertThat(foundTransactionCycle.getCycleId()).isEqualTo(cycleId);
        assertThat(foundTransactionCycle.getCounterparty()).isEqualTo("Ajax");
    }

    @Test
    void testGetAll() {
        var userId = randomUUID();

        bulkCreateTransactionCycles(10, () -> userId,
                (builder, i) -> builder.cycleId(new UUID(1, i)), cycle -> {
                    transactionCyclesService.saveBatch(List.of(cycle));
                    return cycle;
                });

        List<TransactionCycle> all = transactionCyclesService.getAll(userId);

        assertThat(all).hasSize(10);
        assertThat(all).allMatch(transactionCycleV2 -> transactionCycleV2.getCycleId().getMostSignificantBits() == 1); // assert UUID(1,x)
    }

    @Test
    void testReconsile() {
        var userId = UUID.randomUUID();

        // add 2 existing cycles to the database
        var existing1 = createRandomTransactionCycle(userId).toBuilder().cycleId(new UUID(1, 0)).build();
        var existing2 = createRandomTransactionCycle(userId).toBuilder().cycleId(new UUID(1, 1)).build();
        transactionCyclesService.saveBatch(List.of(existing1, existing2));

        // create cycle changes
        var new1 = createRandomTransactionCycle(userId);
        var updated1 = createRandomTransactionCycle(userId).toBuilder().cycleId(new UUID(1, 0)).counterparty("changed").build(); // existing1

        var changeSet = TransactionCycleChangeSet.builder()
                .added(singleton(new1))
                .updated(singleton(updated1))
                .deleted(singleton(existing2))
                .build();

        // reconsile changes in the database
        transactionCyclesService.reconsile(changeSet);

        List<TransactionCycle> all = transactionCyclesService.getAll(userId);
        assertThat(all).hasSize(3);

        assertThat(transactionCyclesService.find(userId, new1.getCycleId())).contains(new1);
        assertThat(transactionCyclesService.find(userId, existing1.getCycleId())).contains(updated1);
        assertThat(transactionCyclesService.find(userId, existing2.getCycleId())).contains(existing2.toBuilder().expired(true).build());
    }

    @Test
    void testUpsert() {
        var userId = UUID.randomUUID();

        var upsertedCycle = transactionCyclesService.upsert(createTransactionCycle(() -> userId, (builder, ignore) -> builder, identity()));

        var readCycle = transactionCyclesService.find(userId, upsertedCycle.getCycleId());
        assertThat(readCycle).contains(upsertedCycle);
    }

    @Test
    void textExpire() {
        var userId = UUID.randomUUID();

        // save non expired
        var newCycle = createTransactionCycle(() -> userId, (builder, ignore) -> builder.expired(false), transactionCycle -> {
            transactionCyclesService.saveBatch(List.of(transactionCycle));
            return transactionCycle;
        });

        // expire
        transactionCyclesService.expire(userId, newCycle.getCycleId());

        var readCycle = transactionCyclesService.find(userId, newCycle.getCycleId());
        assertThat(readCycle).contains(newCycle.toBuilder().expired(true).build()); // assert that the read back cycle is equivalent to the new cycle with expired set to true
        assertThat(readCycle.get().isExpired()).isTrue();
    }
}