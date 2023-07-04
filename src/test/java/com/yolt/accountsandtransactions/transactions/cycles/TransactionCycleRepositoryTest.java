package com.yolt.accountsandtransactions.transactions.cycles;

import com.yolt.accountsandtransactions.BaseIntegrationTest;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static com.yolt.accountsandtransactions.TestBuilders.bulkCreateTransactionCycles;
import static org.assertj.core.api.Assertions.assertThat;

class TransactionCycleRepositoryTest extends BaseIntegrationTest {

    @Autowired
    TransactionCycleRepository repository;

    @Test
    void getTransactionCycles() {
        var userId = UUID.randomUUID();
        bulkCreateTransactionCycles(10, () -> userId,
                (builder, i) -> builder.cycleId(new UUID(1, i)), cycle -> {
                    repository.saveBatch(List.of(cycle), 1);
                    return cycle;
                });
        List<TransactionCycle> transactionCycles = repository.getTransactionCycles(userId);
        assertThat(transactionCycles).hasSize(10);
    }

    @Test
    void findTransactionCycle() {
        var userId = UUID.randomUUID();
        bulkCreateTransactionCycles(10, () -> userId,
                (builder, i) -> builder.cycleId(new UUID(1, i)), cycle -> {
                    repository.saveBatch(List.of(cycle), 1);
                    return cycle;
                });
        Optional<TransactionCycle> transactionCycle = repository.findTransactionCycle(userId, new UUID(1, 5));
        assertThat(transactionCycle).isNotEmpty(); // uuid(1,5) does exist
        assertThat(transactionCycle.get().getCycleId()).isEqualTo(new UUID(1, 5));
    }

    @Test
    void findTransactionCycleNotFound() {
        var userId = UUID.randomUUID();
        bulkCreateTransactionCycles(10, () -> userId,
                (builder, i) -> builder.cycleId(new UUID(1, i)), cycle -> {
                    repository.saveBatch(List.of(cycle), 1);
                    return cycle;
                });
        Optional<TransactionCycle> transactionCycle = repository.findTransactionCycle(userId, new UUID(1, 100));
        assertThat(transactionCycle).isEmpty(); // uuid(1,100) does not exist
    }
}