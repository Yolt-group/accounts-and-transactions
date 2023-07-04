package com.yolt.accountsandtransactions.transactions;

import com.yolt.accountsandtransactions.BaseIntegrationTest;
import nl.ing.lovebird.extendeddata.transaction.TransactionStatus;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.time.Instant;
import java.time.LocalDate;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

import static com.yolt.accountsandtransactions.TestBuilders.bulkCreateTransactions;
import static org.assertj.core.api.Assertions.assertThat;

class TransactionRepositoryTest extends BaseIntegrationTest  {

    @Autowired
    TransactionRepository repository;

    @Test
    void given_ALotOfTransactions_then_transactionsAreFetchedInPaginatedWayForTrxsOnOrAfter() {
        var userId = UUID.randomUUID();
        var accountId = UUID.randomUUID();


        List<Transaction> transactions = bulkCreateTransactions(2000, () -> new TransactionService.TransactionPrimaryKey(userId, accountId, randomDateFromEpochTillNow(), UUID.randomUUID().toString(), TransactionStatus.BOOKED),
                (builder, i) -> builder, t -> t);
        repository.saveBatch(transactions, 500);

        List<Pair<TransactionStatus, Instant>> statusAndTimestampForTrxsOnOrAfter = repository.getStatusAndTimestampForTrxsOnOrAfter(userId, accountId, LocalDate.of(1970, 1, 1));

        assertThat(repository.getFetchSizeForInternalSummary()).isEqualTo(500);
        assertThat(statusAndTimestampForTrxsOnOrAfter).hasSize(2000);
    }

    private LocalDate randomDateFromEpochTillNow() {
        long minDay = LocalDate.of(1970, 1, 1).toEpochDay();
        long maxDay = LocalDate.now().toEpochDay();
        long randomDay = ThreadLocalRandom.current().nextLong(minDay, maxDay);
        return LocalDate.ofEpochDay(randomDay);
    }
}