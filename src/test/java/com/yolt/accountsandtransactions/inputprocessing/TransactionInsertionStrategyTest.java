package com.yolt.accountsandtransactions.inputprocessing;

import com.yolt.accountsandtransactions.TestBuilders;
import com.yolt.accountsandtransactions.inputprocessing.TransactionInsertionStrategy.Instruction;
import com.yolt.accountsandtransactions.transactions.Transaction;
import nl.ing.lovebird.extendeddata.transaction.TransactionStatus;
import nl.ing.lovebird.providerdomain.ProviderTransactionDTO;
import org.junit.jupiter.api.Test;

import java.time.LocalDate;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.Optional;

import static java.math.BigDecimal.TEN;
import static java.util.Collections.emptyMap;
import static nl.ing.lovebird.extendeddata.transaction.TransactionStatus.BOOKED;
import static nl.ing.lovebird.providerdomain.ProviderTransactionType.DEBIT;
import static nl.ing.lovebird.providerdomain.YoltCategory.GENERAL;
import static org.assertj.core.api.Assertions.assertThat;

class TransactionInsertionStrategyTest {

    @Test
    void shouldAppendDeleteToInstruction() {

        var bookedUpstreamA = new ProviderTransactionDTO("external-booked-A",
                ZonedDateTime.now(), TEN, BOOKED, DEBIT, "description", GENERAL, "merchant", null, emptyMap());
        var bookedUpstreamB = new ProviderTransactionDTO("external-booked-B",
                ZonedDateTime.now(), TEN, BOOKED, DEBIT, "description", GENERAL, "merchant", null, emptyMap());
        var bookedUpstreamC = new ProviderTransactionDTO("external-booked-C",
                ZonedDateTime.now(), TEN, BOOKED, DEBIT, "description", GENERAL, "merchant", null, emptyMap());

        Transaction booked = TestBuilders.createTransactionTemplate().toBuilder()
                .status(BOOKED)
                .build();

        Transaction pending = TestBuilders.createTransactionTemplate().toBuilder()
                .status(TransactionStatus.PENDING)
                .build();

        Instruction instruction = Instruction.builder()
                .transactionsToDelete(List.of(booked))
                .transactionsToInsert(List.of(new ProviderTransactionWithId(bookedUpstreamA, "A")))
                .transactionsToUpdate(List.of(new ProviderTransactionWithId(bookedUpstreamB, "B")))
                .transactionsToIgnore(List.of(new ProviderTransactionWithId(bookedUpstreamC, "C")))
                .metrics(null)
                .oldestTransactionChangeDate(Optional.of(LocalDate.EPOCH))
                .build();

        Instruction modified = instruction.appendToDelete(List.of(pending));
        assertThat(modified.getTransactionsToDelete())
                .containsExactly(booked, pending);
        assertThat(instruction.getTransactionsToInsert()).containsExactly(new ProviderTransactionWithId(bookedUpstreamA, "A"));
        assertThat(instruction.getTransactionsToUpdate()).containsExactly(new ProviderTransactionWithId(bookedUpstreamB, "B"));
        assertThat(instruction.getTransactionsToIgnore()).containsExactly(new ProviderTransactionWithId(bookedUpstreamC, "C"));
    }

}