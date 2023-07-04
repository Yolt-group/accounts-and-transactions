package com.yolt.accountsandtransactions.datascience;

import com.yolt.accountsandtransactions.inputprocessing.ProviderTransactionWithId;
import com.yolt.accountsandtransactions.inputprocessing.TransactionInsertionStrategy;
import com.yolt.accountsandtransactions.inputprocessing.TransactionReconciliationResultMetrics;
import com.yolt.accountsandtransactions.transactions.Transaction;
import nl.ing.lovebird.extendeddata.transaction.TransactionStatus;
import nl.ing.lovebird.providerdomain.ProviderTransactionDTO;
import nl.ing.lovebird.providerdomain.ProviderTransactionType;
import nl.ing.lovebird.providerdomain.YoltCategory;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.stream.Collectors;

import static com.yolt.accountsandtransactions.TestBuilders.createTransactionWithId;
import static com.yolt.accountsandtransactions.datascience.TransactionSyncService.reconcileUpstreamTransactionsWithPersisted;
import static org.assertj.core.api.Assertions.assertThat;

public class TransactionReconciliationTest {

    private static ZonedDateTime _1970_01_01 = Instant.EPOCH.atZone(ZoneOffset.UTC);
    private static ZonedDateTime _1970_01_02 = _1970_01_01.plus(1, ChronoUnit.DAYS);
    private static ZonedDateTime _1970_01_03 = _1970_01_02.plus(1, ChronoUnit.DAYS);

    // <editor-fold description="corner cases">

    /**
     * Corner case.  Nothing in database (first fetch): everything from upstream should be inserted.
     */
    @Test
    public void given_nothingIsStored_when_reconcileTransactions_then_everythingFromUpstreamIsUpserted() {
        UUID random = UUID.randomUUID();
        List<Transaction> storedTransactions = Collections.emptyList();
        List<ProviderTransactionDTO> upstreamTransactions = List.of(
                trx("externalId", _1970_01_01, 100, "trx 1")
        );
        TransactionInsertionStrategy.Instruction instruction = reconcileUpstreamTransactionsWithPersisted(
                storedTransactions,
                upstreamTransactions,
                "dummy",
                Clock.systemUTC());
        assertThat(instruction.getTransactionsToInsert().stream()
                .map(ProviderTransactionWithId::getProviderTransactionDTO)
                .collect(Collectors.toList())
        ).containsExactlyElementsOf(upstreamTransactions);
        assertThat(instruction.getTransactionsToUpdate()).isEmpty();
        assertThat(instruction.getTransactionsToDelete()).isEmpty();

        assertThat(instruction.getMetrics()).isEqualToComparingFieldByField(TransactionReconciliationResultMetrics.builder()
                .provider("dummy")
                .upstreamTotal(1)
                .upstreamNew(1)
                .build());

        assertThat(instruction.getOldestTransactionChangeDate()).contains(_1970_01_01.toLocalDate());
    }

    // </editor-fold description="corner cases">

    // <editor-fold description="matching when bank providers externalId">

    /**
     * A matching transaction (by externalId) should have its transactionId altered to what we have stored, and then it can simply be upserted.
     */
    @Test
    public void given_upstreamTransactionWithMatchingExternalIdAndMismatchingTransactionId_when_reconcileTransactions_then_upstreamTransactionIsUpserted() {
        List<Transaction> storedTransactions = List.of(
                createTransactionWithId("1", trx("bank id 1", _1970_01_01, 100, "trx 1"))
        );
        List<ProviderTransactionDTO> upstreamTransactions = List.of(
                trx("bank id 1", _1970_01_01, 100, "trx 1 is updated")
        );
        TransactionInsertionStrategy.Instruction instruction = reconcileUpstreamTransactionsWithPersisted(
                storedTransactions,
                upstreamTransactions,
                "LLOYDS_BANK",
                Clock.systemUTC());
        assertThat(instruction.getTransactionsToInsert()).isEmpty();
        assertThat(instruction.getTransactionsToUpdate()).hasSize(1);
        assertThat(instruction.getTransactionsToUpdate().get(0))
                .isEqualTo(new ProviderTransactionWithId(upstreamTransactions.get(0), "1"));
        assertThat(instruction.getTransactionsToDelete()).isEmpty();

        assertThat(instruction.getMetrics()).isEqualToComparingFieldByField(TransactionReconciliationResultMetrics.builder()
                .provider("LLOYDS_BANK")
                .upstreamTotal(1)
                .storedTotal(1)
                .storedMatchedByExternalId(1)
                .build());

        assertThat(instruction.getOldestTransactionChangeDate()).contains(_1970_01_01.toLocalDate());
    }

    /**
     * A stored transaction for which we can find a match **on a different date** should be deleted and then be re-inserted on the new date.
     */
    @Test
    public void given_upstreamMatchOnDifferentDateWithSameExternalId_when_reconcileTransactions_then_storedTransactionShouldBeDeletedAndReinsertedOnNewDay() {
        List<Transaction> storedTransactions = List.of(
                createTransactionWithId("1", trx("b", _1970_01_03, 100, "trx 1"))
        );
        List<ProviderTransactionDTO> upstreamTransactions = List.of(
                trx("b", _1970_01_02, 100, "trx 1 updated")
        );
        TransactionInsertionStrategy.Instruction instruction = reconcileUpstreamTransactionsWithPersisted(
                storedTransactions,
                upstreamTransactions,
                "dummy",
                Clock.systemUTC());
        assertThat(instruction.getTransactionsToInsert()).isEmpty();
        assertThat(upstreamIds(instruction.getTransactionsToUpdate())).containsExactlyElementsOf(storedIds(storedTransactions));
        assertThat(instruction.getTransactionsToUpdate().get(0)).isEqualTo(new ProviderTransactionWithId(upstreamTransactions.get(0), "1"));
        assertThat(instruction.getTransactionsToDelete()).containsExactlyElementsOf(storedTransactions);

        assertThat(instruction.getMetrics()).isEqualToComparingFieldByField(TransactionReconciliationResultMetrics.builder()
                .provider("dummy")
                .upstreamTotal(1)
                .storedTotal(1)
                .storedMatchedByExternalId(1)
                .storedPrimaryKeyUpdated(1)
                .build());

        assertThat(instruction.getOldestTransactionChangeDate()).contains(_1970_01_02.toLocalDate());
    }

    /**
     * A stored transaction for which we can find a match **with a different status** should be deleted and then be re-inserted on the new date.
     */
    @Test
    public void given_upstreamMatchWithSameExternalIdButDifferentSTatus_when_reconcileTransactions_then_storedTransactionShouldBeDeletedAndReinsertedOnNewDay() {
        List<Transaction> storedTransactions = List.of(
                createTransactionWithId("1", trxPending("b", _1970_01_03, 100, "trx 1"))
        );
        List<ProviderTransactionDTO> upstreamTransactions = List.of(
                trx("b", _1970_01_03, 100, "trx 1 updated")
        );
        TransactionInsertionStrategy.Instruction instruction = reconcileUpstreamTransactionsWithPersisted(
                storedTransactions,
                upstreamTransactions,
                "dummy",
                Clock.systemUTC());
        assertThat(instruction.getTransactionsToInsert()).isEmpty();
        assertThat(upstreamIds(instruction.getTransactionsToUpdate())).containsExactlyElementsOf(storedIds(storedTransactions));
        assertThat(instruction.getTransactionsToUpdate().get(0)).isEqualTo(new ProviderTransactionWithId(upstreamTransactions.get(0), "1"));
        assertThat(instruction.getTransactionsToDelete()).containsExactlyElementsOf(storedTransactions);

        assertThat(instruction.getMetrics()).isEqualToComparingFieldByField(TransactionReconciliationResultMetrics.builder()
                .provider("dummy")
                .upstreamTotal(1)
                .storedTotal(1)
                .storedMatchedByExternalId(1)
                .storedPrimaryKeyUpdated(1)
                .storedPendingToBooked(1)
                .build());

        assertThat(instruction.getOldestTransactionChangeDate()).contains(_1970_01_03.toLocalDate());
    }

    /**
     * Test that the logic works if there are transactions with duplicate transactionIds in the database:
     * - one of them will simply be upserted
     * - one of them will be deleted and be re-inserted because the date changed
     */
    @Test
    public void given_storedTransactionsWithNonUniqueExternalIdsThatAreNotPresentInUpstream_when_reconcileTransactions_then_storedTransactionsArePartiallyDeleted() {
        List<Transaction> storedTransactions = List.of(
                createTransactionWithId("1", trx("bank id 1", _1970_01_01, 100, "trx 1")),
                createTransactionWithId("2", trx("bank id 2", _1970_01_01, 100, "trx 2")),
                createTransactionWithId("3", trx("bank id 1", _1970_01_02, 100, "trx 3")),
                createTransactionWithId("4", trx("bank id 4", _1970_01_01, 100, "trx 4"))
        );
        List<ProviderTransactionDTO> upstreamTransactions = List.of(
                trx("bank id 1", _1970_01_01, 100, "trx 1"), // ignored/ duplicate
                trx("bank id 2", _1970_01_01, 100, "trx 2"), // ignored/ duplicate
                trx("bank id 3", _1970_01_03, 100, "trx 3 is updated"),
                trx("bank id 4", _1970_01_01, 200, "trx 4") // amount updated
        );
        TransactionInsertionStrategy.Instruction instruction = reconcileUpstreamTransactionsWithPersisted(
                storedTransactions,
                upstreamTransactions,
                "LLOYDS_BANK",
                Clock.systemUTC());

        assertThat(instruction.getTransactionsToInsert()).hasSize(1);
        assertThat(instruction.getTransactionsToInsert().get(0).getProviderTransactionDTO()).isEqualTo(upstreamTransactions.get(2));

        assertThat(instruction.getTransactionsToUpdate()).hasSize(1);
        assertThat(instruction.getTransactionsToUpdate()).containsExactlyElementsOf(
                Collections.singletonList(
                        new ProviderTransactionWithId(upstreamTransactions.get(3), "4")
                )
        );

        assertThat(instruction.getTransactionsToIgnore()).containsExactlyElementsOf(
                Arrays.asList(
                        new ProviderTransactionWithId(upstreamTransactions.get(0), "1"),
                        new ProviderTransactionWithId(upstreamTransactions.get(1), "2")
                )
        );
        assertThat(instruction.getTransactionsToDelete()).containsExactlyElementsOf(Collections.singleton(storedTransactions.get(2)));

        assertThat(instruction.getMetrics()).isEqualToComparingFieldByField(TransactionReconciliationResultMetrics.builder()
                .provider("LLOYDS_BANK")
                .upstreamTotal(4)
                .upstreamNew(1)
                .upstreamUnchanged(2)
                .storedTotal(4)
                .storedMatchedByExternalId(3)
                .storedBookedNotMatched(1)
                .build());

        assertThat(instruction.getOldestTransactionChangeDate()).contains(_1970_01_01.toLocalDate());
    }

    /**
     * A matching transaction (by externalId) **but with a different date** should be inserted, and the matching stored transaction should
     * be deleted because date is part of the technical primary key.
     */
    @Test
    public void given_upstreamTransactionWithMatchingExternalIdAndMismatchingTransactionIdAndMismatchingDate_when_reconcileTransactions_then_storedTransactionIsDeletedAndUpstreamIsInserted() {
        List<Transaction> storedTransactions = List.of(
                createTransactionWithId("1", trx("bank id 1", _1970_01_03, 100, "trx 1"))
        );
        List<ProviderTransactionDTO> upstreamTransactions = List.of(
                trx("bank id 1", _1970_01_02, 100, "trx 1 is updated")
        );
        TransactionInsertionStrategy.Instruction instruction = reconcileUpstreamTransactionsWithPersisted(
                storedTransactions,
                upstreamTransactions,
                "LLOYDS_BANK",
                Clock.systemUTC());
        assertThat(instruction.getTransactionsToInsert()).isEmpty();
        assertThat(instruction.getTransactionsToUpdate()).hasSize(1);
        assertThat(instruction.getTransactionsToUpdate().get(0)).isEqualTo(new ProviderTransactionWithId(upstreamTransactions.get(0), "1"));
        assertThat(instruction.getTransactionsToDelete()).containsExactlyElementsOf(storedTransactions);

        assertThat(instruction.getMetrics()).isEqualToComparingFieldByField(TransactionReconciliationResultMetrics.builder()
                .provider("LLOYDS_BANK")
                .upstreamTotal(1)
                .storedTotal(1)
                .storedMatchedByExternalId(1)
                .storedPrimaryKeyUpdated(1)
                .build());

        assertThat(instruction.getOldestTransactionChangeDate()).contains(_1970_01_02.toLocalDate());
    }

    @Test
    public void given_weirdProvidersThatThinkAnIdentifierHasToBeEqualForEveryTransaction_when_reconcileTransaction_then_weDoAGoodJobEvenThoughTheInputIsGarbage() {
        List<Transaction> storedTransactions = List.of(
                createTransactionWithId("1", trx("bank id 1", _1970_01_01, 100, "trx 1")),
                createTransactionWithId("2", trx("bank id 1", _1970_01_02, 100, "trx 2"))
        );
        List<ProviderTransactionDTO> upstreamTransactions = List.of(
                trx("bank id 1", _1970_01_01, 100, "trx 1 is updated"),
                trx("bank id 1", _1970_01_02, 100, "trx 2 is updated"),
                trx("bank id 1", _1970_01_03, 100, "trx 3 is updated")
        );
        TransactionInsertionStrategy.Instruction instruction = reconcileUpstreamTransactionsWithPersisted(
                storedTransactions,
                upstreamTransactions,
                "LLOYDS_BANK",
                Clock.systemUTC());
        assertThat(instruction.getTransactionsToInsert()).hasSize(1);
        assertThat(instruction.getTransactionsToUpdate()).hasSize(2);
        assertThat(instruction.getTransactionsToUpdate().stream().map(ProviderTransactionWithId::getTransactionId).collect(Collectors.toList())).containsExactly("1", "2");
        assertThat(instruction.getTransactionsToDelete()).isEmpty();

        assertThat(instruction.getMetrics()).isEqualToComparingFieldByField(TransactionReconciliationResultMetrics.builder()
                .provider("LLOYDS_BANK")
                .upstreamTotal(3)
                .upstreamNew(1)
                .storedTotal(2)
                .storedMatchedByExternalId(2)
                .upstreamQualityDuplicateExternalIds(3)
                .build());

        assertThat(instruction.getOldestTransactionChangeDate()).contains(_1970_01_01.toLocalDate());
    }

    @Test
    public void given_multipleMatchesOnTheSameDayByExternalId_when_reconcileTransaction_transactionsAreMatchedOnDateAndAmount() {
        List<Transaction> storedTransactions = List.of(
                createTransactionWithId("1", trx("a", _1970_01_01, 100, "trx 1")),
                createTransactionWithId("2", trx("a", _1970_01_01, 200, "trx 2"))
        );
        List<ProviderTransactionDTO> upstreamTransactions = List.of(
                trx("a", _1970_01_01, 100, "trx 1 is updated"),
                trx("a", _1970_01_01, 200, "trx 2 is updated"),
                trx("a", _1970_01_01, 300, "trx 3"),
                trx("a", _1970_01_01, 400, "trx 4"),
                trx("a", _1970_01_01, 500, "trx 5")
        );
        TransactionInsertionStrategy.Instruction instruction = reconcileUpstreamTransactionsWithPersisted(
                storedTransactions,
                upstreamTransactions,
                "LLOYDS_BANK",
                Clock.systemUTC());
        assertThat(instruction.getTransactionsToInsert()).hasSize(3);
        assertThat(instruction.getTransactionsToUpdate()).hasSize(2);
        assertThat(instruction.getTransactionsToDelete()).isEmpty();

        assertThat(instruction.getMetrics()).isEqualToComparingFieldByField(TransactionReconciliationResultMetrics.builder()
                .provider("LLOYDS_BANK")
                .upstreamTotal(5)
                .upstreamNew(3)
                .storedTotal(2)
                .storedMatchedByExternalId(2)
                .upstreamQualityDuplicateExternalIds(5)
                .build());

        assertThat(instruction.getOldestTransactionChangeDate()).contains(_1970_01_01.toLocalDate());
    }

    // </editor-fold>

    // <editor-fold description="matching by attributes">

    @Test
    public void given_noExternalId_when_reconcileTransactions_then_transactionsAreMatchedAndUpdated() {
        List<Transaction> storedTransactions = List.of(
                createTransactionWithId("1", trx(null, _1970_01_01, 100, "trx 1")),
                createTransactionWithId("2", trx(null, _1970_01_01, 100, "trx 2")),
                createTransactionWithId("3", trx(null, _1970_01_02, 100, "trx 3")),
                createTransactionWithId("4", trx(null, _1970_01_03, 100, "trx 4"))
        );
        List<ProviderTransactionDTO> upstreamTransactions = List.of(
                trx(null, _1970_01_01, 100, "trx 1"),
                trx(null, _1970_01_01, 100, "trx 2"),
                trx(null, _1970_01_02, 100, "trx 3 is updated"),
                trx(null, _1970_01_03, 100, "trx 4 is updated")
        );
        TransactionInsertionStrategy.Instruction instruction = reconcileUpstreamTransactionsWithPersisted(
                storedTransactions,
                upstreamTransactions,
                "dummy",
                Clock.systemUTC());
        // Insert
        assertThat(instruction.getTransactionsToInsert()).isEmpty();

        // Update
        assertThat(upstreamIds(instruction.getTransactionsToUpdate())).containsExactlyElementsOf(List.of("3","4"));
        assertThat(instruction.getTransactionsToUpdate()).containsExactlyElementsOf(Arrays.asList(
                new ProviderTransactionWithId(upstreamTransactions.get(2), "3"),
                new ProviderTransactionWithId(upstreamTransactions.get(3), "4")
        ));

        // Ignore
        assertThat(upstreamIds(instruction.getTransactionsToIgnore())).containsExactlyElementsOf(List.of("1","2"));
        assertThat(instruction.getTransactionsToIgnore()).containsExactlyElementsOf(Arrays.asList(
                new ProviderTransactionWithId(upstreamTransactions.get(0), "1"),
                new ProviderTransactionWithId(upstreamTransactions.get(1), "2")
        ));

        // Delete
        assertThat(instruction.getTransactionsToDelete()).isEmpty();

        assertThat(instruction.getMetrics()).isEqualToComparingFieldByField(TransactionReconciliationResultMetrics.builder()
                .provider("dummy")
                .upstreamTotal(4)
                .upstreamUnchanged(2)
                .storedTotal(4)
                .storedMatchedByAttributesUnique(4)
                .upstreamQualityMissingExternalIds(4)
                .build());

        assertThat(instruction.getOldestTransactionChangeDate()).contains(_1970_01_02.toLocalDate());
    }

    @Test
    public void given_noExternalId_when_reconcileTransaction_transactionsAreMatchedOnAttributes() {
        List<Transaction> storedTransactions = List.of(
                createTransactionWithId("1", trx(null, _1970_01_01, 100, "trx 1")),
                createTransactionWithId("2", trx(null, _1970_01_01, 200, "trx 2"))
        );
        List<ProviderTransactionDTO> upstreamTransactions = List.of(
                trx(null, _1970_01_01, 100, "trx 1 is updated"),
                trx(null, _1970_01_01, 200, "trx 2 is updated"),
                trx(null, _1970_01_01, 300, "trx 3 is inserted"),
                trx(null, _1970_01_01, 400, "trx 4 is inserted"),
                trx(null, _1970_01_01, 500, "trx 5 is inserted")
        );
        TransactionInsertionStrategy.Instruction instruction = reconcileUpstreamTransactionsWithPersisted(
                storedTransactions,
                upstreamTransactions,
                "LLOYDS_BANK",
                Clock.systemUTC());
        assertThat(instruction.getTransactionsToInsert().stream().map(ProviderTransactionWithId::getProviderTransactionDTO).collect(Collectors.toList())).containsExactlyElementsOf(upstreamTransactions.subList(2, 5));
        assertThat(upstreamIds(instruction.getTransactionsToUpdate())).containsExactlyElementsOf(storedIds(storedTransactions.subList(0, 2)));
        assertThat(instruction.getTransactionsToUpdate()).containsExactlyElementsOf(Arrays.asList(
                new ProviderTransactionWithId(upstreamTransactions.get(0), "1"),
                new ProviderTransactionWithId(upstreamTransactions.get(1), "2")
        ));
        assertThat(instruction.getTransactionsToDelete()).isEmpty();

        assertThat(instruction.getMetrics()).isEqualToComparingFieldByField(TransactionReconciliationResultMetrics.builder()
                .provider("LLOYDS_BANK")
                .upstreamTotal(5)
                .upstreamNew(3)
                .storedTotal(2)
                .storedMatchedByAttributesUnique(2)
                .upstreamQualityMissingExternalIds(5)
                .build());

        assertThat(instruction.getOldestTransactionChangeDate()).contains(_1970_01_01.toLocalDate());
    }

    // </editor-fold>

    // <editor-fold description="utility functions">

    private static List<String> upstreamIds(Collection<ProviderTransactionWithId> trxs) {
        return trxs.stream().map(ProviderTransactionWithId::getTransactionId).collect(Collectors.toList());
    }

    private static List<String> storedIds(Collection<Transaction> trxs) {
        return trxs.stream().map(Transaction::getId).collect(Collectors.toList());
    }

    private static ProviderTransactionDTO trxPending(String externalId, ZonedDateTime dateTime, int amount, String description) {
        return trx(externalId, dateTime, amount, description).toBuilder()
                .status(TransactionStatus.PENDING)
                .build();
    }

    private static ProviderTransactionDTO trx(String externalId, ZonedDateTime dateTime, int amount, String description) {
        return ProviderTransactionDTO.builder()
                .externalId(externalId)
                .dateTime(dateTime)
                .amount(new BigDecimal(amount))
                .status(TransactionStatus.BOOKED)
                .type(ProviderTransactionType.CREDIT)
                .description(description)
                .category(YoltCategory.GENERAL)
                .build();
    }
    // </editor-fold>

}
