package com.yolt.accountsandtransactions.inputprocessing;

/**
 * This testclass is used to test the edge cases where we don't take upstream or stored transactions into consideration
 * when matching transactions by external-id.
 */
class ExternalIdTransactionMatcherTest {
//
//    @Test
//    void given_storedTransactionsWithNoExternalId_then_willIgnore() {
//        List<Transaction> storedTransactions = List.of(
//                createTransactionWithId("1", transactionWithExternalIdAmountAndDescription("", 100, "trx 1"))
//        );
//
//        var newUpstreamTransaction = List.of(
//                transactionWithExternalIdAmountAndDescription("a", 100, "trx 1")
//        );
//
//        var matches = ExternalIdTransactionMatcher.match(newUpstreamTransaction, storedTransactions);
//
//        assertThat(matches.getMatched()).isEmpty();
//        assertThat(matches.getStoredDuplicates()).isEmpty();
//        assertThat(matches.getUpstreamDuplicates()).isEmpty();
//        assertThat(matches.getUnmatched())
//                .map(ProviderTransactionWithId::getProviderTransactionDTO)
//                .containsAll(newUpstreamTransaction);
//    }
//
//    @ParameterizedTest
//    @NullAndEmptySource
//    void given_upstreamWithNoExternalId_then_willIgnore(String upstreamExternalId) {
//        List<Transaction> storedTransactions = List.of(
//                createTransactionWithId("1", transactionWithExternalIdAmountAndDescription("a", 100, "trx 1"))
//        );
//
//        var newUpstreamTransaction = List.of(
//                transactionWithExternalIdAmountAndDescription(upstreamExternalId, 100, "trx 1")
//        );
//
//        var matches = ExternalIdTransactionMatcher.match(newUpstreamTransaction, storedTransactions);
//
//        assertThat(matches.getMatched()).isEmpty();
//        assertThat(matches.getStoredDuplicates()).isEmpty();
//        assertThat(matches.getUpstreamDuplicates()).isEmpty();
//        assertThat(matches.getUnmatched()).isEmpty();
//    }
}
