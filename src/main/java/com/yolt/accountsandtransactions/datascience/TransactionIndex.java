package com.yolt.accountsandtransactions.datascience;

import com.yolt.accountsandtransactions.transactions.Transaction;
import nl.ing.lovebird.providerdomain.ProviderTransactionDTO;

import java.math.BigDecimal;
import java.util.*;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.toList;
import static nl.ing.lovebird.providerdomain.ProviderTransactionType.CREDIT;

/**
 * Helper class that does two things:
 * - offers facilities to look transactions up by either their transactionId or their externalId
 * - has the {@link #removeFromIndex)} operation to delete a transaction from all related data structures
 */
public class TransactionIndex {

    static BigDecimal _100 = new BigDecimal(100L);
    static BigDecimal negative_100 = _100.negate();

    private final List<ProviderTransactionDTO> transactions;
    private final Map<String, List<ProviderTransactionDTO>> byExternalId;
    private final Map<Long, List<ProviderTransactionDTO>> byAmountInCents;

    TransactionIndex(Collection<ProviderTransactionDTO> input) {
        transactions = new ArrayList<>(input);
        byExternalId = transactions.stream()
                .filter(t -> t.getExternalId() != null)
                .collect(groupingBy(ProviderTransactionDTO::getExternalId, Collectors.toList()));
        byAmountInCents = transactions.stream()
                .collect(groupingBy(TransactionIndex::amountInCents, toList()));
    }

    void removeFromIndex(ProviderTransactionDTO pt) {
        if (!transactions.contains(pt)) {
            throw new IllegalStateException("Can't delete a transaction that is not present.");
        }

        transactions.remove(pt);

        List<ProviderTransactionDTO> byAmount = byAmountInCents.get(amountInCents(pt));
        byAmount.remove(pt);
        if (byAmount.isEmpty()) {
            byAmountInCents.remove(amountInCents(pt));
        }

        if (pt.getExternalId() != null) {
            List<ProviderTransactionDTO> byExtId = byExternalId.get(pt.getExternalId());
            byExtId.remove(pt);
            if (byExtId.isEmpty()) {
                byExternalId.remove(pt.getExternalId());
            }
        }
    }

    /**
     * Changes to {@link #transactions} should only take place via {@link #removeFromIndex}, hence the call to {@link Collections#unmodifiableList}.
     */
    List<ProviderTransactionDTO> transactions() {
        return Collections.unmodifiableList(transactions);
    }

    /**
     * Changes to {@link #byExternalId} should only take place via {@link #removeFromIndex}, hence the call to {@link Collections#unmodifiableMap}.
     */
    public Map<String, List<ProviderTransactionDTO>> byExternalId() {
        return Collections.unmodifiableMap(byExternalId);
    }

    public List<ProviderTransactionDTO> findByAmount(Transaction t) {
        List<ProviderTransactionDTO> result = byAmountInCents.get(amountInCents(t));
        if (result == null) {
            return Collections.emptyList();
        }
        return Collections.unmodifiableList(result);
    }

    static long amountInCents(ProviderTransactionDTO t) {
        return t.getAmount()
                .multiply(t.getType() == CREDIT ? _100 : negative_100)
                .toBigIntegerExact()
                .longValueExact();
    }

    static long amountInCents(Transaction t) {
        return t.getAmount()
                .multiply(_100) // transaction amounts are signed
                .toBigIntegerExact()
                .longValueExact();
    }
}
