package com.yolt.accountsandtransactions.transactions.updates.updatesession;

import com.yolt.accountsandtransactions.transactions.TransactionDTO;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.Optional;
import java.util.UUID;

import static java.util.Collections.emptyMap;
import static java.util.UUID.randomUUID;

/**
 * The BulkUpdateSessionService allows for associating a call to get related-transactions ({@link com.yolt.accountsandtransactions.transactions.updates.SimilarTransactionController})
 * to the actual updates ({@link com.yolt.accountsandtransactions.transactions.updates.TransactionsUpdateController}). Typically the information about the seed-transaction is kept in
 * the session but it also allows for details to be kept (if needed). Note that the entries in the {@link BulkUpdateSession} have a time-to-live so the call to do the actual update
 * should be made within that time-to-live.
 */
@RequiredArgsConstructor
@Service
@Slf4j
public class BulkUpdateSessionService {
    private final BulkUpdateSessionRepository bulkUpdateSessionRepository;

    public BulkUpdateSession startSession(@NonNull UUID userId, @NonNull TransactionDTO seedTransaction) {
        return bulkUpdateSessionRepository.persist(BulkUpdateSession.builder()
                .userId(userId)
                .updateSessionId(randomUUID())
                .accountId(seedTransaction.getAccountId())
                .date(seedTransaction.getDate())
                .transactionId(seedTransaction.getId())
                .details(emptyMap())
                .build());
    }

    public Optional<BulkUpdateSession> find(UUID userId, UUID sessionId) {
        return bulkUpdateSessionRepository.find(userId, sessionId);
    }
}
