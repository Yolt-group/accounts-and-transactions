package com.yolt.accountsandtransactions.transactions.updates;

import com.yolt.accountsandtransactions.datascience.DsShortTransactionKeyDTO;
import com.yolt.accountsandtransactions.datascience.preprocessing.PreProcessingServiceClient;
import com.yolt.accountsandtransactions.datascience.preprocessing.dto.DsSimilarTransactionsDTO;
import com.yolt.accountsandtransactions.transactions.TransactionService;
import com.yolt.accountsandtransactions.transactions.updates.api.SimilarTransactionGroupDTO;
import com.yolt.accountsandtransactions.transactions.updates.api.SimilarTransactionsForUpdatesView;
import com.yolt.accountsandtransactions.transactions.updates.api.TransactionsGroupedByAccountId;
import com.yolt.accountsandtransactions.transactions.updates.updatesession.BulkUpdateSession;
import com.yolt.accountsandtransactions.transactions.updates.updatesession.BulkUpdateSessionService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import nl.ing.lovebird.clienttokens.ClientUserToken;
import org.springframework.lang.NonNull;
import org.springframework.stereotype.Service;

import java.time.LocalDate;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

import static java.util.stream.Collectors.*;

@Service
@RequiredArgsConstructor
@Slf4j
public class SimilarTransactionsService {
    private final TransactionService transactionService;
    private final BulkUpdateSessionService bulkUpdateSessionService;
    private final PreProcessingServiceClient preProcessingServiceClient;

    Optional<BulkUpdateSession> startBulkUpdateSession(@NonNull UUID userId, @NonNull UUID accountId, @NonNull LocalDate date, @NonNull String transactionId) {
        return transactionService.getTransaction(userId, accountId, date, transactionId)
                .map(seedTransaction -> bulkUpdateSessionService.startSession(userId, seedTransaction));
    }

    public Optional<SimilarTransactionsForUpdatesView> getSimilarTransactions(@NonNull ClientUserToken clientUserToken, @NonNull BulkUpdateSession updateSession) {
        return preProcessingServiceClient.getSimilarTransactions(clientUserToken, updateSession.getAccountId(), updateSession.getTransactionId())
                .map(DsSimilarTransactionsDTO::getGroups)
                .map(groups -> SimilarTransactionsForUpdatesView.builder()
                        .bulkUpdateSession(updateSession)
                        .groups(groups.stream()
                                .filter(group -> group.getTransactions() != null)
                                .map(group -> SimilarTransactionGroupDTO.builder()
                                        .groupSelector(group.getGroupSelector())
                                        .groupDescription(group.getGroupSelector())
                                        .count(group.getTransactions().size())
                                        .transactions(groupTransactionsByAccountId(group.getTransactions()))
                                        .build())
                                .collect(toList()))
                        .build());
    }

    private Set<TransactionsGroupedByAccountId> groupTransactionsByAccountId(List<DsShortTransactionKeyDTO> transactionKeys) {
        return transactionKeys.stream()
                .collect(groupingBy(DsShortTransactionKeyDTO::getAccountId, mapping(DsShortTransactionKeyDTO::getTransactionId, toSet())))
                .entrySet()
                .stream()
                .map(it -> TransactionsGroupedByAccountId.builder()
                        .accountId(it.getKey())
                        .transactionIds(it.getValue())
                        .build())
                .collect(toSet());
    }
}
