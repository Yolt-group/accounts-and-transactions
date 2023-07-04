package com.yolt.accountsandtransactions.transactions.updates.api;

import com.yolt.accountsandtransactions.transactions.updates.updatesession.BulkUpdateSession;
import lombok.Builder;
import lombok.NonNull;
import lombok.Value;

import java.util.List;

import static java.util.Collections.emptyList;
import static java.util.stream.Collectors.toList;

@Value
public class SimilarTransactionsForUpdatesView {
    @NonNull
    BulkUpdateSession bulkUpdateSession;
    @NonNull
    List<SimilarTransactionGroupDTO> groups;

    @Builder
    public SimilarTransactionsForUpdatesView(@NonNull BulkUpdateSession bulkUpdateSession, List<SimilarTransactionGroupDTO> groups) {
        this.groups = groups == null ? emptyList() : groups.stream()
                .filter(it -> it.getTransactions() != null)
                .filter(it -> !it.getTransactions().isEmpty())
                .collect(toList());
        this.bulkUpdateSession = bulkUpdateSession;
    }
}
