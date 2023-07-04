package com.yolt.accountsandtransactions.inputprocessing;

import com.yolt.accountsandtransactions.datascience.TransactionSyncService;
import lombok.RequiredArgsConstructor;
import nl.ing.lovebird.clienttokens.ClientUserToken;
import nl.ing.lovebird.extendeddata.common.CurrencyCode;
import nl.ing.lovebird.providerdomain.ProviderTransactionDTO;

import java.util.List;
import java.util.UUID;

@RequiredArgsConstructor
public class DefaultTransactionInsertionStrategy implements TransactionInsertionStrategy {
    private final TransactionSyncService transactionSyncService;

    @Override
    public Instruction determineTransactionPersistenceInstruction(List<ProviderTransactionDTO> upstreamTransactions, ClientUserToken clientUserToken, UUID accountId, String provider, CurrencyCode currencyCode) {
        return transactionSyncService.reconcile(clientUserToken, accountId, upstreamTransactions, provider);
    }

}
