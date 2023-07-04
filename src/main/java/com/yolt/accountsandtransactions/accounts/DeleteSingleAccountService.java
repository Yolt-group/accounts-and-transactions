package com.yolt.accountsandtransactions.accounts;

import com.yolt.accountsandtransactions.datascience.DsAccountsCurrentRepository;
import com.yolt.accountsandtransactions.datascience.DsTransactionsRepository;
import com.yolt.accountsandtransactions.offloading.OffloadService;
import com.yolt.accountsandtransactions.transactions.TransactionService;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.UUID;

@Service
@RequiredArgsConstructor
class DeleteSingleAccountService {
    private final DsTransactionsRepository dsTransactionsRepository;
    private final DsAccountsCurrentRepository dsAccountsCurrentRepository;
    private final OffloadService offloadService;
    private final AccountRepository accountRepository;
    private final TransactionService transactionService;

    /**
     * This method can be used to delete the account and all it's related transaction data from both the
     * accounts-and-transactions keyspace and the DS keyspace.
     * The data is also deleted from the DS keyspace because there is no single source of truth on account and
     * transaction data (yet).
     *
     */
    void deleteSingleAccountData(UUID userId, UUID accountId) {
        accountRepository.getAccounts(userId)
                .stream()
                .filter(account -> accountId.equals(account.getId()))
                .findFirst()
                .ifPresent(account -> {
                    offloadService.offloadDeleteAsync(account);
                    accountRepository.deleteAccounts(account.getUserId(), List.of(account.getId()));
                    transactionService.deleteAllTransactionDataForUserAccounts(account.getUserId(), List.of(account.getId()));
                    deleteDsAccountAndTransactions(account.getUserId(), account.getId());
                });
    }

    private void deleteDsAccountAndTransactions(UUID userId, UUID accountId) {
        dsAccountsCurrentRepository.getAccountsForUser(userId)
                .stream()
                .filter(account -> accountId.equals(account.getAccountId()))
                .findFirst()
                .ifPresent(account -> {
                    dsAccountsCurrentRepository.deleteAccount(account);
                    dsTransactionsRepository.deleteTransactionsForAccount(account.getUserId(), account.getAccountId());
                });
    }
}
