package com.yolt.accountsandtransactions.legacyaccounts;

import com.yolt.accountsandtransactions.ValidationException;
import com.yolt.accountsandtransactions.accounts.Account;
import com.yolt.accountsandtransactions.accounts.AccountRepository;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import nl.ing.lovebird.clienttokens.ClientUserToken;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

import static java.util.function.Function.identity;

@RequiredArgsConstructor
@Service
@Slf4j
public class LegacyAccountService {
    private final AccountRepository accountRepository;
    private final LegacyAccountMapper legacyAccountMapper;
    private final LegacyAccountGroupsService legacyAccountGroupsService;

    List<LegacyAccountGroupDTO> getAccountGroups(final ClientUserToken clientUserToken) {
        final List<LegacyAccountDTO> accounts = accountRepository.getAccounts(clientUserToken.getUserIdClaim()).stream()
                .map(legacyAccountMapper::fromAccount)
                .collect(Collectors.toList());
        return legacyAccountGroupsService.buildAccountGroups(accounts);
    }

    public void updateAccountHiddenStatusForUser(final @NonNull UUID userId,
                                                 final @NonNull List<LegacyAccountDTO> accountsToUpdate) {
        final Map<UUID, Account> dbAccountMap = accountRepository.getAccounts(userId).stream()
                .collect(Collectors.toMap(Account::getId, identity()));

        // Validate the data from the request
        accountsToUpdate.forEach(accountToUpdate -> {
            if (accountToUpdate.getId() == null) {
                throw new ValidationException("Account ID missing");
            }
            if (!dbAccountMap.containsKey(accountToUpdate.getId())) {
                throw new ValidationException("Invalid account ID " + accountToUpdate.getId());
            }
        });

        // Update hidden flag
        accountsToUpdate.forEach(accountToUpdate -> {
            final var dbAccount = dbAccountMap.get(accountToUpdate.getId());
            dbAccount.setHidden(accountToUpdate.isHidden());
            accountRepository.saveBatch(List.of(dbAccount), 1);
        });
    }
}
