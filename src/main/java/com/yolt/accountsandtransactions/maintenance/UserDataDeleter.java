package com.yolt.accountsandtransactions.maintenance;

import com.yolt.accountsandtransactions.accounts.AccountService;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import nl.ing.lovebird.clienttokens.ClientUserToken;
import nl.ing.lovebird.rest.deleteuser.DeleteUserController;
import nl.ing.lovebird.rest.deleteuser.UserDeleter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.UUID;

/**
 * Hook into the mechanism for user data deletion.  The maintenance service calls us over http,
 * see the {@link DeleteUserController} for more information.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class UserDataDeleter {

    private final AccountService accountService;

    @Autowired
    void registerUserDeleter(final UserDeleter userDeleter) {
        userDeleter.registerDeleter(this::deleteUserData);
    }

    /**
     * Delete all account and transaction data in all tables, with the following exceptions:
     *
     * <ul>
     *     <li>bulk_update_sessions: omitted because it has a ttl</li>
     *     <li>activity_enrichments_v2: omitted because it has a ttl</li>
     *     <li>batch_sync_progress_state: omitted because it is a temporary technical table and contains no "user data"</li>
     *     <li>activity_enrichments_initiation: omitted because it has a ttl</li>
     * </ul>
     *
     * @param clientUserToken The client user token.
     */
    public void deleteUserData(@NonNull final ClientUserToken clientUserToken) {
        UUID userId = clientUserToken.getUserIdClaim();
        log.info("Deleting all accounts and transactions for user {}", userId);
        accountService.deleteAccountsAndTransactionsForUser(userId);
    }
}
