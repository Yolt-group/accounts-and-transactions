package com.yolt.accountsandtransactions.accounts;

import com.yolt.accountsandtransactions.datascience.DsAccountsCurrentRepository;
import com.yolt.accountsandtransactions.datascience.DsCreditCardsCurrentRepository;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import nl.ing.lovebird.clienttokens.ClientUserToken;
import nl.ing.lovebird.clienttokens.annotations.VerifiedClientToken;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.reactive.function.client.WebClient;

import java.util.List;
import java.util.UUID;

import static org.springframework.http.MediaType.APPLICATION_JSON_VALUE;

@Slf4j
@RequiredArgsConstructor
@RestController
public class AccountInternalController {

    private final AccountService accountService;
    private final AccountRepository accountRepository;
    private final DsAccountsCurrentRepository dsAccountsCurrentRepository;
    private final DsCreditCardsCurrentRepository dsCreditCardsCurrentRepository;
    private final WebClient.Builder webClientBuilder;
    private final DeleteSingleAccountService accountDeleteService;

    /**
     * Patch an account.
     * <p>
     * Because this service (accounts-and-transactions) is the SoT (source of truth) for account data, it is
     * this services responsibility to propagate the PATCH call to all other relevant places.
     * <p>
     * Note: this procedure currently only supports changing the user_site_id.
     */
    @PatchMapping(value = "/internal/users/{userId}/account/{accountId}", produces = APPLICATION_JSON_VALUE)
    public ResponseEntity<Void> patchAccount(
            @PathVariable("userId") UUID userId,
            @PathVariable("accountId") UUID accountId,
            @RequestBody AccountPatchDTO patch
    ) {
        // Make sure the account we need to change exists.
        List<Account> accounts = accountRepository.getAccounts(userId);
        var account = accounts.stream()
                .filter(a -> a.getId().equals(accountId))
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException(("PATCH /internal/users/" + userId + "/account/" + accountId) + ": cannot find account"));

        // UserSite update.
        // Change the userSiteId, we cannot check that the UserSite exists or is owned by the user that owns the
        // account.  Instead: we trust callers.  This is OK because this endpoint does not permit a caller to do
        // something nefarious.  The userId of the account remains unchanged and it is the userId that is used by
        // the system to retrieve accounts.  Therefore, a potential attacker cannot use this endpoint to change
        // ownership of account data from one user to another.  At worst, the account will be linked to a UserSite
        // that does not actually exist.

        // There are 4 places that we need to update and we cannot do that atomically.  We start with the accounts
        // service because it requires a PATCH call over http.  This has a higher probability of failing compared to
        // the other three locations, we can update those locations directly with a C* query.
        // If the PATCH call succeeds, there is a high probability that the updates to C* will also succeed, and
        // consequently, there is a high probability that the operation will either fail as a whole, or complete
        // as a whole.
        // TL;DR: do the http call first to make the operation ""atomic"" with high probability.

        // - C* table: accounts.account (PATCH http call)
        var resp = webClientBuilder.build().patch()
                .uri("https://accounts/accounts/user-accounts/{userId}/accounts/{accountId}", userId, accountId)
                .bodyValue(InternalUpdateAccountDTO.withUserSiteId(patch.userSiteId))
                .retrieve()
                .toBodilessEntity()
                .block();

        if (resp == null || !resp.getStatusCode().is2xxSuccessful()) {
            log.error("PATCH /internal/users/{}/account/{}: failed, call to accounts service failed with http status {}.",
                    account.getUserId(),
                    account.getId(),
                    resp == null ? null : resp.getStatusCodeValue()
            );
            return ResponseEntity.status(500).build();
        }
        // Update over http succeeded, proceed with the other locations.

        // - C* table: accounts_and_transactions.accounts (we have write access to this keyspace)
        accountRepository.updateUserSiteId(account.getUserId(), account.getId(), patch.userSiteId);
        // - C* table: datascience.account_current (we have write access to this keyspace)
        dsAccountsCurrentRepository.updateUserSiteId(account.getUserId(), account.getId(), patch.userSiteId);
        // - C* table: datascience.creditcards_current (we have write access to this keyspace)
        dsCreditCardsCurrentRepository.updateUserSiteId(account.getUserId(), account.getId(), patch.userSiteId);

        log.info("PATCH /internal/users/{}/account/{}: changed old userSiteId={} to new userSiteId={}.",
                account.getUserId(),
                account.getId(),
                account.getUserSiteId(),
                patch.userSiteId
        );

        return ResponseEntity.ok().build();
    }

    @DeleteMapping(value = "/internal/users/{userId}/account/{accountId}", produces = APPLICATION_JSON_VALUE)
    public ResponseEntity<Void> deleteAccount(
            @PathVariable("userId") UUID userId,
            @PathVariable("accountId") UUID accountId,
            @VerifiedClientToken(restrictedTo = {"assistance-portal-yts" }) ClientUserToken clientUserToken
    ) {
        if (!userId.equals(clientUserToken.getUserIdClaim())) {
            log.error("Ignored a request to delete an account with id {} for user {} because the client user token belongs to user {}.", accountId, userId, clientUserToken.getUserIdClaim());
            return new ResponseEntity<>(HttpStatus.UNAUTHORIZED);
        }

        accountDeleteService.deleteSingleAccountData(userId, accountId);
        log.info("Deleted account with id {} for client {}", accountId, clientUserToken.getClientIdClaim()); // NOSHERIFF
        return new ResponseEntity<>(HttpStatus.OK);
    }

    @Value
    public static class AccountPatchDTO {
        @NonNull UUID userSiteId;
    }

    /**
     * Copied from the accounts service: nl.ing.lovebird.accounts.controller.dto.InternalUpdateAccountDTO
     * <p>
     * Only copied fields that we needed.
     */
    @Value
    static class InternalUpdateAccountDTO {
        public static InternalUpdateAccountDTO withUserSiteId(@NonNull UUID userSiteId) {
            return new InternalUpdateAccountDTO(new InternalUpdateAccountDTO.UserSiteDTO(userSiteId));
        }

        UserSiteDTO userSite;

        @Value
        public static class UserSiteDTO {
            UUID id;
        }
    }


    /**
     * Internal only.
     */
    @DeleteMapping("/v1/users/{userId}/accounts")
    public ResponseEntity<Void> deleteAccountsAndTransactions(@PathVariable("userId") UUID userId,
                                                              @RequestParam(value = "userSiteId", required = false) UUID userSiteId) {
        accountService.deleteAccountsAndTransactionsForUserSite(userId, userSiteId);
        return ResponseEntity.ok().build();
    }

}
