package com.yolt.accountsandtransactions.legacyaccounts;

import lombok.RequiredArgsConstructor;
import nl.ing.lovebird.clienttokens.ClientUserToken;
import nl.ing.lovebird.clienttokens.annotations.VerifiedClientToken;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import javax.validation.Valid;
import java.util.List;

import static org.springframework.http.MediaType.APPLICATION_JSON_VALUE;

@RestController
@RequestMapping(value = "/legacy-accounts/user-accounts/me")
@RequiredArgsConstructor
public class LegacyAccountsController {

    private final LegacyAccountService legacyAccountService;

    @GetMapping(produces = MediaType.APPLICATION_JSON_VALUE)
    @ResponseBody
    public List<LegacyAccountGroupDTO> getUserAccountsForUserNoHiddenStatus(@VerifiedClientToken ClientUserToken clientUserToken) {

        return legacyAccountService.getAccountGroups(clientUserToken);
    }

    @PostMapping(value = "/accounts/hide-unhide", consumes = APPLICATION_JSON_VALUE)
    public ResponseEntity<Void> updateAccountsHiddenStateForUser(@VerifiedClientToken ClientUserToken clientUserToken,
                                                                 @RequestBody @Valid final LegacyAccountsDTO legacyAccountsDTO) {

        legacyAccountService.updateAccountHiddenStatusForUser(clientUserToken.getUserIdClaim(), legacyAccountsDTO.getAccounts());
        return ResponseEntity.ok().build();
    }
}
