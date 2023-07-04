package com.yolt.accountsandtransactions.accounts;

import com.yolt.accountsandtransactions.ApiTags;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.RequiredArgsConstructor;
import nl.ing.lovebird.clienttokens.ClientUserToken;
import nl.ing.lovebird.clienttokens.annotations.VerifiedClientToken;
import nl.ing.lovebird.springdoc.annotations.ExternalApi;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.UUID;

import static org.springframework.http.MediaType.APPLICATION_JSON_VALUE;

@RequiredArgsConstructor
@RestController
@Tag(name = ApiTags.AIS_TRANSACTION_DETAILS)
public class AccountController {

    private final AccountService accountService;

    @Operation(summary = "Retrieve a list of accounts that belong to a specific User.",
            description = "Retrieve a list of accounts for the user identified by the path parameter userId. " +
                    "The query parameter userSiteId can be used to narrow the results down to a specific UserSite.",
            responses = {
                    @ApiResponse(responseCode = "200", description = "Successfully retrieved a list of accounts belonging to the user."),
                    @ApiResponse(responseCode = "403", description = "The userId does not match with the id of the logged in user.", content = @Content(schema = @Schema))
            }
    )

    @ExternalApi
    @GetMapping(value = "/v1/users/{userId}/accounts", produces = APPLICATION_JSON_VALUE)
    public ResponseEntity<List<AccountDTO>> getAccounts(
            @Parameter(description = "Unique identifier of the User for which to list accounts.", required = true) @PathVariable("userId") UUID userId,
            @Parameter(description = "Optional identifier of the UserSite, if present only accounts linked to the UserSite will be returned.") @RequestParam(value = "userSiteId", required = false) UUID userSiteId,
            @Parameter(hidden = true) @VerifiedClientToken ClientUserToken clientUserToken
    ) {
        if (!clientUserToken.getUserIdClaim().equals(userId)) {
            return ResponseEntity.status(HttpStatus.FORBIDDEN).build();
        }
        List<AccountDTO> accounts = accountService.getAccountsDTOsForUserSite(clientUserToken.getUserIdClaim(), userSiteId);
        return ResponseEntity.ok(accounts);
    }

}
