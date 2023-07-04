package com.yolt.accountsandtransactions.transactions;

import com.yolt.accountsandtransactions.ApiTags;
import com.yolt.accountsandtransactions.accounts.AccountService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import nl.ing.lovebird.clienttokens.ClientUserToken;
import nl.ing.lovebird.clienttokens.annotations.VerifiedClientToken;
import com.yolt.accountsandtransactions.datetime.DateInterval;
import nl.ing.lovebird.springdoc.annotations.ExternalApi;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.lang.Nullable;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.UUID;

import static java.util.Comparator.comparing;
import static org.springframework.http.MediaType.APPLICATION_JSON_VALUE;

@Slf4j
@RequiredArgsConstructor
@RestController
@Tag(name = ApiTags.AIS_TRANSACTION_DETAILS)
public class TransactionController {

    /**
     * Number of transactions returned in a single page.
     */
    private static final int pageSize = 100;

    private final AccountService accountService;

    @Operation(summary = "Retrieve a list of at most 100 transactions (per page) that belong to a specific User.", responses = {
            @ApiResponse(responseCode = "200", description = "Successfully retrieved transactions belonging to the user."),
            @ApiResponse(responseCode = "403", description = "The requested userId does not match with the id of the logged in user.", content = @Content(schema = @Schema))
    })
    @ExternalApi
    @GetMapping(value = "/v1/users/{userId}/transactions", produces = APPLICATION_JSON_VALUE)
    public ResponseEntity<TransactionsPageDTO> getTransactions(
            @Parameter(description = "Unique identifier of the User for which to list transactions.", required = true)
            @PathVariable("userId")
                    UUID userId,
            @Parameter(hidden = true)
            @VerifiedClientToken
                    ClientUserToken clientUserToken,
            @Parameter(description = """
                    Date interval with end-date included for which the transactions should be retrieved. Current month if this parameter is not specified. (example: 2016-07-01/2016-07-31) Supported format:
                    * 'start-date'/'end-date'
                    * 'start-date'/'duration'
                    * 'duration'/'end-date'
                                        
                    The duration is in Period format: ISO-8601. The date format is also according to ISO-8601, i.e. the default Local Date (Time) Formatter""", example = "2016-07-01/2016-07-31")
            @Nullable @RequestParam(required = false)
                    DateInterval dateInterval,
            @Parameter(description = "Optional list of accountIds to be used as filter. " +
                    "It can be provided as a comma-separated list.",
                    example = "accountIds=c5c7e3ec-f23b-4304-8799-4a54b0a1f408,62dd6483-d10a-4ba3-b48e-fcb2392b16f7")
            @Nullable @RequestParam(required = false)
                    List<UUID> accountIds,
            @Parameter(description = "Used for pagination. " +
                    "If an initial call to this endpoint returns an object where the field 'next' is not null, then not all transactions were returned and more can be fetched. " +
                    "To fetch more transactions, call this endpoint again and set this query parameter to the value of the field 'next' that was previously returned. " +
                    "Repeat until the field next contains null.")
            @Nullable @RequestParam(required = false)
                    String next
    ) {
        if (!clientUserToken.getUserIdClaim().equals(userId)) {
            return ResponseEntity.status(HttpStatus.FORBIDDEN).build();
        }
        TransactionsPageDTO transactionDTOS = accountService.getTransactionsForAccount(userId, accountIds, dateInterval, next, pageSize);
        var sorted = transactionDTOS.getTransactions().stream()
                .sorted(comparing(TransactionDTO::getDate))
                .toList();
        log.info("GET /v1/users/{}/transactions?dateInterval={}&accountIds={}&next={} -> n={}, lb={}, ub={}, hasNext={}"
                , userId
                , dateInterval
                , accountIds
                , next != null // This is a base64 encoded string, no need to log it in full, just log if it's present/absent
                , transactionDTOS.getTransactions().size() // n = amount of trxs returned
                , !sorted.isEmpty() ? sorted.get(0).getDate() : null // lb_date = lowest date in batch
                , !sorted.isEmpty() ? sorted.get(sorted.size() - 1).getDate() : null // ub_date = highest date in response
                , transactionDTOS.getNext() != null
        ); // NOSHERIFF
        return ResponseEntity.ok(transactionDTOS);
    }

}
