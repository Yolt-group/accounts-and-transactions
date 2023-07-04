package com.yolt.accountsandtransactions.transactions.cycles;

import com.yolt.accountsandtransactions.ApiTags;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.EqualsAndHashCode;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.ToString;
import nl.ing.lovebird.clienttokens.ClientUserToken;
import nl.ing.lovebird.clienttokens.annotations.VerifiedClientToken;
import nl.ing.lovebird.springdoc.annotations.ExternalApi;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.UUID;

import static java.util.stream.Collectors.toList;
import static org.springframework.http.MediaType.APPLICATION_JSON_VALUE;

@RequiredArgsConstructor
@RestController
@Tag(name = ApiTags.AIS_ENRICHMENTS)
public class TransactionCyclesController {

    private final TransactionCyclesService transactionCyclesService;

    @ExternalApi
    @Operation(summary = "Retrieve all the transaction cycles for the given user.", responses = {
            @ApiResponse(responseCode = "200", description = "Successfully retrieved a list of transaction cycles belonging to the user."),
            @ApiResponse(responseCode = "403", description = "The userId does not match with the id of the logged in user.", content = @Content(schema = @Schema))
    })
    @GetMapping(value = "/v1/users/{userId}/transaction-cycles", produces = APPLICATION_JSON_VALUE)
    public ResponseEntity<TransactionCyclesDTO> getTransactionCycles(
            @Parameter(hidden = true) @VerifiedClientToken final ClientUserToken clientUserToken,
            @Parameter(description = "Unique identifier of the User for which to list the transaction cycles.", required = true)
            @PathVariable("userId") UUID userId) {

        List<TransactionCycleDTO> cycles = transactionCyclesService.getAll(clientUserToken.getUserIdClaim()).stream()
                .map(TransactionCycleDTO::fromTransactionCycle)
                .collect(toList());

        return ResponseEntity.ok(new TransactionCyclesDTO(cycles));
    }

    @ToString
    @EqualsAndHashCode
    @RequiredArgsConstructor
    @Schema
    public static class TransactionCyclesDTO {
        @NonNull
        public final List<TransactionCycleDTO> cycles;
    }
}
