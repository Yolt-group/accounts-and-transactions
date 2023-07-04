package com.yolt.accountsandtransactions.transactions.updates;

import com.yolt.accountsandtransactions.ApiTags;
import com.yolt.accountsandtransactions.transactions.updates.api.*;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import nl.ing.lovebird.clienttokens.ClientToken;
import nl.ing.lovebird.clienttokens.ClientUserToken;
import nl.ing.lovebird.clienttokens.annotations.AIS;
import nl.ing.lovebird.clienttokens.annotations.VerifiedClientToken;
import nl.ing.lovebird.springdoc.annotations.ExternalApi;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import javax.validation.Valid;
import java.time.LocalDate;
import java.util.UUID;

import static org.springframework.http.MediaType.APPLICATION_JSON_VALUE;
import static org.springframework.http.ResponseEntity.accepted;
import static org.springframework.http.ResponseEntity.badRequest;

/**
 * Endpoints for updating the category or the counterparty of a single transaction or for a group (bulk) of transactions.
 * <p>
 * Bulk updating transactions is done on a group of transactions that are managed by datascience. Because clients don't know
 * the groups that are present at datascience, we have a fixed order for the process of bulk updates.
 * <p>
 * 1. A client first fetches all transactions that are similar to the one they want to update
 * (see: {@link SimilarTransactionController#getSimilarTransactions(UUID, ClientToken, UUID, String, LocalDate)}).
 * The response for fetching similar transactions contains references to the datascience groups that are related to the transaction and a session-id which needs to be
 * provided in the second step..
 * 2. A client will then be able to issue a request to update the name for a set of datascience groups for a specific user (the session-id maintained in the previous step
 * must be provided here as well)..
 */
@Slf4j
@RestController
@RequiredArgsConstructor
@Tag(name = ApiTags.AIS_ENRICHMENTS)
public class TransactionsUpdateController {
    private final CounterpartyAdjustmentService counterpartyAdjustmentService;
    private final RecategorizationService recategorizationService;

    @ExternalApi
    @Operation(summary = "Update the counterparty on a single transaction.", responses = {
            @ApiResponse(responseCode = "202", description = "Accepted the request to update the counterparty for a single transaction."),
            @ApiResponse(responseCode = "400", description = "Failed accept the request to update the counterparty for a single transaction.", content = @Content(schema = @Schema)),
            @ApiResponse(responseCode = "403", description = "The userId does not match with the id of the logged in user.", content = @Content(schema = @Schema))
    })
    @PatchMapping(value = "/v1/users/{userId}/enrichment-tasks/update-counterparty", produces = APPLICATION_JSON_VALUE)
    @AIS
    public ResponseEntity<TransactionCounterpartyUpdateResponseDTO> updateCounterpartyOfSingleTransaction(
            @PathVariable("userId") final UUID userId,
            @Parameter(hidden = true) @VerifiedClientToken final ClientUserToken clientUserToken,
            @RequestBody @Valid final TransactionCounterpartyUpdateRequestDTO transactionUpdateRequest) {

        log.debug("Updating counterparty {} on transaction {} for account {} to {}", transactionUpdateRequest.getCounterpartyName(), transactionUpdateRequest.getId(), transactionUpdateRequest.getAccountId(), transactionUpdateRequest.getCounterpartyName());

        return counterpartyAdjustmentService.updateCounterpartyOnTransaction(clientUserToken, transactionUpdateRequest)
                .map(result -> accepted().body(new TransactionCounterpartyUpdateResponseDTO(result.activityId, result.counterPartyName, result.knownMerchant)))
                .orElseGet(() -> badRequest().build());
    }

    @ExternalApi
    @Operation(summary = "Update similar transactions based on counterparty name.", responses = {
            @ApiResponse(responseCode = "202", description = "Accepted the request to update the counterparty of a group of transactions."),
            @ApiResponse(responseCode = "400", description = "Failed to accept the request to update the counterparty of a group of transactions.", content = @Content(schema = @Schema)),
            @ApiResponse(responseCode = "403", description = "The userId does not match with the id of the logged in user.", content = @Content(schema = @Schema))
    })
    @PostMapping(value = "/v1/users/{userId}/enrichment-tasks/bulk-update-counterparty", produces = APPLICATION_JSON_VALUE)
    @AIS
    public ResponseEntity<TransactionCounterpartyUpdateResponseDTO> updateCounterpartyForSimilarTransactions(
            @PathVariable("userId") final UUID userId,
            @Parameter(hidden = true) @VerifiedClientToken final ClientUserToken clientUserToken,
            @RequestBody @Valid final BulkTransactionCounterpartyUpdateRequestDTO updateTransactionCounterpartyDTO) {

        log.debug("Updating similar transactions (counterparties to {}) for update-session {}.", updateTransactionCounterpartyDTO.getCounterpartyName(), updateTransactionCounterpartyDTO.getUpdateSessionId());

        return counterpartyAdjustmentService.getUpdateSession(clientUserToken.getUserIdClaim(), updateTransactionCounterpartyDTO)
                .flatMap(bulkUpdateSession -> counterpartyAdjustmentService.updateSimilarTransactions(clientUserToken, bulkUpdateSession, updateTransactionCounterpartyDTO))
                .map(result -> accepted().body(new TransactionCounterpartyUpdateResponseDTO(result.activityId, result.counterPartyName, result.knownMerchant)))
                .orElseGet(() -> badRequest().build());
    }

    @ExternalApi
    @Operation(summary = "Update the category on a single transaction.", responses = {
            @ApiResponse(responseCode = "202", description = "Accepted the request to update the category for a single transaction."),
            @ApiResponse(responseCode = "400", description = "Failed to accept the request to update the category for a single transaction.", content = @Content(schema = @Schema)),
            @ApiResponse(responseCode = "403", description = "The userId does not match with the id of the logged in user.", content = @Content(schema = @Schema))
    })
    @PatchMapping(value = "/v1/users/{userId}/enrichment-tasks/update-category", produces = APPLICATION_JSON_VALUE)
    @AIS
    public ResponseEntity<TransactionUpdateResponseDTO> updateCategoryOfSingleTransaction(
            @PathVariable("userId") final UUID userId,
            @Parameter(hidden = true) @VerifiedClientToken final ClientUserToken clientUserToken,
            @RequestBody @Valid final TransactionCategoryUpdateRequestDTO transactionUpdateRequest) {

        log.debug("Re-categorizing transaction {} for account {} to {}", transactionUpdateRequest.getId(), transactionUpdateRequest.getAccountId(), transactionUpdateRequest.getCategory()); //NOSHERIFF

        return recategorizationService.applyFeedback(clientUserToken, new SeedTransactionKey(transactionUpdateRequest.getAccountId(), transactionUpdateRequest.getId(), transactionUpdateRequest.getDate()), transactionUpdateRequest.getCategory())
                .map(activityId -> accepted().body(new TransactionUpdateResponseDTO(activityId)))
                .orElseGet(() -> badRequest().build());
    }

    @ExternalApi
    @Operation(summary = "Recategorize similar transactions.", responses = {
            @ApiResponse(responseCode = "202", description = "Accepted the request to update the category of a group of transactions."),
            @ApiResponse(responseCode = "400", description = "Failed to accept the request to update the category of a group of transactions.", content = @Content(schema = @Schema)),
            @ApiResponse(responseCode = "403", description = "The requested userId does not match with the id of the logged in user.", content = @Content(schema = @Schema))
    })
    @PostMapping(value = "/v1/users/{userId}/enrichment-tasks/bulk-update-category", produces = APPLICATION_JSON_VALUE)
    @AIS
    public ResponseEntity<TransactionUpdateResponseDTO> recategorizeSimilarTransactions(
            @PathVariable("userId") final UUID userId,
            @Parameter(hidden = true) @VerifiedClientToken final ClientUserToken clientUserToken,
            @RequestBody @Valid final BulkTransactionCategoryUpdateRequestDTO bulkTransactionCategoryUpdateRequestDTO) {

        log.debug("Updating similar transactions (categories to {}) for update-session {}.", bulkTransactionCategoryUpdateRequestDTO.getCategory(), bulkTransactionCategoryUpdateRequestDTO.getUpdateSessionId());

        return recategorizationService.getUpdateSession(userId, bulkTransactionCategoryUpdateRequestDTO)
                .flatMap(bulkUpdateSession -> recategorizationService.applyFeedbackGroups(clientUserToken, bulkUpdateSession, bulkTransactionCategoryUpdateRequestDTO))
                .map(activityId -> accepted().body(new TransactionUpdateResponseDTO(activityId)))
                .orElseGet(() -> badRequest().build());
    }
}
