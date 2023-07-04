package com.yolt.accountsandtransactions.transactions.updates;

import com.yolt.accountsandtransactions.ApiTags;
import com.yolt.accountsandtransactions.transactions.cycles.CycleType;
import com.yolt.accountsandtransactions.transactions.cycles.TransactionCycle;
import com.yolt.accountsandtransactions.transactions.cycles.TransactionCycleDTO;
import com.yolt.accountsandtransactions.transactions.updates.TransactionCyclesFeedbackService.ReferenceTransactionNotBooked;
import com.yolt.accountsandtransactions.transactions.updates.TransactionCyclesFeedbackService.ReferenceTransactionNotFound;
import com.yolt.accountsandtransactions.transactions.updates.TransactionCyclesFeedbackService.TransactionCycleNotFound;
import com.yolt.accountsandtransactions.transactions.updates.api.TransactionCyclesCreateRequest;
import com.yolt.accountsandtransactions.transactions.updates.api.TransactionCyclesUpdateRequest;
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
import lombok.extern.slf4j.Slf4j;
import nl.ing.lovebird.clienttokens.ClientUserToken;
import nl.ing.lovebird.clienttokens.annotations.VerifiedClientToken;
import nl.ing.lovebird.springdoc.annotations.ExternalApi;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Mono;

import javax.validation.Valid;
import java.math.BigDecimal;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

import static com.yolt.accountsandtransactions.transactions.updates.TransactionCyclesFeedbackController.TransactionCyclesFeedbackResponse.CreatedOrUpdatedTransactionCycleDTO.fromTransactionCycle;
import static java.util.Optional.empty;
import static org.springframework.http.MediaType.APPLICATION_JSON_VALUE;
import static org.springframework.http.ResponseEntity.accepted;

@Slf4j
@RestController
@RequiredArgsConstructor
@Tag(name = ApiTags.AIS_ENRICHMENTS)
public class TransactionCyclesFeedbackController {

    private final TransactionCyclesFeedbackService transactionCyclesFeedbackService;

    @ExternalApi
    @Operation(summary = "Create a transaction-cycle for a given user.", responses = {
            @ApiResponse(responseCode = "202", description = "Successfully created the transaction-cycle.", content = @Content(schema = @Schema(implementation = TransactionCyclesFeedbackResponse.class))),
            @ApiResponse(responseCode = "400", description = "The transaction-cycle reference transaction does not exists.", content = @Content(schema = @Schema)),
            @ApiResponse(responseCode = "403", description = "The userId does not match with the id of the logged in user.", content = @Content(schema = @Schema))
    })
    @PostMapping(value = "/v1/users/{userId}/enrichment-tasks/transaction-cycles", produces = APPLICATION_JSON_VALUE)
    public Mono<ResponseEntity<TransactionCyclesFeedbackResponse>> createTransactionCycle(
            @Parameter(hidden = true) @VerifiedClientToken final ClientUserToken clientUserToken,
            @Parameter(description = "Unique identifier of the user for to delete the specified transaction-cycle", required = true)
            @PathVariable("userId") UUID userId,
            @io.swagger.v3.oas.annotations.parameters.RequestBody(description = "The transaction-cycle properties.", required = true)
            @RequestBody @Valid final TransactionCyclesCreateRequest createRequest) {

        if (!clientIsAllowedToMakeCycleFeedbackCall(clientUserToken, userId)) {
            return Mono.just(ResponseEntity.status(HttpStatus.FORBIDDEN).build());
        }

        return transactionCyclesFeedbackService.createTransactionCycleFeedback(clientUserToken, createRequest)
                .map(activityAndCycle -> new TransactionCyclesFeedbackResponse(activityAndCycle.getT1(), Optional.of(fromTransactionCycle(activityAndCycle.getT2()))))
                .transform(TransactionCyclesFeedbackController::toErrorHandledResponseEntity);
    }

    @ExternalApi
    @Operation(summary = "Update a specific transaction-cycle.", responses = {
            @ApiResponse(responseCode = "202", description = "Successfully updated the transaction-cycle.", content = @Content(schema = @Schema(implementation = TransactionCyclesFeedbackResponse.class))),
            @ApiResponse(responseCode = "403", description = "The userId does not match with the id of the logged in user.", content = @Content(schema = @Schema)),
            @ApiResponse(responseCode = "404", description = "The transaction-cycle does not exists or has been marked as deleted.", content = @Content(schema = @Schema))
    })
    @PutMapping(value = "/v1/users/{userId}/enrichment-tasks/transaction-cycles/{cycleId}", produces = APPLICATION_JSON_VALUE)
    public Mono<ResponseEntity<TransactionCyclesFeedbackResponse>> updateTransactionCycle(
            @Parameter(hidden = true) @VerifiedClientToken final ClientUserToken clientUserToken,
            @Parameter(description = "Unique identifier of the user for to delete the specified transaction-cycle", required = true)
            @PathVariable("userId") UUID userId,
            @Parameter(description = "Unique identifier of the transaction-cycle to update.", required = true)
            @PathVariable("cycleId") UUID cycleId,
            @io.swagger.v3.oas.annotations.parameters.RequestBody(description = "The transaction-cycle properties.", required = true)
            @RequestBody @Valid final TransactionCyclesUpdateRequest updateRequest) {

        if (!clientIsAllowedToMakeCycleFeedbackCall(clientUserToken, userId)) {
            return Mono.just(ResponseEntity.status(HttpStatus.FORBIDDEN).build());
        }

        return transactionCyclesFeedbackService.updateTransactionCycleFeedback(clientUserToken, cycleId, updateRequest)
                .map(activityAndCycle -> new TransactionCyclesFeedbackResponse(activityAndCycle.getT1(), Optional.of(fromTransactionCycle(activityAndCycle.getT2()))))
                .transform(TransactionCyclesFeedbackController::toErrorHandledResponseEntity);
    }

    @ExternalApi
    @Operation(summary = "Mark a transaction-cycle belonging to a given user as expired.", responses = {
            @ApiResponse(responseCode = "202", description = "Successfully deleted the transaction-cycle belonging to the user.", content = @Content(schema = @Schema(implementation = TransactionCyclesFeedbackResponse.class))),
            @ApiResponse(responseCode = "403", description = "The userId does not match with the id of the logged in user.", content = @Content(schema = @Schema)),
            @ApiResponse(responseCode = "404", description = "The transaction-cycle does not exist or has already been marked as deleted.", content = @Content(schema = @Schema))
    })
    @DeleteMapping(value = "/v1/users/{userId}/enrichment-tasks/transaction-cycles/{cycleId}", produces = APPLICATION_JSON_VALUE)
    public Mono<ResponseEntity<TransactionCyclesFeedbackResponse>> expireTransactionCycle(
            @Parameter(hidden = true) @VerifiedClientToken final ClientUserToken clientUserToken,
            @Parameter(description = "Unique identifier of the user for to delete the specified transaction-cycle", required = true)
            @PathVariable("userId") UUID userId,
            @Parameter(description = "Unique identifier of the transaction-cycle to delete.", required = true)
            @PathVariable("cycleId") UUID cycleId) {

        if (!clientIsAllowedToMakeCycleFeedbackCall(clientUserToken, userId)) {
            return Mono.just(ResponseEntity.status(HttpStatus.FORBIDDEN).build());
        }

        return transactionCyclesFeedbackService.expireTransactionCycleFeedback(clientUserToken, cycleId)
                .map(activityAndCycle -> new TransactionCyclesFeedbackResponse(activityAndCycle.getT1(), empty()))
                .transform(TransactionCyclesFeedbackController::toErrorHandledResponseEntity);
    }

    private boolean clientIsAllowedToMakeCycleFeedbackCall(ClientUserToken clientUserToken, UUID userId) {
        return userId.equals(clientUserToken.getUserIdClaim())
                && clientUserToken.getClientIdClaim().equals(clientUserToken.getClientIdClaim())
                && clientUserToken.hasDataEnrichmentCycleDetection();
    }

    private static <T> Mono<ResponseEntity<T>> toErrorHandledResponseEntity(final Mono<T> mono) {
        return mono
                .map(response -> accepted().body(response))
                .switchIfEmpty(Mono.error(new RuntimeException("The Mono<T> chain completed with an empty mono. Resolve all occurrences in the chain where an empty mono is produced.")))
                .doOnError(throwable -> log.error("Exception thrown while processing transaction-cycles feedback.", throwable))
                .onErrorReturn(TransactionCycleNotFound.class, ResponseEntity.badRequest().build())
                .onErrorReturn(ReferenceTransactionNotFound.class, ResponseEntity.badRequest().build())
                .onErrorReturn(ReferenceTransactionNotBooked.class, ResponseEntity.badRequest().build())
                .onErrorReturn(ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build());
    }

    @ToString
    @EqualsAndHashCode
    @RequiredArgsConstructor
    @Schema(name = "TransactionCyclesFeedbackResponse")
    public static class TransactionCyclesFeedbackResponse {

        @NonNull
        @Schema(description = "Activity ID of the activity that is started to perform transaction-cycles feedback", required = true)
        public final UUID activityId;

        @NonNull
        @Schema(required = true)
        public final Optional<CreatedOrUpdatedTransactionCycleDTO> cycle;

        @Schema(description = "The created or updated transaction-cycle information.", allOf = TransactionCycleDTO.class)
        public static class CreatedOrUpdatedTransactionCycleDTO extends TransactionCycleDTO {
            public CreatedOrUpdatedTransactionCycleDTO(@NonNull UUID cycleId, @NonNull CycleType cycleType, @NonNull BigDecimal amount,
                                                       @NonNull String currency, @NonNull String period, @NonNull Optional<ModelParameters> detected,
                                                       @NonNull Set<String> predictedOccurrences, @NonNull Optional<String> label, boolean subscription,
                                                       @NonNull String counterparty, boolean expired) {
                super(cycleId, cycleType, amount, currency, period, detected, predictedOccurrences, label, subscription, counterparty, expired);
            }

            public static CreatedOrUpdatedTransactionCycleDTO fromTransactionCycle(TransactionCycle transactionCycle) {
                var tc = TransactionCycleDTO.fromTransactionCycle(transactionCycle);

                return new CreatedOrUpdatedTransactionCycleDTO(tc.getCycleId(), tc.getCycleType(), tc.getAmount(),
                        tc.getCurrency(), tc.getPeriod(), tc.getDetected(), tc.getPredictedOccurrences(), tc.getLabel(),
                        tc.isSubscription(), tc.getCounterparty(), tc.isExpired());
            }
        }
    }
}
