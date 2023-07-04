package com.yolt.accountsandtransactions.transactions.updates;

import com.yolt.accountsandtransactions.ApiTags;
import com.yolt.accountsandtransactions.transactions.updates.api.SimilarTransactionGroupDTO;
import com.yolt.accountsandtransactions.transactions.updates.api.SimilarTransactionsForUpdatesDTO;
import com.yolt.accountsandtransactions.transactions.updates.updatesession.BulkUpdateSession;
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
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.time.LocalDate;
import java.util.Optional;
import java.util.UUID;

import static java.util.stream.Collectors.toList;
import static org.springframework.http.HttpStatus.FORBIDDEN;
import static org.springframework.http.MediaType.APPLICATION_JSON_VALUE;
import static org.springframework.http.ResponseEntity.notFound;
import static org.springframework.http.ResponseEntity.status;

/**
 * Allows a client to fetch transactions to the seed-transaction.
 * <p>
 * The {@link SimilarTransactionsForUpdatesDTO#getGroups()} contains similar transactions that are grouped
 * by the match of the seed transaction. These groups are ordered by datascience on the likelihood of their match.
 * <p>
 * The {@link SimilarTransactionsForUpdatesDTO#getUpdateSessionId()} contains a short-lived session id that should be
 * passed to the {@link TransactionsUpdateController} when performing the actual update.
 */
@RestController
@Slf4j
@RequiredArgsConstructor
@Tag(name = ApiTags.AIS_ENRICHMENTS)
public class SimilarTransactionController {
    private final SimilarTransactionsService similarTransactionsService;

    @ExternalApi
    @Operation(summary = "Get transactions similar to the provided seed transaction.", responses = {
            @ApiResponse(responseCode = "200", description = "Successfully found transactions similar to the seed-transaction."),
            @ApiResponse(responseCode = "404", description = "Unable to find transactions similar to the seed-transaction.", content = @Content(schema = @Schema)),
            @ApiResponse(responseCode = "403", description = "The userId does not match with the id of the logged in user.", content = @Content(schema = @Schema))
    })
    @GetMapping(value = "/v1/users/{userId}/enrichment-tasks/similar-transactions", produces = APPLICATION_JSON_VALUE)
    @AIS
    public ResponseEntity<SimilarTransactionsForUpdatesDTO> getSimilarTransactions(
            @Parameter(description = "userId") @PathVariable("userId") final UUID userId,
            @Parameter(hidden = true) @VerifiedClientToken ClientUserToken clientUserToken,
            @Parameter(description = "ID of the account with the seed transaction.", required = true)
            @RequestParam("accountId") final UUID accountId,
            @Parameter(description = "ID of the seed transaction.", required = true)
            @RequestParam("transactionId") final String transactionId,
            @Parameter(description = "Date of the seed transaction.", required = true)
            @RequestParam("date") @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) final LocalDate date) {
        if (!clientUserToken.getUserIdClaim().equals(userId) && clientHasDataScienceEnabled(clientUserToken)) {
            return status(FORBIDDEN).build();
        }

        log.debug("Retrieving similar transactions for transaction {}/{}", accountId, transactionId); //NOSHERIFF

        return similarTransactionsService.startBulkUpdateSession(userId, accountId, date, transactionId)
                .flatMap(bulkUpdateSession -> getSimilarTransactions(clientUserToken, bulkUpdateSession))
                .map(ResponseEntity::ok)
                .orElseGet(() -> notFound().build());
    }

    private boolean clientHasDataScienceEnabled(ClientToken clientToken) {
        return clientToken.hasDataEnrichmentLabels()
                || clientToken.hasDataEnrichmentCategorization()
                || clientToken.hasDataEnrichmentCycleDetection()
                || clientToken.hasDataEnrichmentMerchantRecognition();
    }

    private Optional<SimilarTransactionsForUpdatesDTO> getSimilarTransactions(ClientUserToken clientUserToken, BulkUpdateSession bulkUpdateSession) {
        return similarTransactionsService.getSimilarTransactions(clientUserToken, bulkUpdateSession)
                .map(similarTxs -> SimilarTransactionsForUpdatesDTO.builder()
                        .updateSessionId(similarTxs.getBulkUpdateSession().getUpdateSessionId())
                        .groups(similarTxs.getGroups().stream()
                                .map(it -> SimilarTransactionGroupDTO.builder()
                                        .count(it.getCount())
                                        .groupDescription(it.getGroupDescription())
                                        .groupSelector(it.getGroupSelector())
                                        .transactions(it.getTransactions())
                                        .build())
                                .collect(toList()))
                        .build());
    }
}
