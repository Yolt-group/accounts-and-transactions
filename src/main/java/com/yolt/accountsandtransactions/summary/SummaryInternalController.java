package com.yolt.accountsandtransactions.summary;

import lombok.RequiredArgsConstructor;
import nl.ing.lovebird.clienttokens.ClientUserToken;
import nl.ing.lovebird.clienttokens.annotations.VerifiedClientToken;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.UUID;

import static org.springframework.http.MediaType.APPLICATION_JSON_VALUE;

@RestController
@RequiredArgsConstructor
public class SummaryInternalController {

    private final SummaryService summaryService;

    @GetMapping(value = "/internal/{userId}/user-site-transaction-status-summary", produces = APPLICATION_JSON_VALUE)
    public ResponseEntity<List<UserSiteTransactionStatusSummary>> getAccounts(
            @PathVariable("userId") UUID userId,
            @VerifiedClientToken ClientUserToken clientUserToken
    ) {
        if (!userId.equals(clientUserToken.getUserIdClaim())) {
            return ResponseEntity.status(HttpStatus.FORBIDDEN).build();
        }
        var summary = summaryService.getUserSiteTransactionStatusSummary(clientUserToken.getUserIdClaim());
        return ResponseEntity.ok(summary);
    }

}
