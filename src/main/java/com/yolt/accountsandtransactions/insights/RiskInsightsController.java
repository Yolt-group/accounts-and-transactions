package com.yolt.accountsandtransactions.insights;

import com.yolt.accountsandtransactions.ApiTags;
import com.yolt.accountsandtransactions.datetime.exception.RiskInsightsClaimMissingException;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.RequiredArgsConstructor;
import nl.ing.lovebird.clienttokens.ClientUserToken;
import nl.ing.lovebird.clienttokens.annotations.VerifiedClientToken;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

import java.util.UUID;

import static org.springframework.http.HttpStatus.FORBIDDEN;
import static org.springframework.http.MediaType.APPLICATION_JSON_VALUE;
import static org.springframework.http.ResponseEntity.noContent;
import static org.springframework.http.ResponseEntity.status;

@Tag(name = ApiTags.RISK_INSIGHTS)
@RestController
@RequiredArgsConstructor
public class RiskInsightsController {

    private final RiskInsightsService riskInsightsService;

    // @ExternalApi
    @Operation(summary = "Beta: Retrieve the latest risk insights report for a given user.",
            description = "Retrieves the latest risk insights report for a given user. Please note that this endpoint is in beta. As such, backwards breaking changes can happen,",
            responses = {
                    @ApiResponse(responseCode = "200", description = "Successfully retrieved the latest risk insights report for the user."),
                    @ApiResponse(responseCode = "204", description = "There's currently no risk insights report for the given user. Please try again later.")
            })
    @GetMapping(value = "/v1/users/{userId}/risk-insights", produces = APPLICATION_JSON_VALUE)
    public ResponseEntity<RiskInsightsReportDTO> getRiskInsights(
            @Parameter(hidden = true) @VerifiedClientToken final ClientUserToken clientUserToken,
            @PathVariable final UUID userId) {

        if (!clientUserToken.hasRiskInsights()) {
            throw new RiskInsightsClaimMissingException();
        } else if (!clientUserToken.getUserIdClaim().equals(userId)) {
            return status(FORBIDDEN).build();
        }

        return riskInsightsService.getRiskInsightsReport(userId)
                .map(ResponseEntity::ok)
                .orElse(noContent().build());
    }
}
