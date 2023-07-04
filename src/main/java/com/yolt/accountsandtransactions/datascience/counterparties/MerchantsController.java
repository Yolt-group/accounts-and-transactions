package com.yolt.accountsandtransactions.datascience.counterparties;

import com.yolt.accountsandtransactions.ApiTags;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.media.ArraySchema;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.Builder;
import lombok.RequiredArgsConstructor;
import lombok.Value;
import nl.ing.lovebird.clienttokens.ClientUserToken;
import nl.ing.lovebird.clienttokens.annotations.AIS;
import nl.ing.lovebird.clienttokens.annotations.VerifiedClientToken;
import nl.ing.lovebird.springdoc.annotations.ExternalApi;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.UUID;

import static org.springframework.http.HttpStatus.FORBIDDEN;
import static org.springframework.http.MediaType.APPLICATION_JSON_VALUE;
import static org.springframework.http.ResponseEntity.status;

@RequiredArgsConstructor
@RestController
@Tag(name = ApiTags.AIS_ENRICHMENTS)
public class MerchantsController {

    private final MerchantsService merchantsService;

    @ExternalApi
    @GetMapping(value = "/v1/users/{userId}/merchants/suggestions", produces = APPLICATION_JSON_VALUE)
    @Operation(summary = "Returns a list of suggested merchants that users can use to set a new merchant for one or more transactions.", responses = {
            @ApiResponse(responseCode = "200", description = "Successfully retrieved a list of merchant suggestions."),
            @ApiResponse(responseCode = "403", description = "The userId does not match with the id of the logged in user.", content = @Content(schema = @Schema))
    })
    @AIS
    public ResponseEntity<MerchantSuggestionsDTO> getSuggestedMerchants(
            @Parameter(description = "userId") @PathVariable("userId") final UUID userId,
            @Parameter(hidden = true) @VerifiedClientToken final ClientUserToken clientUserToken,
            @Parameter(description = "First characters of the merchant name", required = true)
            @RequestParam final String searchText,
            @Parameter(description = "Country for which merchants will be searched. This should be formatted according to the ISO-3166 two character format", example = "NL", required = true)
            @RequestParam final String country) {
        if (!clientUserToken.getUserIdClaim().equals(userId)) {
            return status(FORBIDDEN).build();
        }

        var merchantSuggestions = MerchantSuggestionsDTO.builder()
                .merchantSuggestions(merchantsService.searchMerchants(clientUserToken, searchText, country))
                .build();

        return ResponseEntity.ok(merchantSuggestions);
    }

    @Value
    @Builder
    @Schema(name = "MerchantSuggestions", description = "Contains a list of merchant suggestions.")
    public static class MerchantSuggestionsDTO {
        @ArraySchema(arraySchema = @Schema(description = "List of merchant names, which match the search text", required = true))
        List<String> merchantSuggestions;
    }
}
