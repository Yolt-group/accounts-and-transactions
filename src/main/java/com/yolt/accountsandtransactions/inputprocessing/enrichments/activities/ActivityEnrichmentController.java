package com.yolt.accountsandtransactions.inputprocessing.enrichments.activities;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;

import static org.springframework.http.ResponseEntity.ok;

@RequiredArgsConstructor
@RestController
public class ActivityEnrichmentController {
    private final ActivityEnrichmentService activityEnrichmentService;

    @Operation(summary = "Trigger process to clean-up activities with missing enrichments", responses = {
            @ApiResponse(responseCode = "200", description = "Successful")
    })
    @PostMapping(value = "/enrichments/activities/timedout")
    public ResponseEntity<Void> markTimedOutActivities() {
        activityEnrichmentService.markTimedOutActivities();
        return ok().build();
    }
}
