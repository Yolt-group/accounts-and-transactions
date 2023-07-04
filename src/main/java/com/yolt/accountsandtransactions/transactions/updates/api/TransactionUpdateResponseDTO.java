package com.yolt.accountsandtransactions.transactions.updates.api;

import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Data;
import lombok.RequiredArgsConstructor;

import java.util.UUID;

@Data
@RequiredArgsConstructor
@Schema(name = "TransactionUpdateResponse")
public class TransactionUpdateResponseDTO {
    @Schema(description = "Activity ID of the activity that is started to perform transaction-update.", required = true)
    private final UUID activityId;
}
