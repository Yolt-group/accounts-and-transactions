package com.yolt.accountsandtransactions.transactions.updates.api;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import io.swagger.v3.oas.annotations.media.ArraySchema;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Builder;
import lombok.Data;
import lombok.RequiredArgsConstructor;

import javax.validation.constraints.NotNull;
import javax.validation.constraints.Size;
import java.util.Set;
import java.util.UUID;

@Data
@Builder(toBuilder = true)
@RequiredArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
@Schema(name = "BulkCounterpartyAdjustmentRequest")
public class BulkTransactionCounterpartyUpdateRequestDTO {
    @NotNull
    @Schema(description = "Session ID that is associated with this transaction update, retrieved when determining related transactions.", required = true)
    private final UUID updateSessionId;

    @NotNull
    @Size(min = 1, max = 50)
    @ArraySchema(arraySchema = @Schema(description = "The list of transaction groups that should have their counterparty name updated", required = true))
    private final Set<String> groupSelectors;

    @NotNull
    @Size(max = 200)
    @Schema(description = "The new name of the counterparty for the transactions in the transaction groups", required = true)
    private final String counterpartyName;
}
