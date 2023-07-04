package com.yolt.accountsandtransactions.transactions.updates.api;

import io.swagger.v3.oas.annotations.media.ArraySchema;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Builder;
import lombok.NonNull;
import lombok.Value;

import java.util.List;
import java.util.UUID;

@Value
@Builder
@Schema(name = "SimilarTransactionsForUpdatesDTO")
public class SimilarTransactionsForUpdatesDTO {

    @NonNull
    @Schema(description = "Session ID that is associated with this transaction update (this ID has a time-to-live).", required = true)
    UUID updateSessionId;

    @ArraySchema(arraySchema = @Schema(description = "Groups of similar transactions", required = true))
    List<SimilarTransactionGroupDTO> groups;
}
