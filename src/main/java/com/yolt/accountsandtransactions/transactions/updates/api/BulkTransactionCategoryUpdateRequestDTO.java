package com.yolt.accountsandtransactions.transactions.updates.api;


import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import io.swagger.v3.oas.annotations.media.ArraySchema;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Builder;
import lombok.Data;
import lombok.RequiredArgsConstructor;

import javax.validation.constraints.NotNull;
import javax.validation.constraints.Pattern;
import javax.validation.constraints.Size;
import java.util.Set;
import java.util.UUID;

@Data
@Builder(toBuilder = true)
@RequiredArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
@Schema
public class BulkTransactionCategoryUpdateRequestDTO {
    @NotNull
    @Schema(description = "Session ID that is associated with this transaction update, retrieved when determining related transactions.", required = true)
    private final UUID updateSessionId;

    @NotNull
    @Size(min = 1, max = 50)
    @ArraySchema(arraySchema = @Schema(description = "List of groups of similar transactions to recategorize", required = true))
    private final Set<String> groupSelectors;

    @Schema(description = "New category of the transaction")
    @Pattern(regexp = "^[A-Za-z][A-Za-z ]{0,30}[A-Za-z]$")
    private final String category;
}
