package com.yolt.accountsandtransactions.datascience;

import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;

import javax.validation.constraints.Pattern;
import java.util.UUID;

@Getter
@RequiredArgsConstructor
@Builder(toBuilder = true)
@Schema
public class DsShortTransactionKeyDTO {

    @NonNull
    @Schema(description = " An unique identifier for an account", required = true)
    private final UUID accountId;

    @NonNull
    @Pattern(regexp = "^[\\\\p{Print}]{1,100}$")
    @Schema(description = "An identifier for a transaction", required = true)
    private final String transactionId;
}
