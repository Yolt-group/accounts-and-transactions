package com.yolt.accountsandtransactions.transactions.updates.api;

import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Data;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;

import java.util.UUID;

@Data
@RequiredArgsConstructor
@Schema(name = "TransactionCounterpartyUpdateResponse")
public class TransactionCounterpartyUpdateResponseDTO {

    @NonNull
    @Schema(description = "Activity ID of the activity that is started to perform transaction-update.", required = true)
    private final UUID activityId;

    @NonNull
    @Schema(description = "The counterparty name that will be applied after resolving a potentially more appropriate alias. " +
            "For example: applying the counterparty name 'M&S' will be expanded to 'Marks & Spencer' since this is a known merchant with a more appropriate alias.", required = true)
    private final String counterpartyName;

    @Schema(description = "Indicates if the counterparty name provided is a known merchant.", required = true)
    private final boolean knownMerchant;
}
