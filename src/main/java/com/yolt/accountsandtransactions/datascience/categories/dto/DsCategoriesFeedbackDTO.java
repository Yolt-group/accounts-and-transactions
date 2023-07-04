package com.yolt.accountsandtransactions.datascience.categories.dto;

import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;

@Builder
@Getter
@RequiredArgsConstructor
@Schema
public class DsCategoriesFeedbackDTO {

    @NonNull
    @Schema(description = "A transaction key of the reference transaction", required = true)
    private final DsCategoriesTransactionKeyDTO transactionKey;

    @NonNull
    @Schema(description = "A category. Needs to be a valid category", required = true)
    private final String category;
}
