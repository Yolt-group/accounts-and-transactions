package com.yolt.accountsandtransactions.datascience.categories.dto;

import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;

import java.util.Set;
import java.util.UUID;

/**
 * <pre>
 * {
 *   "userId": "59ae97bd-e914-41c0-9829-2efc1bc9f9e2",
 *   "groupSelectors": [
 *     "string"
 *   ],
 *   "category": "Housing",
 *   "seedTransaction": {
 *     "userId": "59ae97bd-e914-41c0-9829-2efc1bc9f9e2",
 *     "accountId": "55a9c145-9aed-430d-84b1-9e293cc12616",
 *     "transactionId": "transaction111",
 *     "transactionType": "REGULAR",
 *     "date": "1979-12-28"
 *   }
 * }
 * </pre>
 */
@Builder
@Getter
@RequiredArgsConstructor
@Schema
public class DsCategoriesFeedbackGroupsDTO {

    @NonNull
    @Schema(description = "A unique identifier for an user", required = true)
    private final UUID userId;

    @NonNull
    @Schema(description = "The seed transaction", required = true)
    private final DsCategoriesTransactionKeyDTO seedTransaction;

    @NonNull
    @Schema(description = "The groups to apply the feedback to", required = true)
    private final Set<String> groupSelectors;

    @NonNull
    @Schema(description = "A category. Needs to be a valid category", required = true)
    private final String category;
}
