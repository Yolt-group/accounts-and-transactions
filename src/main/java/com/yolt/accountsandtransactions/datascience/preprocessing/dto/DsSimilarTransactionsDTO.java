package com.yolt.accountsandtransactions.datascience.preprocessing.dto;

import io.swagger.v3.oas.annotations.media.ArraySchema;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;

import java.util.List;

/**
 * Example:
 *
 * <pre>
 * {
 *   "groups": [
 *     {
 *       "groupSelector": "string",
 *       "transactions": [
 *         {
 *           "accountId": "55a9c145-9aed-430d-84b1-9e293cc12616",
 *           "transactionId": "transaction111"
 *         }
 *       ]
 *     }
 *   ]
 * }
 * </pre>
 */
@Getter
@Builder(toBuilder = true)
@RequiredArgsConstructor
@Schema
public class DsSimilarTransactionsDTO {

    @NonNull
    @ArraySchema(arraySchema = @Schema(description = "Similarity groups for the seed transaction", required = true))
    // Ordering of groups must be preserved
    private final List<DsSimilarTransactionGroupsDTO> groups;
}
