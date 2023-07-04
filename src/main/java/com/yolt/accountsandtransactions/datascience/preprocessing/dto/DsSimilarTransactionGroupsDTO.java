package com.yolt.accountsandtransactions.datascience.preprocessing.dto;

import com.yolt.accountsandtransactions.datascience.DsShortTransactionKeyDTO;
import io.swagger.v3.oas.annotations.media.ArraySchema;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;

import javax.validation.constraints.Size;
import java.util.List;

@Getter
@Builder
@RequiredArgsConstructor
@Schema
public class DsSimilarTransactionGroupsDTO {

    @NonNull
    @Schema(description = "Selector for the group", required = true)
    @Size(min = 1, max = 2048)
    private final String groupSelector;

    @NonNull
    @ArraySchema(arraySchema = @Schema(description = "Transaction keys belonging to this group", required = true))
    private final List<DsShortTransactionKeyDTO> transactions;
}