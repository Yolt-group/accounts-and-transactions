package com.yolt.accountsandtransactions.transactions;

import io.swagger.v3.oas.annotations.media.ArraySchema;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.List;

@Data
@AllArgsConstructor
@Schema
public class TransactionsPageDTO {
    @ArraySchema(arraySchema = @Schema(description = "The list of transactions"))
    private List<TransactionDTO> transactions;
    @Schema(description = "The reference to the next page. This value should be provided by making a subsequent API call with this as query parameter next=${next}")
    private String next;
}
