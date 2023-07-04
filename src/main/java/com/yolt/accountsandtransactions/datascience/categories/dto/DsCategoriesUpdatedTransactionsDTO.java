package com.yolt.accountsandtransactions.datascience.categories.dto;

import com.yolt.accountsandtransactions.datascience.DsShortTransactionKeyDTO;
import lombok.Builder;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

import java.util.List;

@Getter
@Builder(toBuilder = true)
@RequiredArgsConstructor
public class DsCategoriesUpdatedTransactionsDTO {
    private final List<DsShortTransactionKeyDTO> transactions;
}
