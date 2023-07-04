package com.yolt.accountsandtransactions.datascience.counterparties.client.dto;

import lombok.Builder;
import lombok.RequiredArgsConstructor;
import lombok.Value;

@Value
@Builder
@RequiredArgsConstructor
public class DsCounterpartiesFeedbackDTO {
    DsCounterpartiesTransactionKeyDTO transactionKey;
    String counterpartyName;
}
