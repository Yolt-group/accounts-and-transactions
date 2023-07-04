package com.yolt.accountsandtransactions.datascience.counterparties.client.dto;

import lombok.Builder;
import lombok.RequiredArgsConstructor;
import lombok.Value;

import java.time.LocalDate;
import java.util.UUID;

@Value
@Builder
@RequiredArgsConstructor
public class DsCounterpartiesTransactionKeyDTO {
    UUID accountId;
    String transactionId;
    String transactionType;
    LocalDate date;
}
