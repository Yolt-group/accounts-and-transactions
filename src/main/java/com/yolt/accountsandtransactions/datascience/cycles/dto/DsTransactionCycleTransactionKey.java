package com.yolt.accountsandtransactions.datascience.cycles.dto;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.ToString;

import java.time.LocalDate;
import java.util.UUID;

@Getter
@ToString
@EqualsAndHashCode
@RequiredArgsConstructor
public class DsTransactionCycleTransactionKey {

    @NonNull
    private final UUID accountId;

    @NonNull
    private final String id;

    @NonNull
    private final String transactionType;

    @NonNull
    private final LocalDate date;
}
