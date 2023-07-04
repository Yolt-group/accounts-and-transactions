package com.yolt.accountsandtransactions.datascience.cycles.dto;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;

import java.math.BigDecimal;
import java.time.Period;
import java.util.Optional;

@Getter
@Builder
@RequiredArgsConstructor
@JsonInclude(JsonInclude.Include.NON_ABSENT)
public class DsTransactionCycleCreateRequest {

    @NonNull
    private final DsTransactionCycleTransactionKey transactionKey;

    @NonNull
    private final BigDecimal amount;

    @NonNull
    private final Period period;

    @NonNull
    private final Optional<String> label;
}
