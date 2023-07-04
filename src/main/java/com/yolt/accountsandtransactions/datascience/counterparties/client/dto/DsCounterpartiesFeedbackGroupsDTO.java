package com.yolt.accountsandtransactions.datascience.counterparties.client.dto;

import lombok.Builder;
import lombok.RequiredArgsConstructor;
import lombok.Value;

import javax.validation.constraints.NotNull;
import java.util.Set;

@Value
@Builder
@RequiredArgsConstructor
public class DsCounterpartiesFeedbackGroupsDTO {
    @NotNull
    Set<String> groupSelectors;
    @NotNull
    String counterpartyName;
}
