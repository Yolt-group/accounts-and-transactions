package com.yolt.accountsandtransactions.datascience.counterparties.client.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Value;

import java.util.List;

@Value
@Builder
@JsonIgnoreProperties(ignoreUnknown = true)
@AllArgsConstructor
public class DsCounterpartiesAdjustedDTO {
    List<String> counterpartyNames;
}
