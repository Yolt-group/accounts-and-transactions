package com.yolt.accountsandtransactions.datascience.counterparties.client.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.Builder;
import lombok.RequiredArgsConstructor;
import lombok.Value;

@JsonIgnoreProperties(ignoreUnknown = true)
@Value
@RequiredArgsConstructor
@Builder
public class DsCounterpartiesFeedbackResponseDTO {
    String counterpartyName;
    Boolean knownMerchant;
}
