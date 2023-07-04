package com.yolt.accountsandtransactions.datascience.counterparties.client.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.yolt.accountsandtransactions.datascience.DsShortTransactionKeyDTO;
import lombok.Builder;
import lombok.RequiredArgsConstructor;
import lombok.Value;

import javax.validation.constraints.NotNull;
import java.util.List;

@Value
@Builder
@RequiredArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class DsCounterpartiesFeedbackGroupsResponseDTO {
    @NotNull
    String counterpartyName;
    boolean knownMerchant;
    @NotNull
    List<DsShortTransactionKeyDTO> transactions;
}
