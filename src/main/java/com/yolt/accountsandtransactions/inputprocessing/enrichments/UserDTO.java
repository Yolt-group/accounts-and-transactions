package com.yolt.accountsandtransactions.inputprocessing.enrichments;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import javax.validation.constraints.NotNull;
import java.util.UUID;

@JsonIgnoreProperties(ignoreUnknown = true)
public record UserDTO(@NotNull UUID clientId, @NotNull UUID userId) {
}
