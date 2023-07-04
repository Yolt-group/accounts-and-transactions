package com.yolt.accountsandtransactions.legacyaccounts;

import lombok.AllArgsConstructor;
import lombok.Value;

import javax.validation.Valid;
import java.util.List;

@Value
@AllArgsConstructor
public class LegacyAccountsDTO {

    @Valid
    List<LegacyAccountDTO> accounts;
}
