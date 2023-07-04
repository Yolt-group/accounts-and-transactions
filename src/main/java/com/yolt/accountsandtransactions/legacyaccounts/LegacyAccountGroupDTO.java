package com.yolt.accountsandtransactions.legacyaccounts;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.Value;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Currency;
import java.util.List;

@Data
@AllArgsConstructor
public class LegacyAccountGroupDTO {

    LegacyAccountType type;
    List<LegacyAccountDTO> accounts;
    List<TotalDTO> totals;

    public LegacyAccountGroupDTO(LegacyAccountType type) {
        this.type = type;
        this.accounts = new ArrayList<>();
        this.totals = null;
    }

    @Value
    public static class TotalDTO {
        Currency currencyCode;
        BigDecimal total;
    }
}
