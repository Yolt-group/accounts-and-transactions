package com.yolt.accountsandtransactions.accounts;

import com.datastax.driver.mapping.annotations.Field;
import com.datastax.driver.mapping.annotations.UDT;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import nl.ing.lovebird.extendeddata.account.BalanceType;
import nl.ing.lovebird.extendeddata.common.CurrencyCode;

import java.math.BigDecimal;
import java.time.Instant;

@AllArgsConstructor
@NoArgsConstructor
@Data
@Builder
@UDT(name = "balance")
public class Balance {
    @Field(name = "balance_type")
    private BalanceType balanceType;
    @Field(name = "currency")
    private CurrencyCode currency;
    @Field(name = "amount")
    private BigDecimal amount;
    @Field(name = "last_change_date_time")
    private Instant lastChangeDateTime;
}