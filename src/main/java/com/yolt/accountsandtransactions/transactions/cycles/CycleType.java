package com.yolt.accountsandtransactions.transactions.cycles;

import com.fasterxml.jackson.annotation.JsonCreator;

public enum CycleType {
    CREDITS,
    DEBITS;

    @JsonCreator
    public static CycleType fromValue(String value) {
        return CycleType.valueOf(value.toUpperCase());
    }
}
