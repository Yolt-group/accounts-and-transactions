package com.yolt.accountsandtransactions.legacytransactions;

import nl.ing.lovebird.extendeddata.transaction.TransactionStatus;

public enum LegacyTransactionType {
    REGULAR(1),
    PENDING(2),
    PREDICTED(3);

    public final int value;

    LegacyTransactionType(int value) {
        this.value = value;
    }

    public static LegacyTransactionType from(TransactionStatus value) {
        return switch (value) {
            case BOOKED -> REGULAR;
            case PENDING, HOLD -> PENDING;
        };
    }
}
