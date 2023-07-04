package com.yolt.accountsandtransactions.datascience;

import lombok.experimental.UtilityClass;
import nl.ing.lovebird.extendeddata.transaction.TransactionStatus;

@UtilityClass
public class PendingType {
    public static final Integer REGULAR = 1;
    public static final Integer PENDING = 2;

    static int of(final TransactionStatus transactionStatus) {
        if (transactionStatus == TransactionStatus.BOOKED) {
            return PendingType.REGULAR;
        } else if (transactionStatus == TransactionStatus.PENDING) {
            return PendingType.PENDING;
        }
        throw new IllegalArgumentException("Unknown ProviderTransactionStatus: " + transactionStatus);
    }
}
