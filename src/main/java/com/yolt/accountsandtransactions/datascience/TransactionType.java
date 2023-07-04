package com.yolt.accountsandtransactions.datascience;

import lombok.experimental.UtilityClass;
import nl.ing.lovebird.providerdomain.ProviderTransactionType;

@UtilityClass
class TransactionType {
    public static final String CREDIT = "credit";
    public static final String DEBIT = "debit";

    static String of(final ProviderTransactionType transactionType) {
        if (transactionType == ProviderTransactionType.DEBIT) {
            return TransactionType.DEBIT;
        } else if (transactionType == ProviderTransactionType.CREDIT) {
            return TransactionType.CREDIT;
        }
        throw new IllegalArgumentException("Unknown ProviderTransactionType: " + transactionType);
    }

    static ProviderTransactionType fromString(final String s) {
        if (CREDIT.equals(s)) {
            return ProviderTransactionType.CREDIT;
        }
        if (DEBIT.equals(s)) {
            return ProviderTransactionType.DEBIT;
        }
        throw new IllegalArgumentException("Can't convert \"" + s + "\" to ProviderTransactionType.");
    }

}
