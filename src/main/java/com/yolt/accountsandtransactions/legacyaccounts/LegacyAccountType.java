package com.yolt.accountsandtransactions.legacyaccounts;

import nl.ing.lovebird.providerdomain.AccountType;

public enum LegacyAccountType {
    CURRENT_ACCOUNT,
    CREDIT_CARD,
    SAVINGS_ACCOUNT,
    PREPAID_ACCOUNT,
    PENSION,
    INVESTMENT,
    FOREIGN_CURRENCY,
    UNKNOWN;

    public static LegacyAccountType from(AccountType accountType) {
        return switch (accountType) {
            case CURRENT_ACCOUNT -> CURRENT_ACCOUNT;
            case CREDIT_CARD -> CREDIT_CARD;
            case SAVINGS_ACCOUNT -> SAVINGS_ACCOUNT;
            case PREPAID_ACCOUNT -> PREPAID_ACCOUNT;
            case PENSION -> PENSION;
            case INVESTMENT -> INVESTMENT;
        };
    }
}