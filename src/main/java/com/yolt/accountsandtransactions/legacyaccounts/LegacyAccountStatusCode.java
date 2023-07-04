package com.yolt.accountsandtransactions.legacyaccounts;

/**
 * Reminder of times when the status of data refresh was tracked per account in accounts pod.
 * See https://git.yolt.io/backend/health/-/blob/master/src/main/java/nl/ing/lovebird/health/controller/dto/AccountHealthDTO.java#L35 fpr more details
 */
public enum LegacyAccountStatusCode {

    DATASCIENCE_FINISHED("Datascience has crunched the numbers. All is up to date."),
    DELETED("This account is closed/deleted.");

    String description;

    LegacyAccountStatusCode(final String description) {
        this.description = description;
    }

    @Override
    public String toString() {
        return String.format("%s : %s", name(), description);
    }
}
