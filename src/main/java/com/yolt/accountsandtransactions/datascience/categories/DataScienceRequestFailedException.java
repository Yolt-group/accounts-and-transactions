package com.yolt.accountsandtransactions.datascience.categories;

import lombok.Getter;

import java.util.UUID;

import static java.lang.String.format;

/**
 * A DataScience specific {@link RuntimeException} for {@see org.springframework.web.reactive.function.client.WebClient} failures.
 */
public class DataScienceRequestFailedException extends RuntimeException {

    @Getter
    private final int status;

    @Getter
    private final transient UUID accountId;

    @Getter
    private final transient String transactionId;

    public DataScienceRequestFailedException(final String message, final int status, final UUID accountId, final String transactionId) {
        super(message);
        this.status = status;
        this.accountId = accountId;
        this.transactionId = transactionId;
    }

    @Override
    public String getMessage() {
        if (accountId != null && transactionId != null) {
            return format("Request failed with status %d and message '%s' for transaction %s (account: %s)", status, super.getMessage(), transactionId, accountId);
        }
        return format("Request failed with status %d and message '%s'", status, super.getMessage());
    }
}