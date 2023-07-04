package com.yolt.accountsandtransactions.exception;

import nl.ing.lovebird.errorhandling.ErrorInfo;

public enum ErrorConstants implements ErrorInfo {
    NOT_ALLOWED_TO_ACCESS_RISK_INSIGHTS("001", "Client is not configured to access risk insights. Please contact Yolt if you would like to have this functionality.");

    private final String code;
    private final String message;

    ErrorConstants(final String code, final String message) {
        this.code = code;
        this.message = message;
    }

    @Override
    public String getCode() {
        return code;
    }

    @Override
    public String getMessage() {
        return message;
    }
}
