package com.yolt.accountsandtransactions.exception;

import com.yolt.accountsandtransactions.datetime.exception.RiskInsightsClaimMissingException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import nl.ing.lovebird.errorhandling.ErrorDTO;
import nl.ing.lovebird.errorhandling.ExceptionHandlingService;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.ResponseStatus;

import static com.yolt.accountsandtransactions.exception.ErrorConstants.NOT_ALLOWED_TO_ACCESS_RISK_INSIGHTS;

/**
 * Contains handlers for predefined exception.
 */
@ControllerAdvice
@ResponseBody
@RequiredArgsConstructor
@Slf4j
@Order(Ordered.HIGHEST_PRECEDENCE)
public class ExceptionHandlers {

    private final ExceptionHandlingService exceptionHandlingService;

    @ExceptionHandler(RiskInsightsClaimMissingException.class)
    @ResponseStatus(HttpStatus.FORBIDDEN)
    public ErrorDTO handle(final RiskInsightsClaimMissingException e) {
        return exceptionHandlingService.logAndConstruct(NOT_ALLOWED_TO_ACCESS_RISK_INSIGHTS, e);
    }

}
