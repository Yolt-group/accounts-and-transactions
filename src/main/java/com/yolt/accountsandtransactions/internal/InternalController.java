package com.yolt.accountsandtransactions.internal;

import com.yolt.accountsandtransactions.inputprocessing.AccountsAndTransactionDiagnosticsService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.UUID;

import static com.yolt.accountsandtransactions.inputprocessing.TransactionInsertionStrategy.Mode.TEST;
import static org.springframework.http.MediaType.APPLICATION_JSON_VALUE;

@RestController
@RequiredArgsConstructor
public class InternalController {
    private final AccountsAndTransactionDiagnosticsService diagnosticsService;

    @PostMapping(value = "/internal/trigger-reconciliation-failure-metric", produces = APPLICATION_JSON_VALUE)
    public ResponseEntity<Void> triggerReconciliationFailure() {
        diagnosticsService.logReconciliationFailureEvent(TEST, "Dummy", new UUID(0, 0), new IllegalArgumentException("Test Exception"));
        return ResponseEntity.accepted().build();
    }
}
