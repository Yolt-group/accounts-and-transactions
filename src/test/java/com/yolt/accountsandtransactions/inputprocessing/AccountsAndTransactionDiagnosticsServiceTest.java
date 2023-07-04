package com.yolt.accountsandtransactions.inputprocessing;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.Appender;
import com.yolt.accountsandtransactions.datascience.DataScienceService;
import com.yolt.accountsandtransactions.metrics.AccountsAndTransactionMetrics;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.util.UUID;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

class AccountsAndTransactionDiagnosticsServiceTest {

    @Test
    void emitReconciliationFailureEvent() {

        var metricsMock = mock(AccountsAndTransactionMetrics.class);
        var sut = new AccountsAndTransactionDiagnosticsService(
                Mockito.mock(DataScienceService.class),
                metricsMock,
                Clock.systemUTC()
        );

        Appender<ILoggingEvent> mockAppender = Mockito.mock(Appender.class);
        var root = (Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
        root.addAppender(mockAppender);

        try {
            sut.logReconciliationFailureEvent(
                    TransactionInsertionStrategy.Mode.ACTIVE,
                    "Test Provider",
                    UUID.randomUUID(),
                    new IllegalStateException("Error")
            );
            verify(mockAppender).doAppend(any());
            verify(metricsMock).incrementReconciliationFailure("Test Provider");
        } finally {
            root.detachAppender(mockAppender);
        }
    }
}