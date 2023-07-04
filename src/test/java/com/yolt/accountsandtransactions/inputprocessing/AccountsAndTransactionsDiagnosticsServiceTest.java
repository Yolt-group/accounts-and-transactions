package com.yolt.accountsandtransactions.inputprocessing;

import com.yolt.accountsandtransactions.BaseIntegrationTest;
import com.yolt.accountsandtransactions.datascience.DataScienceService;
import com.yolt.accountsandtransactions.metrics.AccountsAndTransactionMetrics;
import nl.ing.lovebird.extendeddata.transaction.ExtendedTransactionDTO;
import nl.ing.lovebird.extendeddata.transaction.TransactionStatus;
import nl.ing.lovebird.providerdomain.ProviderTransactionDTO;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.Clock;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.UUID;

import static java.time.Clock.systemUTC;
import static java.util.UUID.randomUUID;
import static org.mockito.Mockito.*;


@ExtendWith(MockitoExtension.class)
public class AccountsAndTransactionsDiagnosticsServiceTest extends BaseIntegrationTest {

    @Mock
    DataScienceService dataScienceService;
    @Mock
    AccountsAndTransactionMetrics metrics;

    Clock clock = Clock.systemUTC();

    AccountsAndTransactionDiagnosticsService accountsAndTransactionDiagnosticsService;

    @BeforeEach
    public void setup() {
        accountsAndTransactionDiagnosticsService = new AccountsAndTransactionDiagnosticsService(
                dataScienceService,
                metrics,
                clock
        );
    }

    @Test
    public void measureTransactionsInTheFuture() {
        var now = ZonedDateTime.now(systemUTC());
        accountsAndTransactionDiagnosticsService.updateFutureTransactionsStatistics("A_PROVIDER",
                List.of(
                        new ProviderTransactionWithId(ProviderTransactionDTO.builder().dateTime(now.minusHours(2)).status(TransactionStatus.BOOKED).build(), randomUUID().toString()),
                        new ProviderTransactionWithId(ProviderTransactionDTO.builder().dateTime(now.minusHours(1)).status(TransactionStatus.BOOKED).build(), randomUUID().toString()),
                        new ProviderTransactionWithId(ProviderTransactionDTO.builder().dateTime(now.plusHours(1)).status(TransactionStatus.BOOKED).build(), randomUUID().toString()),
                        new ProviderTransactionWithId(ProviderTransactionDTO.builder().dateTime(now.minusHours(8)).status(TransactionStatus.BOOKED).build(), randomUUID().toString()),
                        new ProviderTransactionWithId(ProviderTransactionDTO.builder().dateTime(now.plusHours(2)).status(TransactionStatus.BOOKED).build(), randomUUID().toString())
                ),
                true);

        verify(metrics, times(2)).incrementTransactionWithFutureDate(any(), anyBoolean(), any());
    }

    @Test
    public void validateWithJustStructuredRemittanceInformation() {
        accountsAndTransactionDiagnosticsService.recordInvalidRemittanceInformation(List.of(
                        new ProviderTransactionWithId(
                                ProviderTransactionDTO.builder()
                                        .extendedTransaction(
                                                ExtendedTransactionDTO.builder()
                                                        .remittanceInformationStructured("Structured is there")
                                                        .build())
                                        .build(), UUID.randomUUID().toString())),
                "A_PROVIDER");

        verify(metrics, never()).incrementTransactionWithStructuredAndUnstructuredRemittanceInformation(any());
        verify(metrics, never()).incrementTransactionWithoutStructuredAndUnstructuredRemittanceInformation(any());
    }

    @Test
    public void validateWithBothStructuredAndUnstructuredRemittanceInformation() {
        accountsAndTransactionDiagnosticsService.recordInvalidRemittanceInformation(List.of(
                        new ProviderTransactionWithId(
                                ProviderTransactionDTO.builder()
                                        .extendedTransaction(
                                                ExtendedTransactionDTO.builder()
                                                        .remittanceInformationStructured("Structured is there")
                                                        .remittanceInformationUnstructured("UnStructured is there")
                                                        .build())
                                        .build(), UUID.randomUUID().toString())),
                "A_PROVIDER");

        verify(metrics).incrementTransactionWithStructuredAndUnstructuredRemittanceInformation(any());
        verify(metrics, never()).incrementTransactionWithoutStructuredAndUnstructuredRemittanceInformation(any());
    }

    @Test
    public void validateWithEmptyRemittanceInformation() {
        accountsAndTransactionDiagnosticsService.recordInvalidRemittanceInformation(List.of(
                        new ProviderTransactionWithId(
                                ProviderTransactionDTO.builder()
                                        .extendedTransaction(
                                                ExtendedTransactionDTO.builder()
                                                        .remittanceInformationStructured("")
                                                        .remittanceInformationUnstructured("")
                                                        .build())
                                        .build(), UUID.randomUUID().toString())),
                "A_PROVIDER");

        verify(metrics, never()).incrementTransactionWithStructuredAndUnstructuredRemittanceInformation(any());
        verify(metrics).incrementTransactionWithoutStructuredAndUnstructuredRemittanceInformation(any());
    }

    @Test
    public void validateWithoutRemittanceInformation() {
        accountsAndTransactionDiagnosticsService.recordInvalidRemittanceInformation(List.of(
                        new ProviderTransactionWithId(
                                ProviderTransactionDTO.builder()
                                        .extendedTransaction(ExtendedTransactionDTO.builder().build())
                                        .build(), UUID.randomUUID().toString())),
                "A_PROVIDER");

        verify(metrics, never()).incrementTransactionWithStructuredAndUnstructuredRemittanceInformation(any());
        verify(metrics, never()).incrementTransactionWithoutStructuredAndUnstructuredRemittanceInformation(any());
    }

}
