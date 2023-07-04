package com.yolt.accountsandtransactions.inputprocessing.dataquality;


import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.Appender;
import com.yolt.accountsandtransactions.inputprocessing.AccountFromProviders;
import com.yolt.accountsandtransactions.inputprocessing.ProviderTransactionWithId;
import com.yolt.accountsandtransactions.metrics.AccountsAndTransactionMetrics;
import nl.ing.lovebird.extendeddata.account.BalanceDTO;
import nl.ing.lovebird.extendeddata.account.ExtendedAccountDTO;
import nl.ing.lovebird.extendeddata.common.BalanceAmountDTO;
import nl.ing.lovebird.extendeddata.transaction.TransactionStatus;
import nl.ing.lovebird.providerdomain.ProviderTransactionDTO;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.time.Clock;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.UUID;

import static ch.qos.logback.classic.Level.DEBUG;
import static ch.qos.logback.classic.Logger.ROOT_LOGGER_NAME;
import static com.yolt.accountsandtransactions.inputprocessing.AccountFromProviders.accountsFromProvidersBuilder;
import static java.math.BigDecimal.*;
import static java.time.Clock.systemUTC;
import static java.util.Collections.emptyList;
import static java.util.stream.Collectors.toList;
import static nl.ing.lovebird.extendeddata.account.BalanceType.AVAILABLE;
import static nl.ing.lovebird.extendeddata.account.BalanceType.EXPECTED;
import static nl.ing.lovebird.extendeddata.transaction.TransactionStatus.BOOKED;
import static nl.ing.lovebird.extendeddata.transaction.TransactionStatus.PENDING;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
public class StartBalanceStrategyAnalyzerTest {
    private StartBalanceStrategyAnalyzer analyzer;

    private final Clock clock = systemUTC();

    @Mock
    private AccountsAndTransactionMetrics metrics;

    @Mock
    protected Appender<ILoggingEvent> logAppender;

    @Captor
    protected ArgumentCaptor<ILoggingEvent> loggingEventArgumentCaptor;

    @BeforeEach
    public void init() {
        analyzer = new StartBalanceStrategyAnalyzer(metrics, clock);

        var root = (Logger) LoggerFactory.getLogger(ROOT_LOGGER_NAME);
        root.setLevel(DEBUG);
        root.addAppender(logAppender);
    }

    @Test
    public void skipExistingAccounts() {
        analyzer.analyze(account("6", ONE, TEN), true, List.of(transaction(10, BOOKED, ONE)), emptyList(), emptyList());

        verify(this.logAppender, atLeastOnce()).doAppend(loggingEventArgumentCaptor.capture());

        assertThat(loggingEventArgumentCaptor.getAllValues().stream()
                .map(ILoggingEvent::getFormattedMessage)
                .collect(toList()))
                .containsAll(List.of(
                        "This account (6) can be skipped for analysis.",
                        "Existing account."));
    }

    @Test
    public void skipAccountWithoutExtendedInformation() {
        analyzer.analyze(accountsFromProvidersBuilder().accountId("6").build(),
                false,
                List.of(
                        transaction(10, BOOKED, ONE),
                        transaction(2 * 365, BOOKED, ONE)),
                emptyList(),
                emptyList());

        verify(this.logAppender, atLeastOnce()).doAppend(loggingEventArgumentCaptor.capture());
        assertThat(loggingEventArgumentCaptor.getAllValues().stream()
                .map(ILoggingEvent::getFormattedMessage)
                .collect(toList()))
                .containsAll(List.of(
                        "This account (6) can be skipped for analysis.",
                        "Account without extended information."));
    }

    @Test
    public void skipForAccountsWithUpdates() {
        analyzer.analyze(account("6", ONE, TEN),
                false,
                List.of(transaction(10, BOOKED, ONE)),
                List.of(transaction(1, BOOKED, TEN)),
                emptyList());

        verify(this.logAppender, atLeastOnce()).doAppend(loggingEventArgumentCaptor.capture());
        assertThat(loggingEventArgumentCaptor.getAllValues().stream()
                .map(ILoggingEvent::getFormattedMessage)
                .collect(toList()))
                .containsAll(List.of(
                        "This account (6) can be skipped for analysis.",
                        "Account with updates/deletions."));
    }

    @Test
    public void skipForVeryOldTransactions() {
        analyzer.analyze(account("6", ONE, TEN),
                false,
                List.of(
                        transaction(10, BOOKED, ONE),
                        transaction(2 * 365, BOOKED, ONE)),
                emptyList(),
                emptyList());

        verify(this.logAppender, atLeastOnce()).doAppend(loggingEventArgumentCaptor.capture());
        assertThat(loggingEventArgumentCaptor.getAllValues().stream()
                .map(ILoggingEvent::getFormattedMessage)
                .collect(toList()))
                .contains("This account (6) can be skipped for analysis.");
    }

    @Test
    public void bookedAndPendingTransactionsMatchAvailableBalance() {
        analyzer.analyze(account("6", valueOf(6L), valueOf(100)),
                false,
                List.of(
                        transaction(1, BOOKED, valueOf(2)),
                        transaction(2, BOOKED, valueOf(-3)),
                        transaction(0, BOOKED, valueOf(4)),
                        transaction(1, PENDING, valueOf(-1)),
                        transaction(2, PENDING, valueOf(6)),
                        transaction(0, PENDING, valueOf(-2))),
                emptyList(),
                emptyList());

        verify(metrics).incrementComputeZeroBalanceResult(eq("SomeProvider"), eq(AVAILABLE), eq(true), eq("booked_and_pending"));
        verify(metrics, times(7)).incrementComputeZeroBalanceResult(eq("SomeProvider"), any(), eq(false), any());
    }

    @Test
    public void bookedAndPendingBeforeTodayTransactionsMatchAvailableBalance() {
        analyzer.analyze(account("6", valueOf(4L), valueOf(100)),
                false,
                List.of(
                        transaction(1, BOOKED, valueOf(2)),
                        transaction(2, BOOKED, valueOf(-3)),
                        transaction(0, BOOKED, valueOf(4)),
                        transaction(1, PENDING, valueOf(-1)),
                        transaction(2, PENDING, valueOf(6)),
                        transaction(0, PENDING, valueOf(-2))),
                emptyList(),
                emptyList());

        verify(metrics).incrementComputeZeroBalanceResult(eq("SomeProvider"), eq(AVAILABLE), eq(true), eq("booked_and_pending_before_today"));
        verify(metrics, times(7)).incrementComputeZeroBalanceResult(eq("SomeProvider"), any(), eq(false), any());
    }

    @Test
    public void bookedTransactionsMatchAvailableBalance() {
        analyzer.analyze(account("6", valueOf(100L), valueOf(3)),
                false,
                List.of(
                        transaction(1, BOOKED, valueOf(2)),
                        transaction(2, BOOKED, valueOf(-3)),
                        transaction(0, BOOKED, valueOf(4)),
                        transaction(1, PENDING, valueOf(-1)),
                        transaction(2, PENDING, valueOf(6)),
                        transaction(0, PENDING, valueOf(-2))),
                emptyList(),
                emptyList());

        verify(metrics).incrementComputeZeroBalanceResult(eq("SomeProvider"), eq(EXPECTED), eq(true), eq("booked"));
        verify(metrics, times(7)).incrementComputeZeroBalanceResult(eq("SomeProvider"), any(), eq(false), any());
    }

    @Test
    public void bookedBeforeTodayTransactionsMatchAvailableBalance() {
        analyzer.analyze(account("6", valueOf(100L), valueOf(-1)),
                false,
                List.of(
                        transaction(1, BOOKED, valueOf(2)),
                        transaction(2, BOOKED, valueOf(-3)),
                        transaction(0, BOOKED, valueOf(4)),
                        transaction(1, PENDING, valueOf(-1)),
                        transaction(2, PENDING, valueOf(6)),
                        transaction(0, PENDING, valueOf(-2))),
                emptyList(),
                emptyList());

        verify(metrics).incrementComputeZeroBalanceResult(eq("SomeProvider"), eq(EXPECTED), eq(true), eq("booked_before_today"));
        verify(metrics, times(7)).incrementComputeZeroBalanceResult(eq("SomeProvider"), any(), eq(false), any());
    }

    @Test
    public void multipleMatchesBalance() {
        analyzer.analyze(account("6", valueOf(6L), valueOf(-1)),
                false,
                List.of(
                        transaction(1, BOOKED, valueOf(2)),
                        transaction(2, BOOKED, valueOf(-3)),
                        transaction(0, BOOKED, valueOf(4)),
                        transaction(1, PENDING, valueOf(-1)),
                        transaction(2, PENDING, valueOf(6)),
                        transaction(0, PENDING, valueOf(-2))),
                emptyList(),
                emptyList());

        verify(metrics).incrementComputeZeroBalanceResult(eq("SomeProvider"), eq(EXPECTED), eq(true), eq("booked_before_today"));
        verify(metrics).incrementComputeZeroBalanceResult(eq("SomeProvider"), eq(AVAILABLE), eq(true), eq("booked_and_pending"));
        verify(metrics, times(6)).incrementComputeZeroBalanceResult(eq("SomeProvider"), any(), eq(false), any());
    }

    private AccountFromProviders account(String accountId, BigDecimal availableBalance, BigDecimal expectedBalance) {
        return accountsFromProvidersBuilder()
                .accountId(accountId)
                .provider("SomeProvider")
                .extendedAccount(ExtendedAccountDTO.builder()
                        .balances(List.of(
                                BalanceDTO.builder().balanceType(AVAILABLE).balanceAmount(BalanceAmountDTO.builder().amount(availableBalance).build()).build(),
                                BalanceDTO.builder().balanceType(EXPECTED).balanceAmount(BalanceAmountDTO.builder().amount(expectedBalance).build()).build()))
                        .build())
                .build();
    }

    private ProviderTransactionWithId transaction(long daysAgo, TransactionStatus status, BigDecimal amount) {
        return new ProviderTransactionWithId(
                ProviderTransactionDTO.builder()
                        .dateTime(ZonedDateTime.now(clock).minusDays(daysAgo))
                        .status(status)
                        .amount(amount)
                        .build(), UUID.randomUUID().toString());
    }
}