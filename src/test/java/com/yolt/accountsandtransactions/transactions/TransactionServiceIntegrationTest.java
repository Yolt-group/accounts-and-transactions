package com.yolt.accountsandtransactions.transactions;

import com.yolt.accountsandtransactions.BaseIntegrationTest;
import com.yolt.accountsandtransactions.inputprocessing.ProviderTransactionWithId;
import com.yolt.accountsandtransactions.transactions.TransactionService.AccountIdentifiable;
import nl.ing.lovebird.extendeddata.common.CurrencyCode;
import nl.ing.lovebird.extendeddata.transaction.TransactionStatus;
import nl.ing.lovebird.providerdomain.ProviderTransactionDTO;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.math.BigDecimal;
import java.time.Clock;
import java.time.LocalDate;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

public class TransactionServiceIntegrationTest extends BaseIntegrationTest {

    @Autowired
    private TransactionRepository repository;

    @Autowired
    private Clock clock;

    @Test
    void transactionMapperShouldTruncateJavaTimeToMillis() {

        var userId = UUID.randomUUID();
        var accountId = UUID.randomUUID();
        var transactionId = UUID.randomUUID().toString();

        var providerTransaction = ProviderTransactionDTO.builder()
                .dateTime(ZonedDateTime.now(clock))
                .status(TransactionStatus.BOOKED)
                .amount(BigDecimal.TEN)
                .build();

        var transaction = TransactionService.map(
                new ProviderTransactionWithId(providerTransaction, transactionId),
                new AccountIdentifiable(userId, accountId, CurrencyCode.EUR),
                false,
                clock,
                null
        );

        repository.saveBatch(List.of(transaction), 1);

        assertThat(repository.get(userId, accountId, LocalDate.now(clock), transactionId)).contains(transaction);
    }

}
