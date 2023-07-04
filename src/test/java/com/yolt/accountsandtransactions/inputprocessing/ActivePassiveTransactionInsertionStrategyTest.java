package com.yolt.accountsandtransactions.inputprocessing;

import nl.ing.lovebird.clienttokens.ClientUserToken;
import nl.ing.lovebird.clienttokens.test.TestJwtClaims;
import nl.ing.lovebird.extendeddata.common.CurrencyCode;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

class ActivePassiveTransactionInsertionStrategyTest {

    @Test
    void determineTransactionPersistenceInstruction() {

        var activeStubInstruction = new TransactionInsertionStrategy.Instruction(List.of(), Optional.empty());

        var active = Mockito.mock(DefaultTransactionInsertionStrategy.class);
        when(active.determineTransactionPersistenceInstruction(any(), any(), any(), any(), any()))
                .thenReturn(activeStubInstruction);
        when(active.getMode()).thenReturn(TransactionInsertionStrategy.Mode.ACTIVE);

        var passive = Mockito.mock(AttributeInsertionStrategy.class);
        when(passive.getMode()).thenReturn(TransactionInsertionStrategy.Mode.TEST);

        var activePassiveTransactionInsertionStrategy
                = new ActivePassiveTransactionInsertionStrategy(active, passive);

        final ClientUserToken clientUserToken = new ClientUserToken(null, TestJwtClaims.createClientUserClaims("junit", UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID()));

        var instruction = activePassiveTransactionInsertionStrategy.determineTransactionPersistenceInstruction(
                List.of(),
                clientUserToken,
                UUID.randomUUID(),
                "RABOBANK",
                CurrencyCode.EUR
        );

        assertThat(instruction).isEqualTo(activeStubInstruction);
        Mockito.verify(active).determineTransactionPersistenceInstruction(any(), any(), any(), any(), any());
        Mockito.verify(passive).determineTransactionPersistenceInstruction(any(), any(), any(), any(), any());
    }
}