package com.yolt.accountsandtransactions.inputprocessing;

import lombok.extern.slf4j.Slf4j;
import nl.ing.lovebird.clienttokens.ClientUserToken;
import nl.ing.lovebird.extendeddata.common.CurrencyCode;
import nl.ing.lovebird.providerdomain.ProviderTransactionDTO;
import org.springframework.util.Assert;

import java.util.List;
import java.util.UUID;

@Slf4j
public class ActivePassiveTransactionInsertionStrategy implements TransactionInsertionStrategy {

    public final TransactionInsertionStrategy activeTransactionInsertionStrategy;
    public final TransactionInsertionStrategy passiveTransactionInsertionStrategy;

    public ActivePassiveTransactionInsertionStrategy(
            final TransactionInsertionStrategy activeTransactionInsertionStrategy,
            final TransactionInsertionStrategy passiveTransactionInsertionStrategy) {

        Assert.isTrue(activeTransactionInsertionStrategy.getMode() == Mode.ACTIVE,
                "The activated strategy should run in ACTIVE mode.");
        Assert.isTrue(passiveTransactionInsertionStrategy.getMode() == Mode.TEST,
                "The passive strategy should run in TEST mode.");

        this.activeTransactionInsertionStrategy = activeTransactionInsertionStrategy;
        this.passiveTransactionInsertionStrategy = passiveTransactionInsertionStrategy;
    }

    @Override
    public Instruction determineTransactionPersistenceInstruction(
            final List<ProviderTransactionDTO> upstreamTransactions,
            final ClientUserToken clientUserToken,
            final UUID yoltAccountId,
            final String provider,
            final CurrencyCode currencyCode) {

        try {
            passiveTransactionInsertionStrategy.determineTransactionPersistenceInstruction(upstreamTransactions, clientUserToken, yoltAccountId, provider, currencyCode);
        } catch (Exception e) {
            log.info("Failure while running passive transaction insertion strategy. This is not a production error.", e);
        }

        return activeTransactionInsertionStrategy.determineTransactionPersistenceInstruction(upstreamTransactions, clientUserToken, yoltAccountId, provider, currencyCode);
    }
}