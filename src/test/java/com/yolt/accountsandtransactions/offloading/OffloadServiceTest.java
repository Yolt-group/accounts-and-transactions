package com.yolt.accountsandtransactions.offloading;

import com.yolt.accountsandtransactions.BaseIntegrationTest;
import com.yolt.accountsandtransactions.accounts.Account;
import com.yolt.accountsandtransactions.transactions.Transaction;
import com.yolt.accountsandtransactions.transactions.TransactionService.TransactionPrimaryKey;
import nl.ing.lovebird.extendeddata.transaction.TransactionStatus;
import org.awaitility.Duration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.time.LocalDate;
import java.util.Optional;
import java.util.UUID;

import static com.yolt.accountsandtransactions.TestBuilders.createAllFieldsRandomAccount;
import static com.yolt.accountsandtransactions.TestBuilders.createTransactionTemplate;
import static java.lang.String.format;
import static java.util.UUID.randomUUID;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

class OffloadServiceTest extends BaseIntegrationTest {

    @Autowired
    OffloadService offloadService;

    @Autowired
    OffloadedTransactionsConsumer offloadedTransactionsConsumer;

    @Autowired
    OffloadedAccountsConsumer offloadedAccountsConsumer;

    @BeforeEach
    void onBeforeEach() {
        offloadedTransactionsConsumer.reset();
        offloadedAccountsConsumer.reset();
    }

    @AfterEach
    void onAfterEach() {
        offloadedTransactionsConsumer.reset();
        offloadedAccountsConsumer.reset();
    }

    @Test
    void offloadInsertOrUpdateAsyncTransaction() {

        TransactionPrimaryKey transactionPrimaryKey = new TransactionPrimaryKey(randomUUID(), randomUUID(), LocalDate.EPOCH, "tx-1", TransactionStatus.BOOKED);
        Transaction transaction = createTransactionTemplate(transactionPrimaryKey);

        offloadService.offloadInsertOrUpdateAsync(transaction).join();

        await().atMost(Duration.TEN_SECONDS).untilAsserted(() -> {
            Optional<OffloadableEnvelope<?>> envelope = offloadedTransactionsConsumer.head();
            assertThat(envelope).isNotEmpty();

            envelope.ifPresent(offloadableEnvelope -> {
                assertThat(offloadableEnvelope.getDelete()).isFalse();
                assertThat(offloadableEnvelope.getSchemaVersion()).isOne();
                assertThat(offloadableEnvelope.getEntityId())
                        .isEqualTo(format("%s:%s:%s:%s", transactionPrimaryKey.getUserId(), transactionPrimaryKey.getAccountId(), transactionPrimaryKey.getDate(), transactionPrimaryKey.getId()));

                assertThat(envelope.flatMap(OffloadableEnvelope::getPayload)).contains(
                        OffloadableTransaction.builder()
                                .accountId(transactionPrimaryKey.getAccountId())
                                .amount(transaction.getAmount())
                                .bankTransactionCode(transaction.getBankTransactionCode())
                                .bookingDate(transaction.getBookingDate())
                                .bankSpecific(transaction.getBankSpecific())
                                .createdAt(transaction.getCreatedAtOrEPOCH())
                                .creditorBban(transaction.getCreditorBban())
                                .creditorIban(transaction.getCreditorIban())
                                .creditorMaskedPan(transaction.getCreditorMaskedPan())
                                .creditorName(transaction.getCreditorName())
                                .creditorPan(transaction.getCreditorPan())
                                .creditorSortCodeAccountNumber(transaction.getCreditorSortCodeAccountNumber())
                                .currency(transaction.getCurrency())
                                .date(transactionPrimaryKey.getDate())
                                .debtorBban(transaction.getDebtorBban())
                                .debtorIban(transaction.getDebtorIban())
                                .debtorMaskedPan(transaction.getDebtorMaskedPan())
                                .debtorName(transaction.getDebtorName())
                                .debtorPan(transaction.getDebtorPan())
                                .debtorSortCodeAccountNumber(transaction.getDebtorSortCodeAccountNumber())
                                .endToEndId(transaction.getEndToEndId())
                                .exchangeRateCurrencyFrom(transaction.getExchangeRateCurrencyFrom())
                                .exchangeRateCurrencyTo(transaction.getExchangeRateCurrencyTo())
                                .exchangeRateRate(transaction.getExchangeRateRate())
                                .externalId(transaction.getExternalId())
                                .lastUpdatedTime(transaction.getLastUpdatedTime())
                                .originalAmountAmount(transaction.getOriginalAmountAmount())
                                .originalAmountCurrency(transaction.getOriginalAmountCurrency())
                                .originalCategory(transaction.getOriginalCategory())
                                .originalMerchantName(transaction.getOriginalMerchantName())
                                .id(transactionPrimaryKey.getId())
                                .purposeCode(transaction.getPurposeCode())
                                .remittanceInformationStructured(transaction.getRemittanceInformationStructured())
                                .remittanceInformationUnstructured(transaction.getRemittanceInformationUnstructured())
                                .status(transaction.getStatus())
                                .timestamp(transaction.getTimestamp())
                                .timeZone(transaction.getTimeZone())
                                .userId(transactionPrimaryKey.getUserId())
                                .valueDate(transaction.getValueDate())
                                .build());
            });
        });
    }

    @Test
    void offloadDeleteAsyncTransaction() {
        TransactionPrimaryKey transactionPrimaryKey = new TransactionPrimaryKey(randomUUID(), randomUUID(), LocalDate.EPOCH, "tx-2", TransactionStatus.BOOKED);
        Transaction transaction = createTransactionTemplate(transactionPrimaryKey);

        offloadService.offloadDeleteAsync(transaction).join();

        await().atMost(Duration.TEN_SECONDS).untilAsserted(() -> {
            Optional<OffloadableEnvelope<?>> envelope = offloadedTransactionsConsumer.head();
            assertThat(envelope).isNotEmpty();

            envelope.ifPresent(offloadableEnvelope -> {
                assertThat(offloadableEnvelope.getDelete()).isTrue();
                assertThat(offloadableEnvelope.getSchemaVersion()).isOne();
                assertThat(offloadableEnvelope.getEntityId())
                        .isEqualTo(format("%s:%s:%s:%s", transactionPrimaryKey.getUserId(), transactionPrimaryKey.getAccountId(), transactionPrimaryKey.getDate(), transactionPrimaryKey.getId()));
                assertThat(offloadableEnvelope.getPayload()).isEmpty();
            });
        });
    }

    @Test
    void offloadInsertOrUpdateAsyncAccount() {

        Account account = createAllFieldsRandomAccount(randomUUID(), randomUUID());
        UUID clientId = randomUUID();

        offloadService.offloadInsertOrUpdateAsync(account, clientId).join();

        await().atMost(Duration.TEN_SECONDS).untilAsserted(() -> {
            Optional<OffloadableEnvelope<?>> envelope = offloadedAccountsConsumer.head();
            assertThat(envelope).isNotEmpty();

            envelope.ifPresent(offloadableEnvelope -> {
                assertThat(offloadableEnvelope.getDelete()).isFalse();
                assertThat(offloadableEnvelope.getSchemaVersion()).isOne();
                assertThat(offloadableEnvelope.getEntityId()).isEqualTo(account.getId().toString());

                OffloadableAccount.builder()
                        .accountHolder(account.getAccountHolder())
                        .availableCredit(account.getAvailableCredit())
                        .balance(account.getBalance())
                        .balances(account.getBalances())
                        .bban(account.getBban())
                        .bankSpecific(account.getBankSpecific())
                        .bic(account.getBic())
                        .clientId(clientId)
                        .createdAt(account.getCreatedAtOrDefault())
                        .creditLimit(account.getCreditLimit())
                        .currency(account.getCurrency())
                        .externalId(account.getExternalId())
                        .hidden(account.isHidden())
                        .iban(account.getIban())
                        .id(account.getId())
                        .interestRate(account.getInterestRate())
                        .isMoneyPotOf(account.getIsMoneyPotOf())
                        .lastDataFetchTime(account.getLastDataFetchTime())
                        .maskedPan(account.getMaskedPan())
                        .linkedAccount(account.getLinkedAccount())
                        .pan(account.getPan())
                        .name(account.getName())
                        .product(account.getProduct())
                        .siteId(account.getSiteId())
                        .sortCodeAccountNumber(account.getSortCodeAccountNumber())
                        .status(account.getStatus())
                        .type(account.getType())
                        .userId(account.getUserId())
                        .usage(account.getUsage())
                        .userSiteId(account.getUserSiteId())
                        .build();
            });
        });
    }

    @Test
    void offloadDeleteAsyncAccount() {
        Account account = createAllFieldsRandomAccount(randomUUID(), randomUUID());

        offloadService.offloadDeleteAsync(account).join();

        await().atMost(Duration.TEN_SECONDS).untilAsserted(() -> {
            Optional<OffloadableEnvelope<?>> envelope = offloadedAccountsConsumer.head();
            assertThat(envelope).isNotEmpty();

            envelope.ifPresent(offloadableEnvelope -> {
                assertThat(offloadableEnvelope.getDelete()).isTrue();
                assertThat(offloadableEnvelope.getSchemaVersion()).isOne();
                assertThat(offloadableEnvelope.getEntityId()).isEqualTo(account.getId().toString());
                assertThat(offloadableEnvelope.getPayload()).isEmpty();
            });
        });
    }
}