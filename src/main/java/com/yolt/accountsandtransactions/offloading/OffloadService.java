package com.yolt.accountsandtransactions.offloading;

import com.yolt.accountsandtransactions.accounts.Account;
import com.yolt.accountsandtransactions.concurrency.Futures;
import com.yolt.accountsandtransactions.transactions.Transaction;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Service;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static java.lang.String.format;
import static org.springframework.kafka.support.KafkaHeaders.MESSAGE_KEY;
import static org.springframework.kafka.support.KafkaHeaders.TOPIC;

@Slf4j
@Service
public class OffloadService {
    private static final int ACCOUNT_OFFLOAD_SCHEMA_VERSION = 1;
    private static final int TRANSACTION_OFFLOAD_SCHEMA_VERSION = 1;

    private final KafkaTemplate<String, OffloadableEnvelope<?>> kafkaTemplate;
    private final boolean offloadAISEnabled;
    private final String accountsOffloadTopic;
    private final String transactionsOffloadTopic;

    public OffloadService(
            KafkaTemplate<String, OffloadableEnvelope<?>> kafkaTemplate,
            @Value("${yolt.accounts-and-transactions.offload.ais.enabled:false}") boolean offloadAISEnabled,
            @Value("${yolt.kafka.topics.offload-yts-accounts.topic-name}") String accountsOffloadTopic,
            @Value("${yolt.kafka.topics.offload-yts-transactions.topic-name}") String transactionsOffloadTopic) {
        this.kafkaTemplate = kafkaTemplate;
        this.offloadAISEnabled = offloadAISEnabled;
        this.accountsOffloadTopic = accountsOffloadTopic;
        this.transactionsOffloadTopic = transactionsOffloadTopic;
    }

    public CompletableFuture<Void> offloadInsertOrUpdateAsync(@NonNull Account account, @NonNull UUID clientId) {
        var payload = mapToOffloadable(account, clientId);
        var envelope = OffloadableEnvelope.createInsertOrUpdate(ACCOUNT_OFFLOAD_SCHEMA_VERSION, account.getId(), payload);

        return send(envelope, accountsOffloadTopic, account.getUserId());
    }

    public CompletableFuture<Void> offloadDeleteAsync(@NonNull Account account) {
        var envelope = OffloadableEnvelope.createDelete(ACCOUNT_OFFLOAD_SCHEMA_VERSION, account.getId());

        return send(envelope, this.accountsOffloadTopic, account.getUserId());
    }

    public CompletableFuture<Void> offloadInsertOrUpdateAsync(@NonNull Transaction transaction) {
        var payload = mapToOffloadable(transaction);
        var envelope = OffloadableEnvelope.createInsertOrUpdate(TRANSACTION_OFFLOAD_SCHEMA_VERSION, createTransactionId(transaction), payload);

        return send(envelope, transactionsOffloadTopic, transaction.getUserId());
    }

    public CompletableFuture<Void> offloadDeleteAsync(@NonNull Transaction transaction) {
        var envelope = OffloadableEnvelope.createDelete(TRANSACTION_OFFLOAD_SCHEMA_VERSION, createTransactionId(transaction));

        return send(envelope, this.transactionsOffloadTopic, transaction.getUserId());
    }

    private static String createTransactionId(@NonNull Transaction transaction) {
        return format("%s:%s:%s:%s", transaction.getUserId(), transaction.getAccountId(), transaction.getDate(), transaction.getId());
    }

    private CompletableFuture<Void> send(OffloadableEnvelope<?> envelope, String topic, UUID userId) {
        if (!offloadAISEnabled) {
            return CompletableFuture.completedFuture(null);
        }

        var message = MessageBuilder
                .withPayload(envelope)
                .setHeader(TOPIC, topic)
                .setHeader(MESSAGE_KEY, userId.toString())
                .build();

        return Futures.from(kafkaTemplate.send(message))
                .handle((ignored, throwable) -> {
                    if (throwable != null) {
                        log.error("Failed to offload CAM data.", throwable);
                    }
                    return null;
                })
                .thenApply(ignored -> null);
    }

    private OffloadableTransaction mapToOffloadable(Transaction transaction) {
        return OffloadableTransaction.builder()
                .accountId(transaction.getAccountId())
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
                .date(transaction.getDate())
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
                .id(transaction.getId())
                .purposeCode(transaction.getPurposeCode())
                .remittanceInformationStructured(transaction.getRemittanceInformationStructured())
                .remittanceInformationUnstructured(transaction.getRemittanceInformationUnstructured())
                .status(transaction.getStatus())
                .timestamp(transaction.getTimestamp())
                .timeZone(transaction.getTimeZone())
                .userId(transaction.getUserId())
                .valueDate(transaction.getValueDate())
                .build();
    }

    private OffloadableAccount mapToOffloadable(Account account, UUID clientId) {
        return OffloadableAccount.builder()
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
    }
}
