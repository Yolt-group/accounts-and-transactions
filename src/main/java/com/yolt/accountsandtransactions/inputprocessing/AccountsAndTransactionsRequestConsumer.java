package com.yolt.accountsandtransactions.inputprocessing;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import nl.ing.lovebird.clienttokens.ClientUserToken;
import nl.ing.lovebird.extendeddata.transaction.TransactionStatus;
import nl.ing.lovebird.logging.LogTypeMarker;
import nl.ing.lovebird.providerdomain.ProviderTransactionDTO;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

import java.util.UUID;

import static nl.ing.lovebird.clienttokens.constants.ClientTokenConstants.CLIENT_TOKEN_HEADER_NAME;

@Component
@Slf4j
class AccountsAndTransactionsRequestConsumer {

    private final AccountsAndTransactionsService service;
    private final KafkaTemplate<String, String> stringKafkaTemplate;
    private final String requestsErrorsTopic;
    private final ObjectMapper objectMapper;

    AccountsAndTransactionsRequestConsumer(AccountsAndTransactionsService service,
                                           @Value("${yolt.kafka.topics.requests-errors.topic-name}") String requestsErrorsTopic,
                                           KafkaTemplate<String, String> stringKafkaTemplate,
                                           ObjectMapper objectMapper) {
        this.service = service;
        this.stringKafkaTemplate = stringKafkaTemplate;
        this.requestsErrorsTopic = requestsErrorsTopic;
        this.objectMapper = objectMapper;
    }

    @KafkaListener(topics = "${yolt.kafka.topics.ingestion-requests.topic-name}",
            concurrency = "${yolt.kafka.topics.ingestion-requests.listener-concurrency}")
    public void transactionsUpdate(@Payload AccountsAndTransactionsRequestDTO accountsAndTransactionsRequestDTO,
                                   @Header(value = CLIENT_TOKEN_HEADER_NAME) final @NonNull ClientUserToken clientUserToken
    ) throws JsonProcessingException {
        UUID userId = clientUserToken.getUserIdClaim();
        try {
            // Can throw.
            validateProviderAccountDTO(accountsAndTransactionsRequestDTO);

            // XXX remove all transactions with status HOLD.  Datascience has no concept of HOLD transaction, so we ignore
            //     them entirely.
            boolean didRemoveHoldTransaction = accountsAndTransactionsRequestDTO.getIngestionAccounts().stream()
                    .map(a -> a.getTransactions().removeIf(trx -> trx.getStatus() == TransactionStatus.HOLD))
                    .reduce(false, (a, b) -> a || b);
            if (didRemoveHoldTransaction) {
                log.warn("Removed HOLD transactions for site {}.", accountsAndTransactionsRequestDTO.getSiteId());
            }

            service.processAccountsAndTransactionsForUserSite(clientUserToken, accountsAndTransactionsRequestDTO);
        } catch (RuntimeException e) {
            log.error(LogTypeMarker.getDataErrorMarker(), "Error while processing update for user {}.  Sending message to {} topic.", userId, requestsErrorsTopic, e);
            stringKafkaTemplate.send(requestsErrorsTopic, userId.toString(), objectMapper.writeValueAsString(accountsAndTransactionsRequestDTO));
            return;
        }

        log.debug("Processed account update with key {}", userId);
    }

    private void validateProviderAccountDTO(AccountsAndTransactionsRequestDTO accountsAndTransactionsRequestDTO) {
        // Can throw.
        accountsAndTransactionsRequestDTO.getIngestionAccounts().stream()
                .flatMap(a -> a.getTransactions().stream())
                .forEach(ProviderTransactionDTO::validate);
    }
}
