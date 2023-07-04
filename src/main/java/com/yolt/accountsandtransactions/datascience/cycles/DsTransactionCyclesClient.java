package com.yolt.accountsandtransactions.datascience.cycles;

import com.yolt.accountsandtransactions.datascience.DSLegacy;
import com.yolt.accountsandtransactions.datascience.cycles.dto.DsTransactionCycleCreateRequest;
import com.yolt.accountsandtransactions.datascience.cycles.dto.DsTransactionCycleTransactionKey;
import com.yolt.accountsandtransactions.datascience.cycles.dto.DsTransactionCycleUpdateRequest;
import com.yolt.accountsandtransactions.inputprocessing.enrichments.api.cycles.DsTransactionCycle;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import nl.ing.lovebird.clienttokens.ClientUserToken;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.ClientResponse;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.UUID;

import static com.yolt.accountsandtransactions.datascience.UserContext.USER_CONTEXT_HEADER_KEY;
import static nl.ing.lovebird.clienttokens.constants.ClientTokenConstants.CLIENT_TOKEN_HEADER_NAME;

@Service
@Slf4j
public class DsTransactionCyclesClient {
    private static final String ACTIVITY_ID_HEADER_KEY = "activity-id";

    private final WebClient webClient;

    public DsTransactionCyclesClient(final WebClient.Builder webClientBuilder,
                                     final @NonNull @Value("${service.transaction-cycles.url:https://transaction-cycles/transaction-cycles}") String transactionCyclesUrl) {
        this.webClient = webClientBuilder
                .baseUrl(transactionCyclesUrl)
                .build();
    }

    /**
     * Create a new transaction-cycle at datascience
     *
     * @param clientUserToken the client-token to identify the client
     * @param activityId  the activity-id identifies the activity subsequently triggered by the update
     * @param requestBody the transaction-cycle creation parameters
     * @return a transaction cycle is successfully created, error otherwise
     */
    public Mono<DsTransactionCycle> createTransactionCycleAsync(
            final @NonNull ClientUserToken clientUserToken,
            final @NonNull UUID activityId,
            final @NonNull DsTransactionCycleCreateRequest requestBody) {

        UUID userId = clientUserToken.getUserIdClaim();

        var userContext = DSLegacy.fakeUserContext(userId);

        return webClient.post()
                .uri("/users/{userId}/transaction-cycles", userId)
                .header(CLIENT_TOKEN_HEADER_NAME, clientUserToken.getSerialized())
                .header(USER_CONTEXT_HEADER_KEY, userContext.toJson())
                .header(ACTIVITY_ID_HEADER_KEY, activityId.toString())
                .bodyValue(requestBody)
                .retrieve()
                .onStatus(
                        httpStatus -> httpStatus.equals(HttpStatus.BAD_REQUEST),
                        clientResponse -> clientResponse.toEntity(String.class) // consume entity to prevent resource leakage
                                .flatMap(responseEntity -> Mono.error(new DsTransactionCycleReferenceTransactionNotFound(responseEntity.getBody(), requestBody.getTransactionKey()))))
                .onStatus(HttpStatus::isError, DsTransactionCyclesClient::onErrorClientResponse)
                .bodyToMono(DsTransactionCycle.class);
    }

    public Mono<DsTransactionCycle> updateTransactionCycleAsync(
            final @NonNull ClientUserToken clientUserToken,
            final @NonNull UUID activityId,
            final @NonNull UUID cycleId,
            final @NonNull DsTransactionCycleUpdateRequest requestBody) {

        UUID userId = clientUserToken.getUserIdClaim();
        
        return webClient.put()
                .uri("/users/{userId}/transaction-cycles/{cycleId}", userId, cycleId)
                .header(CLIENT_TOKEN_HEADER_NAME, clientUserToken.getSerialized())
                .header(USER_CONTEXT_HEADER_KEY, DSLegacy.fakeUserContext(userId).toJson())
                .header(ACTIVITY_ID_HEADER_KEY, activityId.toString())
                .bodyValue(requestBody)
                .retrieve()
                .onStatus(
                        httpStatus -> httpStatus.equals(HttpStatus.NOT_FOUND),
                        clientResponse -> clientResponse.toEntity(String.class) // consume entity to prevent resource leakage
                                .flatMap(responseEntity -> Mono.error(new DsTransactionCycleNotFound(responseEntity.getBody(), cycleId))))
                .onStatus(HttpStatus::isError, DsTransactionCyclesClient::onErrorClientResponse)
                .bodyToMono(DsTransactionCycle.class);
    }

    /**
     * Delete a single transaction-cycle at datascience
     *
     * @param clientUserToken the client-token to identify the client
     * @param activityId  the activity-id identifies the activity subsequently triggered by this delete
     * @param cycleId     the transaction-cycle identifier to delete
     * @return A {@link Mono} containing the cycle-id
     */
    public Mono<UUID> deleteTransactionCycleAsync(
            final @NonNull ClientUserToken clientUserToken,
            final @NonNull UUID activityId,
            final @NonNull UUID cycleId) {

        UUID userId = clientUserToken.getUserIdClaim();
        
        return webClient
                .method(HttpMethod.DELETE)
                .uri("/users/{userId}/transaction-cycles/{cycleId}", userId, cycleId)
                .header(CLIENT_TOKEN_HEADER_NAME, clientUserToken.getSerialized())
                .header(USER_CONTEXT_HEADER_KEY, DSLegacy.fakeUserContext(clientUserToken.getUserIdClaim()).toJson())
                .header(ACTIVITY_ID_HEADER_KEY, activityId.toString())
                .retrieve()
                .onStatus(
                        httpStatus -> httpStatus.equals(HttpStatus.NOT_FOUND),
                        clientResponse -> clientResponse.toEntity(String.class) // consume entity to prevent resource leakage
                                .flatMap(responseEntity -> Mono.error(new DsTransactionCycleNotFound(responseEntity.getBody(), cycleId))))
                .onStatus(HttpStatus::isError, DsTransactionCyclesClient::onErrorClientResponse)
                .bodyToMono(Void.class)
                .thenReturn(cycleId);
    }

    /**
     * Deletes all data for given account at datascience
     *
     * @param userId user ID
     * @param accountId account ID
     */
    public void deleteAccountData(
            @NonNull final UUID userId,
            @NonNull final UUID accountId) {

        webClient.delete()
                .uri("/users/{userId}/accounts/{accountId}", userId, accountId)
                .retrieve()
                .toBodilessEntity()
                .timeout(Duration.ofSeconds(5))
                .retry(2)
                .onErrorResume(throwable -> {
                    log.error("Error deleting data for account {}. {}", accountId, throwable.getMessage());
                    return Mono.empty();
                })
                .block();
    }

    private static Mono<? extends Throwable> onErrorClientResponse(final @NonNull ClientResponse clientResponse) {

        return clientResponse.toEntity(String.class) // consume entity to prevent resource leakage
                .flatMap(responseEntity -> Mono.error(new RuntimeException("Datascience transaction-cycles clients uncaught error.")));
    }

    @RequiredArgsConstructor
    public static class DsTransactionCycleNotFound extends RuntimeException {

        private final String reply;
        private final UUID cycleId;

        @Override
        public String getMessage() {
            return "Datascience transaction-cycle (%s) not found. Server replied:\n %s".formatted(cycleId, reply);
        }
    }

    @RequiredArgsConstructor
    public static class DsTransactionCycleReferenceTransactionNotFound extends RuntimeException {

        private final String reply;
        private final DsTransactionCycleTransactionKey transactionKey;

        @Override
        public String getMessage() {
            return "Datascience transaction-cycle reference transaction (%s) not found. Server replied:\n %s".formatted(transactionKey, reply);
        }
    }
}
