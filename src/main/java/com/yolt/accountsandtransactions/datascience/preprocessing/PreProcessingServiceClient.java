package com.yolt.accountsandtransactions.datascience.preprocessing;

import com.yolt.accountsandtransactions.datascience.DSLegacy;
import com.yolt.accountsandtransactions.datascience.DsShortTransactionKeyDTO;
import com.yolt.accountsandtransactions.datascience.categories.DataScienceRequestFailedException;
import com.yolt.accountsandtransactions.datascience.categories.DatascienceTransactionReferenceNotFoundException;
import com.yolt.accountsandtransactions.datascience.categories.dto.DsCategoriesFeedbackDTO;
import com.yolt.accountsandtransactions.datascience.categories.dto.DsCategoriesFeedbackGroupsDTO;
import com.yolt.accountsandtransactions.datascience.categories.dto.DsCategoriesTransactionKeyDTO;
import com.yolt.accountsandtransactions.datascience.categories.dto.DsCategoriesUpdatedTransactionsDTO;
import com.yolt.accountsandtransactions.datascience.counterparties.client.dto.*;
import com.yolt.accountsandtransactions.datascience.preprocessing.dto.DsSimilarTransactionsDTO;
import com.yolt.accountsandtransactions.transactions.TransactionDTO;
import com.yolt.accountsandtransactions.transactions.updates.api.SeedTransactionKey;
import lombok.extern.slf4j.Slf4j;
import nl.ing.lovebird.clienttokens.ClientUserToken;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.lang.NonNull;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.BodyInserters;
import org.springframework.web.reactive.function.client.ClientResponse;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

import static com.google.common.collect.Lists.newArrayList;
import static com.yolt.accountsandtransactions.datascience.UserContext.USER_CONTEXT_HEADER_KEY;
import static java.time.temporal.ChronoUnit.SECONDS;
import static nl.ing.lovebird.clienttokens.constants.ClientTokenConstants.CLIENT_TOKEN_HEADER_NAME;
import static org.springframework.http.HttpStatus.NOT_FOUND;
import static org.springframework.http.MediaType.APPLICATION_JSON;
import static org.springframework.http.MediaType.TEXT_PLAIN;

@Component
@Slf4j
public class PreProcessingServiceClient {
    private static final int DEFAULT_TIMEOUT_IN_SECONDS = 10;
    private static final String ACTIVITY_ID_HEADER_KEY = "activity-id";

    private final WebClient webClient;

    public PreProcessingServiceClient(final WebClient.Builder webClientBuilder,
                                      @Value("${yolt.service.preprocessing.url:https://preprocessing/preprocessing/}") final String categoriesUrl) {
        this.webClient = webClientBuilder.baseUrl(categoriesUrl).build();
    }

    public Optional<DsSimilarTransactionsDTO> getSimilarTransactions(
            @NonNull final ClientUserToken clientUserToken,
            @NonNull final UUID accountId,
            @NonNull final String transactionId) {

        log.debug("Send request to get transactions similar to {}/{}", accountId, transactionId); //NOSHERIFF

        return this.webClient.get()
                .uri(builder -> builder.path("/v1/similar-transactions")
                        .queryParam("accountId", accountId.toString())
                        .queryParam("transactionId", transactionId)
                        .build())
                .accept(APPLICATION_JSON, TEXT_PLAIN)
                .header(USER_CONTEXT_HEADER_KEY, DSLegacy.fakeUserContext(clientUserToken.getUserIdClaim()).toJson())
                .retrieve()
                .onStatus(httpStatus -> httpStatus.equals(NOT_FOUND), clientResponse -> on404ClientResponse(clientResponse, accountId, transactionId))
                .onStatus(HttpStatus::isError, clientResponse -> onErrorClientResponse(clientResponse, accountId, transactionId))
                .bodyToMono(DsSimilarTransactionsDTO.class)
                .blockOptional(Duration.of(DEFAULT_TIMEOUT_IN_SECONDS, SECONDS));
    }


    public Mono<MerchantsInCountriesDTO> getMerchantsByCountries() {
        return this.webClient.get()
                .uri(builder -> builder.path("/counterparties/merchants-by-countries").build())
                .accept(APPLICATION_JSON, TEXT_PLAIN)
                .retrieve()
                .bodyToMono(MerchantsInCountriesDTO.class);
    }

    public DsCounterpartiesAdjustedDTO getAdjustedCounterpartiesIgnoreErrors(ClientUserToken clientUserToken) {

        return this.webClient.get()
                .uri(builder -> builder.path("/counterparties/users/{userId}/feedback/counterparty-names").build(clientUserToken.getUserIdClaim()))
                .accept(APPLICATION_JSON, TEXT_PLAIN)
                .header(USER_CONTEXT_HEADER_KEY, DSLegacy.fakeUserContext(clientUserToken.getUserIdClaim()).toJson())
                .retrieve()
                .bodyToMono(DsCounterpartiesAdjustedDTO.class)
                .doOnError(throwable -> log.error("Failed to find counter-party names for feedback.", throwable))
                .onErrorResume(__ -> Mono.empty())
                .blockOptional(Duration.of(DEFAULT_TIMEOUT_IN_SECONDS, SECONDS))
                .orElseGet(() -> DsCounterpartiesAdjustedDTO.builder().counterpartyNames(newArrayList()).build());
    }

    public Mono<DsCounterpartiesFeedbackResponseDTO> updateCounterparty(
            final ClientUserToken clientUserToken,
            final UUID activityId,
            final DsCounterpartiesFeedbackDTO counterpartyUpdateDTO) {

        log.debug("Send request to update counterparty ({}) for activity {}", counterpartyUpdateDTO.getCounterpartyName(), activityId.toString());
        return this.webClient.put()
                .uri(builder -> builder
                        .path("/counterparties/users/{userId}/feedback")
                        .build(clientUserToken.getUserIdClaim()))
                .contentType(APPLICATION_JSON)
                .accept(APPLICATION_JSON, TEXT_PLAIN)
                .body(BodyInserters.fromValue(counterpartyUpdateDTO))
                .header(USER_CONTEXT_HEADER_KEY, DSLegacy.fakeUserContext(clientUserToken.getUserIdClaim()).toJson())
                .header(ACTIVITY_ID_HEADER_KEY, activityId.toString())
                .header(CLIENT_TOKEN_HEADER_NAME, clientUserToken.getSerialized())
                .retrieve()
                .bodyToMono(DsCounterpartiesFeedbackResponseDTO.class)
                .doOnError(throwable -> log.error("Failed to update counterparty.", throwable));
    }

    public Mono<DsCounterpartiesFeedbackGroupsResponseDTO> updateMultipleCounterparties(
            ClientUserToken clientUserToken,
            UUID activityId,
            DsCounterpartiesFeedbackGroupsDTO dsCounterpartiesFeedbackGroupsDTO) {
        log.debug("Send request to update multiple counterparties ({}) for activity {}", dsCounterpartiesFeedbackGroupsDTO.getCounterpartyName(), activityId.toString());
        return this.webClient.put()
                .uri(builder -> builder
                        .path("/counterparties/users/{userId}/feedback/groups")
                        .build(clientUserToken.getUserIdClaim()))
                .contentType(APPLICATION_JSON)
                .accept(APPLICATION_JSON, TEXT_PLAIN)
                .body(BodyInserters.fromValue(dsCounterpartiesFeedbackGroupsDTO))
                .header(USER_CONTEXT_HEADER_KEY, DSLegacy.fakeUserContext(clientUserToken.getUserIdClaim()).toJson())
                .header(ACTIVITY_ID_HEADER_KEY, activityId.toString())
                .header(CLIENT_TOKEN_HEADER_NAME, clientUserToken.getSerialized())
                .retrieve()
                .bodyToMono(DsCounterpartiesFeedbackGroupsResponseDTO.class);
    }

    /**
     * Instruct datascience to apply category feedback for a single transaction
     *
     * @param activityId      the activity-ID associated with this feedback
     * @param seedTransaction the transaction-key identifying the seed/ reference transaction
     * @param category        the category to apply
     */
    public Mono<Void> applyCategoriesFeedbackAsync(
            @NonNull final ClientUserToken clientUserToken,
            @NonNull final UUID activityId,
            @NonNull final SeedTransactionKey seedTransaction,
            @NonNull final String category) {
        var payload = DsCategoriesFeedbackDTO.builder()
                .transactionKey(DsCategoriesTransactionKeyDTO.builder()
                        .userId(clientUserToken.getUserIdClaim())
                        .accountId(seedTransaction.getAccountId())
                        .transactionId(seedTransaction.getId())
                        .transactionType("REGULAR")
                        .date(seedTransaction.getDate())
                        .build())
                .category(category)
                .build();

        log.debug("Send request to update category ({}) for activity {}", category, activityId);

        return this.webClient.put()
                .uri(builder -> builder.path("/categories/v1/feedback/single").build())
                .contentType(APPLICATION_JSON)
                .accept(APPLICATION_JSON, TEXT_PLAIN)
                .body(BodyInserters.fromValue(payload))
                .header(USER_CONTEXT_HEADER_KEY,  DSLegacy.fakeUserContext(clientUserToken.getUserIdClaim()).toJson())
                .header(ACTIVITY_ID_HEADER_KEY, activityId.toString())
                .header(CLIENT_TOKEN_HEADER_NAME, clientUserToken.getSerialized())
                .retrieve()
                .onStatus(httpStatus -> httpStatus.equals(HttpStatus.NOT_FOUND), clientResponse -> on404ClientResponse(clientResponse, seedTransaction.getAccountId(), seedTransaction.getId()))
                .onStatus(HttpStatus::isError, clientResponse -> onErrorClientResponse(clientResponse, seedTransaction.getAccountId(), seedTransaction.getId()))
                .bodyToMono(Void.class);
    }

    /**
     * Instruct datascience to apply category feedback to a group of transactions.
     *
     * @param activityId      the activity-ID associated with this feedback
     * @param seedTransaction the seed transaction
     * @param groupSelectors  the groups to apply the feedback to
     * @param category        the category to apply
     * @return Keys of the transactions that are updated (short version of the transaction key).
     */
    public Mono<List<DsShortTransactionKeyDTO>> applyCategoriesFeedbackGroupsAsync(
            @NonNull final ClientUserToken clientUserToken,
            @NonNull final UUID activityId,
            @NonNull final TransactionDTO seedTransaction,
            @NonNull final Set<String> groupSelectors,
            @NonNull final String category) {

        var payload = DsCategoriesFeedbackGroupsDTO.builder()
                .userId(clientUserToken.getUserIdClaim())
                .seedTransaction(DsCategoriesTransactionKeyDTO.builder()
                        .userId(clientUserToken.getUserIdClaim())
                        .accountId(seedTransaction.getAccountId())
                        .transactionId(seedTransaction.getId())
                        .date(seedTransaction.getDate())
                        .transactionType("REGULAR")
                        .build())
                .groupSelectors(groupSelectors)
                .category(category)
                .build();

        log.debug("Send request to update multiple categories ({}) for activity {}", category, activityId);

        return this.webClient.put()
                .uri(builder -> builder.path("/categories/v1/feedback/groups").build())
                .contentType(MediaType.APPLICATION_JSON)
                .accept(MediaType.APPLICATION_JSON, MediaType.TEXT_PLAIN)
                .body(BodyInserters.fromValue(payload))
                .header(USER_CONTEXT_HEADER_KEY, DSLegacy.fakeUserContext(clientUserToken.getUserIdClaim()).toJson())
                .header(ACTIVITY_ID_HEADER_KEY, activityId.toString())
                .header(CLIENT_TOKEN_HEADER_NAME, clientUserToken.getSerialized())
                .retrieve()
                .onStatus(httpStatus -> httpStatus.equals(HttpStatus.NOT_FOUND), clientResponse -> on404ClientResponse(clientResponse, seedTransaction.getAccountId(), seedTransaction.getId()))
                .onStatus(HttpStatus::isError, clientResponse -> onErrorClientResponse(clientResponse, seedTransaction.getAccountId(), seedTransaction.getId()))
                .bodyToMono(DsCategoriesUpdatedTransactionsDTO.class)
                .map(DsCategoriesUpdatedTransactionsDTO::getTransactions);
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
    private static Mono<? extends Throwable> on404ClientResponse(@NonNull final ClientResponse clientResponse, @NonNull final UUID accountId, @NonNull final String transactionId) {
        return clientResponse.toEntity(String.class)
                .flatMap(responseEntity -> Mono.error(new DatascienceTransactionReferenceNotFoundException(responseEntity.getBody(), accountId, transactionId)));
    }

    private static Mono<? extends Throwable> onErrorClientResponse(@NonNull final ClientResponse clientResponse, @NonNull final UUID accountId, @NonNull final String transactionId) {
        return clientResponse.toEntity(String.class)
                .flatMap(responseEntity -> Mono.error(new DataScienceRequestFailedException(responseEntity.getBody(), clientResponse.statusCode().value(), accountId, transactionId)));
    }


}
