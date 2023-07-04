package com.yolt.accountsandtransactions.transactions.updates;

import com.yolt.accountsandtransactions.datascience.cycles.DsTransactionCyclesClient;
import com.yolt.accountsandtransactions.datascience.cycles.DsTransactionCyclesClient.DsTransactionCycleNotFound;
import com.yolt.accountsandtransactions.datascience.cycles.DsTransactionCyclesClient.DsTransactionCycleReferenceTransactionNotFound;
import com.yolt.accountsandtransactions.datascience.cycles.dto.DsTransactionCycleCreateRequest;
import com.yolt.accountsandtransactions.datascience.cycles.dto.DsTransactionCycleTransactionKey;
import com.yolt.accountsandtransactions.datascience.cycles.dto.DsTransactionCycleUpdateRequest;
import com.yolt.accountsandtransactions.inputprocessing.enrichments.activities.ActivityEnrichmentService;
import com.yolt.accountsandtransactions.inputprocessing.enrichments.activities.ActivityEnrichmentType;
import com.yolt.accountsandtransactions.inputprocessing.enrichments.api.cycles.DsTransactionCycle;
import com.yolt.accountsandtransactions.transactions.TransactionDTO;
import com.yolt.accountsandtransactions.transactions.TransactionService;
import com.yolt.accountsandtransactions.transactions.cycles.TransactionCycle;
import com.yolt.accountsandtransactions.transactions.cycles.TransactionCyclesService;
import com.yolt.accountsandtransactions.transactions.updates.api.TransactionCyclesCreateRequest;
import com.yolt.accountsandtransactions.transactions.updates.api.TransactionCyclesUpdateRequest;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import nl.ing.lovebird.clienttokens.ClientUserToken;
import nl.ing.lovebird.extendeddata.transaction.TransactionStatus;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuples;

import java.time.LocalDate;
import java.time.Period;
import java.util.UUID;

import static com.yolt.accountsandtransactions.inputprocessing.enrichments.activities.ActivityEnrichmentType.FEEDBACK_TRANSACTION_CYCLES;
import static java.util.UUID.randomUUID;
import static reactor.core.scheduler.Schedulers.boundedElastic;

@Service
@RequiredArgsConstructor
public class TransactionCyclesFeedbackService {

    private final ActivityEnrichmentService activityEnrichmentService;
    private final TransactionService transactionService;
    private final TransactionCyclesService transactionCyclesService;
    private final DsTransactionCyclesClient dsTransactionCyclesClient;

    /**
     * Update a new {@link TransactionCycle}
     *
     * @param clientUserToken   the client identifier
     * @param createRequest the properties of the new {@link TransactionCycle}
     * @return a tuple of type (activityId, TransactionCycle)
     */
    Mono<Tuple2<UUID, TransactionCycle>> createTransactionCycleFeedback(
            final @NonNull ClientUserToken clientUserToken,
            final @NonNull TransactionCyclesCreateRequest createRequest) {

        var ref = createRequest.getTransactionKey();

        var request = DsTransactionCycleCreateRequest.builder()
                .transactionKey(new DsTransactionCycleTransactionKey(ref.getAccountId(), ref.getId(), "REGULAR", ref.getDate()))
                .amount(createRequest.getAmount())
                .period(Period.parse(createRequest.getPeriod()))
                .label(createRequest.getLabel())
                .build();

        // first, check if the reference transaction is booked
        // second, start an activity
        // third, create the transaction-cycle at datascience
        // fourth, upsert the created transaction-cycle in our local database
        return assertReferenceTransactionIsBooked(clientUserToken.getUserIdClaim(), ref.getAccountId(), ref.getDate(), ref.getId())
                .flatMap(unused -> startFeedbackActivity(clientUserToken)
                        .name("create-transaction-cycle")
                        .metrics()
                        .flatMap(activityId -> dsTransactionCyclesClient.createTransactionCycleAsync(clientUserToken, activityId, request)
                                .onErrorMap(DsTransactionCycleReferenceTransactionNotFound.class,
                                        cause -> new ReferenceTransactionNotFound("Transaction cycle not found.", cause))
                                .transform(cycle -> upsert(clientUserToken.getUserIdClaim(), cycle))
                                .map(cycle -> Tuples.of(activityId, cycle))));
    }

    /**
     * Update an existing {@link TransactionCycle}
     *
     * @param clientUserToken   the client identifier
     * @param cycleId       the cycle identifier of the existing {@link TransactionCycle}
     * @param updateRequest the properties to update on the existing {@link TransactionCycle}
     * @return a tuple of type (activityId, TransactionCycle)
     */
    Mono<Tuple2<UUID, TransactionCycle>> updateTransactionCycleFeedback(
            final @NonNull ClientUserToken clientUserToken,
            final @NonNull UUID cycleId,
            final @NonNull TransactionCyclesUpdateRequest updateRequest) {

        var request = DsTransactionCycleUpdateRequest.builder()
                .amount(updateRequest.getAmount())
                .period(Period.parse(updateRequest.getPeriod()))
                .label(updateRequest.getLabel())
                .build();

        return assertTransactionCycleExists(clientUserToken.getUserIdClaim(), cycleId)
                .flatMap(unused -> startFeedbackActivity(clientUserToken)
                        .name("update-transaction-cycle")
                        .metrics()
                        .flatMap(activityId -> dsTransactionCyclesClient.updateTransactionCycleAsync(clientUserToken, activityId, cycleId, request)
                                .onErrorMap(DsTransactionCycleNotFound.class,
                                        cause -> new TransactionCycleNotFound("Transaction cycle not found.", cause))
                                .transform(cycle -> upsert(clientUserToken.getUserIdClaim(), cycle))
                                .map(cycle -> Tuples.of(activityId, cycle))));
    }

    /**
     * Expire a transaction-cycle via feedback
     *
     * @param clientUserToken the client identifier
     * @param cycleId     the transaction -cycle identifier to delete
     * @return A tuple of type (activityId, cycleId)
     */
    Mono<Tuple2<UUID, UUID>> expireTransactionCycleFeedback(
            final @NonNull ClientUserToken clientUserToken,
            final @NonNull UUID cycleId) {

        return assertTransactionCycleExists(clientUserToken.getUserIdClaim(), cycleId)
                .flatMap(unused -> startFeedbackActivity(clientUserToken)
                        .name("delete-transaction-cycle")
                        .metrics()
                        .flatMap(activityId -> dsTransactionCyclesClient.deleteTransactionCycleAsync(clientUserToken, activityId, cycleId)
                                .onErrorMap(DsTransactionCycleNotFound.class,
                                        cause -> new TransactionCycleNotFound("Transaction cycle not found.", cause))
                                .flatMap(id -> expire(clientUserToken.getUserIdClaim(), id))
                                .map(cycle -> Tuples.of(activityId, cycleId))));
    }

    /**
     * Start an {@link ActivityEnrichmentType#FEEDBACK_TRANSACTION_CYCLES} activity
     *
     * @param clientUserToken the client identifier
     * @return the activity-id
     */
    private Mono<UUID> startFeedbackActivity(final @NonNull ClientUserToken clientUserToken) {
        var activityId = randomUUID();
        return Mono.fromRunnable(() -> activityEnrichmentService.startActivityEnrichment(clientUserToken, FEEDBACK_TRANSACTION_CYCLES, activityId))
                .subscribeOn(boundedElastic())
                .thenReturn(activityId);
    }

    /**
     * Assert that the reference transaction exists and is booked.
     *
     * @param userId    the user-id
     * @param accountId the account-id
     * @param date      the date
     * @param id        the transaction-id
     * @return a empty {@link Mono}
     */
    private Mono<TransactionDTO> assertReferenceTransactionIsBooked(
            final @NonNull UUID userId,
            final @NonNull UUID accountId,
            final @NonNull LocalDate date,
            final @NonNull String id) {

        return Mono.fromCallable(() -> {
            var transaction = transactionService.getTransaction(userId, accountId, date, id)
                    .orElseThrow(() -> new ReferenceTransactionNotFound("Transaction not found."));

            if (transaction.getStatus() != TransactionStatus.BOOKED) {
                throw new ReferenceTransactionNotBooked("Transaction is not booked.");
            }

            return transaction;
        }).subscribeOn(boundedElastic());
    }

    private Mono<Boolean> assertTransactionCycleExists(final @NonNull UUID userId, final @NonNull UUID cycleId) {
        return Mono.fromCallable(() -> {
            if (transactionCyclesService.find(userId, cycleId).isEmpty()) {
                throw new TransactionCycleNotFound("Transaction-cycle %s not found.".formatted(cycleId));
            }
            return true; // prevent the callable from completing with an empty mono. It's a trap.
        }).subscribeOn(boundedElastic());
    }

    /**
     * Create or update a {@link DsTransactionCycle}
     *
     * @param userId             the user identifier owning the cycle
     * @param dsTransactionCycle the Datascience transaction-cycle to create or update
     * @return a {@link Mono} containing the created or updated {@link TransactionCycle}
     */
    private Mono<TransactionCycle> upsert(final @NonNull UUID userId, final @NonNull Mono<DsTransactionCycle> dsTransactionCycle) {
        return dsTransactionCycle
                .map(cycle -> TransactionCycle.fromDatascienceTransactionCycle(userId, cycle))
                .flatMap(cycle -> Mono.fromCallable(() -> transactionCyclesService.upsert(cycle))
                        .subscribeOn(Schedulers.boundedElastic()));
    }

    /**
     * Expire a {@link TransactionCycle}.
     *
     * @param userId  the user identifier owning the cycle
     * @param cycleId the cycle identifier
     * @return the cycle-id
     */
    private Mono<UUID> expire(final @NonNull UUID userId, final @NonNull UUID cycleId) {
        return Mono.fromRunnable(() -> transactionCyclesService.expire(userId, cycleId))
                .subscribeOn(boundedElastic())
                .thenReturn(cycleId); // prevent the runnable from completing with an empty mono. It's a trap.
    }

    public static class TransactionCycleNotFound extends RuntimeException {

        public TransactionCycleNotFound(String message) {
            super(message);
        }

        public TransactionCycleNotFound(String message, Throwable cause) {
            super(message, cause);
        }
    }

    public static class ReferenceTransactionNotFound extends RuntimeException {

        public ReferenceTransactionNotFound(String message) {
            super(message);
        }

        public ReferenceTransactionNotFound(String message, Throwable cause) {
            super(message, cause);
        }
    }

    public static class ReferenceTransactionNotBooked extends RuntimeException {

        public ReferenceTransactionNotBooked(String message) {
            super(message);
        }
    }
}
