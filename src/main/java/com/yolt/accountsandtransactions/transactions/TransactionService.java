package com.yolt.accountsandtransactions.transactions;

import com.google.common.annotations.VisibleForTesting;
import com.yolt.accountsandtransactions.accounts.AccountReferencesDTO;
import com.yolt.accountsandtransactions.inputprocessing.AccountFromProviders;
import com.yolt.accountsandtransactions.inputprocessing.ProviderTransactionWithId;
import com.yolt.accountsandtransactions.inputprocessing.TransactionInsertionStrategy.Instruction.InstructionType;
import com.yolt.accountsandtransactions.metrics.AccountsAndTransactionMetrics;
import com.yolt.accountsandtransactions.offloading.OffloadService;
import com.yolt.accountsandtransactions.transactions.TransactionDTO.EnrichmentDTO;
import com.yolt.accountsandtransactions.transactions.cycles.TransactionCyclesService;
import com.yolt.accountsandtransactions.transactions.enrichments.TransactionEnrichments;
import com.yolt.accountsandtransactions.transactions.enrichments.TransactionEnrichmentsService;
import lombok.*;
import lombok.extern.slf4j.Slf4j;
import nl.ing.lovebird.clienttokens.ClientUserToken;
import com.yolt.accountsandtransactions.datetime.DateInterval;
import nl.ing.lovebird.extendeddata.common.AccountReferenceDTO;
import nl.ing.lovebird.extendeddata.common.CurrencyCode;
import nl.ing.lovebird.extendeddata.transaction.AccountReferenceType;
import nl.ing.lovebird.extendeddata.transaction.ExchangeRateDTO;
import nl.ing.lovebird.extendeddata.transaction.TransactionStatus;
import nl.ing.lovebird.providerdomain.ProviderTransactionDTO;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.time.*;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

import static java.lang.String.format;
import static java.util.Collections.emptyList;
import static java.util.Optional.empty;
import static java.util.Optional.ofNullable;
import static java.util.stream.Collectors.toList;
import static nl.ing.lovebird.providerdomain.ProviderTransactionType.DEBIT;

@RequiredArgsConstructor
@Service
@Slf4j
public class TransactionService {

    private final TransactionRepository transactionRepository;
    private final TransactionEnrichmentsService transactionEnrichmentsService;
    private final TransactionCyclesService transactionCyclesService;
    private final OffloadService offloadService;
    private final Clock clock;
    private final AccountsAndTransactionMetrics metrics;

    /**
     * This delete method is used to delete *all* transactions for the given user's accounts that are present in our keyspace.
     * It will also delete all transaction enrichments to ensure no lingering data is left after the delete.
     * This method will usually be called to remove all user data (e.g. when deleting a usersite).
     */
    public void deleteAllTransactionDataForUserAccounts(UUID userId, List<UUID> accountIds) {
        String pagingState = null;
        do {
            var transactionsPage = transactionRepository.getPageOfTransactionsForAccounts(userId, accountIds, pagingState);
            transactionsPage.getTransactions()
                    .forEach(offloadService::offloadDeleteAsync);
            pagingState = transactionsPage.getNext();
        } while (pagingState != null);

        transactionRepository.deleteAllTransactionsForAccounts(userId, accountIds);
        transactionEnrichmentsService.getTransactionEnrichments(userId, accountIds, null).stream()
                .map(TransactionEnrichments::getEnrichmentCycleId)
                .filter(Objects::nonNull)
                .forEach(cycleId -> transactionCyclesService.deleteCycle(userId, cycleId));
        transactionEnrichmentsService.deleteAllEnrichmentsForAccounts(userId, accountIds);
    }

    /**
     * This method is used to delete a select number of transactions for the user.
     * It will also delete the enrichments related to the transactions that are being deleted.
     * This method will usually be called to clean up data on our side (e.g. remove pending transactions).
     */
    public void deleteSpecificTransactions(final @NonNull List<TransactionPrimaryKey> transactionsToDelete) {
        if (!transactionsToDelete.isEmpty()) {
            transactionsToDelete.stream()
                    .flatMap(key -> transactionRepository.get(key.getUserId(), key.getAccountId(), key.getDate(), key.getId()).stream())
                    .forEach(offloadService::offloadDeleteAsync);

            transactionRepository.deleteSpecificTransactions(transactionsToDelete);
            transactionEnrichmentsService.deleteSpecificEnrichments(transactionsToDelete);
        }
    }

    /**
     * This method is only exposed for testing purposes.
     * <p/>
     * TODO: move this to the test package to prevent usage of this method
     */
    @VisibleForTesting
    public List<Transaction> getTransactionsForUser(UUID userId) {
        return transactionRepository.getTransactionsForUser(userId);
    }

    public List<Transaction> saveTransactionsBatch(UUID accountId, ClientUserToken clientUserToken, AccountFromProviders accountFromProviders, List<ProviderTransactionWithId> upstreamTransactions, InstructionType instructionType) {
        var accountIdentifiable = new AccountIdentifiable(clientUserToken.getUserIdClaim(), accountId, accountFromProviders.getCurrency());

        var transactions = upstreamTransactions.stream()
                .map(providerTx -> map(providerTx, accountIdentifiable, instructionType == InstructionType.INSERT, clock, null))
                .collect(toList());
        transactionRepository.upsert(transactions);

        transactions.forEach(trx -> {
            offloadService.offloadInsertOrUpdateAsync(trx);
            metrics.incrementDebtorOrCreditorNameIsPresent(
                    accountFromProviders.getProvider(),
                    trx.getAmount().compareTo(BigDecimal.ZERO) >= 0,
                    trx.getCreditorName() != null,
                    trx.getDebtorName() != null
            );
        });

        return transactions;
    }

    public Optional<TransactionDTO> getTransaction(UUID userId, UUID accountId, LocalDate date, String transactionId) {
        return transactionRepository.get(userId, accountId, date, transactionId)
                .map(transaction -> map(transaction, transactionEnrichmentsService.getTransactionEnrichments(userId, accountId, date, transactionId).orElse(null)));
    }

    public TransactionsPageDTO getTransactions(UUID userId, List<UUID> accountIds, DateInterval interval, String pagingState, int pageSize) {
        var dateInterval = ofNullable(interval).orElseGet(() -> new DateInterval(LocalDate.now(clock).minusMonths(1), LocalDate.now(clock)));
        var transactionTransactionsPage = transactionRepository.get(userId, accountIds, dateInterval, pagingState, pageSize);
        var transactionEnrichments = getEnrichmentsForTransactions(userId, accountIds, transactionTransactionsPage.getTransactions());

        return new TransactionsPageDTO(
                transactionTransactionsPage.getTransactions().stream()
                        .filter(transaction -> transaction.getFillTypeOrDefault() == Transaction.FillType.REGULAR)
                        .map(transaction -> map(transaction, getEnrichmentsForTransaction(transactionEnrichments, transaction).orElse(null)))
                        .collect(toList()),
                transactionTransactionsPage.getNext()
        );
    }

    private List<TransactionEnrichments> getEnrichmentsForTransactions(UUID userId, List<UUID> accountIds, List<Transaction> transactions) {
        // The `transactions` can be a subset of the all transactions requested (due to paging) so here we only need to get the transaction-enrichments for this specific set of transactions. Therefore
        // we narrow the search for transaction-enrichments that fall within the date-interval of the `transactions`. For this we sort the list of transactions based on the date and get the first and last
        // date and use that in the selection.
        //
        // Note 1: We cannot use the `id` field that is part of the composite key because of Cassandra restrictions. Reason is a range check on the `date` cannot be followed by an `IN` selection on `id`.
        // Note 2: The resulting list of transaction-enrichments can be longer than the list of transactions if there are a lot of transactions within the date-interval which means that it could contain
        //         transaction-enrichments for transactions that are not in `transactions`. However for each entry in `transactions` for which there are transaction-enrichments they will be available in the
        //         returned list.
        if (transactions.isEmpty()) {
            return emptyList();
        } else {
            var sortedDates = transactions.stream()
                    .map(Transaction::getDate)
                    .sorted()
                    .collect(toList());
            var dateInterval = new DateInterval(sortedDates.get(0), sortedDates.get(sortedDates.size() - 1));
            return transactionEnrichmentsService.getTransactionEnrichments(userId, accountIds, dateInterval);
        }
    }

    private Optional<TransactionEnrichments> getEnrichmentsForTransaction(List<TransactionEnrichments> enrichments, Transaction transaction) {
        return enrichments.stream()
                .filter(enrichment -> enrichment.getUserId().equals(transaction.getUserId()))
                .filter(enrichment -> enrichment.getAccountId().equals(transaction.getAccountId()))
                .filter(enrichment -> enrichment.getDate().equals(transaction.getDate()))
                .filter(enrichment -> enrichment.getId().equals(transaction.getId()))
                .findFirst();
    }

    @SuppressWarnings("squid:S3776")
    static TransactionDTO map(Transaction transaction, TransactionEnrichments enrichments) {
        var transactionDTOBuilder = TransactionDTO.builder()
                .id(transaction.getId())
                .externalId(transaction.getExternalId())
                .accountId(transaction.getAccountId())
                .status(transaction.getStatus())
                .date(transaction.getDate())
                .timestamp(ZonedDateTime.ofInstant(transaction.getTimestamp(), ZoneId.of(ofNullable(transaction.getTimeZone()).orElse("-00:00"))))
                .bookingDate(transaction.getBookingDate())
                .valueDate(transaction.getValueDate())
                .amount(transaction.getAmount())
                .currency(transaction.getCurrency())
                .description(transaction.getDescription())
                .endToEndId(transaction.getEndToEndId())
                .bankTransactionCode(transaction.getBankTransactionCode())
                .purposeCode(transaction.getPurposeCode())
                .lastUpdatedTime(transaction.getLastUpdatedTime())
                .bankSpecific(transaction.getBankSpecific())
                .createdAt(transaction.getCreatedAtOrEPOCH());

        if (transaction.getCreditorName() != null ||
                transaction.getCreditorIban() != null ||
                transaction.getCreditorSortCodeAccountNumber() != null ||
                transaction.getCreditorBban() != null ||
                transaction.getCreditorMaskedPan() != null ||
                transaction.getCreditorPan() != null) {
            transactionDTOBuilder.creditor(
                    new TransactionDTO.CreditorDTO(transaction.getCreditorName(),
                            new AccountReferencesDTO(transaction.getCreditorIban(), transaction.getCreditorMaskedPan(), transaction.getCreditorPan(), transaction.getCreditorBban(), transaction.getCreditorSortCodeAccountNumber())
                    )
            );
        }

        if (transaction.getDebtorName() != null ||
                transaction.getDebtorIban() != null ||
                transaction.getDebtorSortCodeAccountNumber() != null ||
                transaction.getDebtorBban() != null ||
                transaction.getDebtorMaskedPan() != null ||
                transaction.getDebtorPan() != null) {
            transactionDTOBuilder.debtor(
                    new TransactionDTO.DebtorDTO(
                            transaction.getDebtorName(),
                            new AccountReferencesDTO(transaction.getDebtorIban(), transaction.getDebtorMaskedPan(), transaction.getDebtorPan(), transaction.getDebtorBban(), transaction.getDebtorSortCodeAccountNumber())
                    )
            );
        }

        if (transaction.getExchangeRateRate() != null ||
                transaction.getExchangeRateCurrencyFrom() != null ||
                transaction.getExchangeRateCurrencyTo() != null) {
            transactionDTOBuilder.exchangeRate(
                    new TransactionDTO.ExchangeRateDTO(
                            transaction.getExchangeRateCurrencyFrom(),
                            transaction.getExchangeRateCurrencyTo(),
                            transaction.getExchangeRateRate()
                    )
            );
        }

        if (transaction.getOriginalAmountAmount() != null) {
            transactionDTOBuilder.originalAmount(
                    new TransactionDTO.OriginalAmountDTO(
                            transaction.getOriginalAmountAmount(),
                            transaction.getOriginalAmountCurrency()
                    )
            );
        }

        ofNullable(enrichments)
                .map(TransactionService::map)
                .ifPresent(transactionDTOBuilder::enrichment);

        transactionDTOBuilder.remittanceInformationStructured(transaction.getRemittanceInformationStructured());
        transactionDTOBuilder.remittanceInformationUnstructured(transaction.getRemittanceInformationUnstructured());

        return transactionDTOBuilder.build();
    }

    private static EnrichmentDTO map(TransactionEnrichments enrichments) {
        return new EnrichmentDTO(
                enrichments.getEnrichmentCategoryPersonal(),
                enrichments.getEnrichmentCategorySME(),
                enrichments.getMerchantName().map(EnrichmentDTO.MerchantDTO::new).orElse(null),
                enrichments.getCounterparty().map(counterparty -> new EnrichmentDTO.CounterpartyDTO(counterparty.name, counterparty.isKnownMerchant)).orElse(null),
                enrichments.getCycle().orElse(null),
                enrichments.getLabelsOrEmptySet()
        );
    }

    @SuppressWarnings("squid:S3776")
    public static Transaction map(
            final @NonNull ProviderTransactionWithId wrappedProviderTx,
            final @NonNull AccountIdentifiable accountIdentifiable,
            final boolean isInsertMode,
            final @NonNull Clock clock,
            final Instant storedCreatedAtTransactionTime) {

        var providerTx = wrappedProviderTx.getProviderTransactionDTO();
        var extendedTx = providerTx.getExtendedTransaction();

        var transactionBuilder = Transaction.builder()
                .userId(accountIdentifiable.userId)
                .accountId(accountIdentifiable.accountId)
                .id(wrappedProviderTx.getTransactionId())
                .timestamp(providerTx.getDateTime().toInstant().truncatedTo(ChronoUnit.MILLIS))
                .timeZone(providerTx.getDateTime().getOffset().toString())
                .lastUpdatedTime(Instant.now(clock).truncatedTo(ChronoUnit.MILLIS))
                .fillType(wrappedProviderTx.getFillType())
                .externalId(providerTx.getExternalId())
                .status(providerTx.getStatus())
                .date(providerTx.getDateTime().toLocalDate())
                .amount(getAmount(providerTx))
                .currency(accountIdentifiable.currencyCode)
                .description(providerTx.getDescription())
                .originalCategory(providerTx.getCategory())
                .originalMerchantName(providerTx.getMerchant())
                .bankSpecific(providerTx.getBankSpecific());

        if (extendedTx != null) {
            transactionBuilder
                    // Berlin Standard defines booking/ value date as ISODate.
                    // This value is truncated and not converted because we don't want to re-interpret the date/time in a particular time-zone
                    .bookingDate(ofNullable(extendedTx.getBookingDate())
                            .map(ZonedDateTime::toLocalDate)
                            .orElse(null))
                    .valueDate(ofNullable(extendedTx.getValueDate())
                            .map(ZonedDateTime::toLocalDate)
                            .orElse(null))
                    .endToEndId(extendedTx.getEndToEndId())
                    .creditorName(extendedTx.getCreditorName())
                    .creditorIban(getAccountReference(extendedTx.getCreditorAccount(), AccountReferenceType.IBAN).orElse(null))
                    .creditorBban(getAccountReference(extendedTx.getCreditorAccount(), AccountReferenceType.BBAN).orElse(null))
                    .creditorMaskedPan(getAccountReference(extendedTx.getCreditorAccount(), AccountReferenceType.MASKED_PAN).orElse(null))
                    .creditorPan(getAccountReference(extendedTx.getCreditorAccount(), AccountReferenceType.PAN).orElse(null))
                    .creditorSortCodeAccountNumber(getAccountReference(extendedTx.getCreditorAccount(), AccountReferenceType.SORTCODEACCOUNTNUMBER).orElse(null))

                    .debtorName(extendedTx.getDebtorName())
                    .debtorIban(getAccountReference(extendedTx.getDebtorAccount(), AccountReferenceType.IBAN).orElse(null))
                    .debtorBban(getAccountReference(extendedTx.getDebtorAccount(), AccountReferenceType.BBAN).orElse(null))
                    .debtorMaskedPan(getAccountReference(extendedTx.getDebtorAccount(), AccountReferenceType.MASKED_PAN).orElse(null))
                    .debtorPan(getAccountReference(extendedTx.getDebtorAccount(), AccountReferenceType.PAN).orElse(null))
                    .debtorSortCodeAccountNumber(getAccountReference(extendedTx.getDebtorAccount(), AccountReferenceType.SORTCODEACCOUNTNUMBER).orElse(null))

                    .bankTransactionCode(extendedTx.getBankTransactionCode())
                    .purposeCode(extendedTx.getPurposeCode());

            if (extendedTx.getExchangeRate() != null && !extendedTx.getExchangeRate().isEmpty()) {
                // We just pick the first. I know this is a bad idea, but in this regard it's: 'orders are orders'.
                var exchangeRateDTO = extendedTx.getExchangeRate().get(0);
                transactionBuilder.exchangeRateRate(getExchangeRate(exchangeRateDTO).orElse(null))
                        .exchangeRateCurrencyFrom(exchangeRateDTO.getCurrencyFrom())
                        .exchangeRateCurrencyTo(exchangeRateDTO.getCurrencyTo());
            }

            if (extendedTx.getOriginalAmount() != null) {
                transactionBuilder
                        .originalAmountAmount(extendedTx.getOriginalAmount().getAmount())
                        .originalAmountCurrency(extendedTx.getOriginalAmount().getCurrency());
            }

            transactionBuilder.remittanceInformationStructured(extendedTx.getRemittanceInformationStructured());
            transactionBuilder.remittanceInformationUnstructured(extendedTx.getRemittanceInformationUnstructured());
        }

        if (isInsertMode) {
            transactionBuilder.createdAt(Instant.now(clock).truncatedTo(ChronoUnit.MILLIS));
        } else {
            transactionBuilder.createdAt(storedCreatedAtTransactionTime);
        }

        return transactionBuilder.build();
    }

    private static BigDecimal getAmount(ProviderTransactionDTO providerTx) {
        return ofNullable(providerTx.getAmount())
                .map(BigDecimal::abs)
                .map(bigDecimal -> DEBIT == providerTx.getType() ? bigDecimal.negate() : bigDecimal)
                .orElseThrow(() -> new IllegalArgumentException(format("Transaction (id: %s) without an amount.", ofNullable(providerTx.getExternalId()).orElse("unknown"))));
    }

    private static Optional<BigDecimal> getExchangeRate(ExchangeRateDTO exchangeRateDTO) {
        try {
            return Optional.of(new BigDecimal(exchangeRateDTO.getRateFrom()));
        } catch (Exception e) {
            log.warn("Unable to parse exchange rate {}. Mapping it to null.", exchangeRateDTO.getRateFrom()); //NOSHERIFF
            return empty();
        }
    }

    private static Optional<String> getAccountReference(AccountReferenceDTO accountReferenceDTO, AccountReferenceType referenceType) {
        return ofNullable(accountReferenceDTO)
                .filter(accountRef -> accountRef.getType() == referenceType)
                .map(AccountReferenceDTO::getValue);
    }

    /**
     * Unique user-account identifier
     */
    @ToString
    @EqualsAndHashCode
    @RequiredArgsConstructor
    public static class AccountIdentifiable {
        @NonNull
        public final UUID userId;

        @NonNull
        public final UUID accountId;

        @NonNull
        public final CurrencyCode currencyCode;
    }


    @Value
    public static class TransactionPrimaryKey {
        @NonNull
        UUID userId;

        @NonNull
        UUID accountId;

        @NonNull
        LocalDate date;

        @NonNull
        String id;

        /**
         * The TransactionStatus is not part of the primary key of a {@link Transaction},
         * but it's part of the primary key of a {@link com.yolt.accountsandtransactions.datascience.DsTransaction}.
         */
        @NonNull
        TransactionStatus status;
    }

}
