package com.yolt.accountsandtransactions.inputprocessing.matching;

import com.yolt.accountsandtransactions.transactions.Transaction;
import lombok.Builder;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import nl.ing.lovebird.extendeddata.common.AccountReferenceDTO;
import nl.ing.lovebird.extendeddata.transaction.TransactionStatus;
import nl.ing.lovebird.providerdomain.ProviderTransactionDTO;
import nl.ing.lovebird.providerdomain.ProviderTransactionType;
import org.apache.commons.lang3.StringUtils;
import org.springframework.lang.Nullable;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;

public interface GeneralizedTransaction {

    @Nullable
    String getInternalId();

    @Nullable
    String getExternalId();

    @NonNull
    Long getAmountInCents();

    @NonNull
    LocalDate getDate();

    @NonNull
    TransactionStatus getStatus();

    @Nullable
    LocalDate getBookingDate();

    @Nullable
    Instant getTimestamp();

    @Nullable
    String getDebtorName();

    @Nullable
    String getDebtorAccountNr();

    @Nullable
    String getCreditorName();

    @Nullable
    String getCreditorAccountNr();

    @Nullable
    String getDescription();

    @Nullable
    String getEndToEndId();

    /**
     * Truncate an instant to milliseconds
     */
    static Instant truncateToMillis(final @NonNull Instant instant) {
        return instant.truncatedTo(ChronoUnit.MILLIS);
    }

    /**
     * Convert a {@link BigDecimal} amount into cents
     */
    static long toAmountsInCents(final @NonNull BigDecimal amount) {
        return amount.setScale(2, RoundingMode.HALF_UP)
                .multiply(new BigDecimal(100L))
                .toBigIntegerExact()
                .longValueExact();
    }

    static List<StoredGeneralizedTransaction> toStoredGeneralized(final List<Transaction> transactions) {
        return transactions.stream()
                .map(GeneralizedTransaction::toGeneralized)
                .collect(toList());
    }

    static List<ProviderGeneralizedTransaction> toProviderGeneralized(final List<ProviderTransactionDTO> transactions) {
        return transactions.stream()
                .map(GeneralizedTransaction::toGeneralized)
                .collect(toList());
    }

    static ProviderGeneralizedTransaction toGeneralized(final ProviderTransactionDTO transaction) {
        return new ProviderGeneralizedTransaction(transaction);
    }

    static StoredGeneralizedTransaction toGeneralized(final Transaction transaction) {
        return new StoredGeneralizedTransaction(transaction);
    }

    @Builder(toBuilder = true)
    @RequiredArgsConstructor
    class ProviderGeneralizedTransaction implements GeneralizedTransaction {

        @NonNull
        public final ProviderTransactionDTO provider;

        @Override
        public @Nullable
        String getInternalId() {
            return null;
        }

        @Override
        public @Nullable
        String getExternalId() {
            return provider.getExternalId();
        }

        @Override
        public @NonNull Long getAmountInCents() {
            var amountAbs = provider.getAmount().abs();
            var amount = provider.getType() == ProviderTransactionType.DEBIT
                    ? amountAbs.negate()
                    : amountAbs;

            return toAmountsInCents(amount);
        }

        @Override
        public @NonNull LocalDate getDate() {
            return provider.getDateTime().toLocalDate();
        }

        @Override
        public @NonNull TransactionStatus getStatus() {
            return provider.getStatus();
        }

        @Override
        public @Nullable
        LocalDate getBookingDate() {
            return Optional.ofNullable(provider.getExtendedTransaction().getBookingDate())
                    .map(ZonedDateTime::toLocalDate)
                    .orElse(null);
        }

        @Override
        public @Nullable
        Instant getTimestamp() {
            return Optional.ofNullable(provider.getDateTime())
                    .map(ZonedDateTime::toInstant)
                    .map(GeneralizedTransaction::truncateToMillis) // Instant precision changed in recent JDK versions
                    .orElse(null);
        }

        @Override
        public @Nullable
        String getDebtorName() {
            return provider.getExtendedTransaction().getDebtorName();
        }

        @Nullable
        @Override
        public String getDebtorAccountNr() {
            return Optional.ofNullable(provider.getExtendedTransaction().getDebtorAccount())
                    .map(AccountReferenceDTO::getValue)
                    .orElse(null);
        }

        @Override
        public @Nullable
        String getCreditorName() {
            return provider.getExtendedTransaction().getCreditorName();
        }

        @Nullable
        @Override
        public String getCreditorAccountNr() {
            return Optional.ofNullable(provider.getExtendedTransaction().getCreditorAccount())
                    .map(AccountReferenceDTO::getValue)
                    .orElse(null);
        }

        @Override
        public @Nullable
        String getDescription() {
            return provider.getDescription();
        }

        @Nullable
        @Override
        public String getEndToEndId() {
            return provider.getExtendedTransaction().getEndToEndId();
        }

        @Override
        public String toString() {
            throw new IllegalAccessError("toString is disabled to prevent data leakage.");
        }
    }

    @Builder(toBuilder = true)
    @RequiredArgsConstructor
    class StoredGeneralizedTransaction implements GeneralizedTransaction {

        @NonNull
        public final Transaction stored;

        @Override
        public @Nullable
        String getInternalId() {
            return stored.getId();
        }

        @Override
        public @Nullable
        String getExternalId() {
            return stored.getExternalId();
        }

        @Override
        public @NonNull Long getAmountInCents() {
            return toAmountsInCents(stored.getAmount());
        }

        @Override
        public @NonNull LocalDate getDate() {
            return stored.getDate();
        }

        @Override
        public @NonNull TransactionStatus getStatus() {
            return stored.getStatus();
        }

        @Override
        public @Nullable
        LocalDate getBookingDate() {
            return stored.getBookingDate();
        }

        @Override
        public @Nullable
        Instant getTimestamp() {
            return Optional.ofNullable(stored.getTimestamp())
                    .map(GeneralizedTransaction::truncateToMillis) // Instant precision changed in recent JDK versions
                    .orElse(null);
        }

        @Override
        public @Nullable
        String getDebtorName() {
            return stored.getDebtorName();
        }

        @Nullable
        @Override
        public String getDebtorAccountNr() {
            return firstNonBlank(
                    stored.getDebtorBban(),
                    stored.getDebtorIban(),
                    stored.getDebtorPan(),
                    stored.getDebtorMaskedPan(),
                    stored.getDebtorSortCodeAccountNumber()).orElse(null);
        }

        @Override
        public @Nullable
        String getCreditorName() {
            return stored.getCreditorName();
        }

        @Nullable
        @Override
        public String getCreditorAccountNr() {
            return firstNonBlank(
                    stored.getCreditorBban(),
                    stored.getCreditorIban(),
                    stored.getCreditorPan(),
                    stored.getCreditorMaskedPan(),
                    stored.getCreditorSortCodeAccountNumber()).orElse(null);
        }

        @Override
        public @Nullable
        String getDescription() {
            return stored.getDescription();
        }

        @Nullable
        @Override
        public String getEndToEndId() {
            return stored.getEndToEndId();
        }

        @Override
        public String toString() {
            throw new IllegalAccessError("toString is disabled to prevent data leakage.");
        }
    }

    private static Optional<String> firstNonBlank(String... inputs) {
        return Stream.of(inputs)
                .filter(StringUtils::isNotBlank)
                .findFirst();
    }
}
