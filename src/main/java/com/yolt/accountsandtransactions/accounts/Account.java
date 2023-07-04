package com.yolt.accountsandtransactions.accounts;

import com.datastax.driver.mapping.annotations.*;
import lombok.*;
import lombok.extern.slf4j.Slf4j;
import nl.ing.lovebird.extendeddata.account.BalanceType;
import nl.ing.lovebird.extendeddata.account.UsageType;
import nl.ing.lovebird.extendeddata.common.CurrencyCode;
import nl.ing.lovebird.providerdomain.AccountType;
import org.springframework.lang.Nullable;

import javax.validation.constraints.NotNull;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static com.yolt.accountsandtransactions.Predef.*;


@Slf4j
@AllArgsConstructor
@NoArgsConstructor
@Data
@Builder(toBuilder = true)
@Table(name = "accounts")
public class Account {

    @NotNull
    @PartitionKey
    @Column(name = "user_id")
    private UUID userId;

    @NotNull
    @ClusteringColumn
    @Column(name = "id")
    private UUID id;

    @NotNull
    @Column(name = "user_site_id")
    private UUID userSiteId;

    @NotNull
    @Column(name = "site_id")
    private UUID siteId;

    @NotNull
    @Column(name = "external_id")
    private String externalId;

    @NotNull
    @Column(name = "name")
    private String name;

    @NotNull
    @Column(name = "type")
    private AccountType type;

    @NotNull
    @Column(name = "currency")
    private CurrencyCode currency;

    @NotNull
    @Column(name = "balance")
    private BigDecimal balance;

    @Nullable
    @Column(name = "last_data_fetch_time")
    private Instant lastDataFetchTime;

    @NotNull
    @Column(name = "status")
    private Status status;

    @Nullable
    @Column(name = "product")
    private String product;

    @Nullable
    @Column(name = "account_holder")
    private String accountHolder;

    @Nullable
    @Column(name = "iban")
    private String iban;

    @Nullable
    @Column(name = "masked_pan")
    private String maskedPan;

    @Nullable
    @Column(name = "pan")
    private String pan;

    @Nullable
    @Column(name = "bban")
    private String bban;

    @Nullable
    @Column(name = "sort_code_account_number")
    private String sortCodeAccountNumber;

    @Nullable
    @Column(name = "interest_rate")
    private BigDecimal interestRate;

    @Nullable
    @Column(name = "credit_limit")
    private BigDecimal creditLimit;

    @Nullable
    @Column(name = "available_credit")
    private BigDecimal availableCredit;

    @Nullable
    @Column(name = "linked_account")
    private String linkedAccount;

    @Nullable
    @Column(name = "bic")
    private String bic;

    @Nullable
    @Column(name = "is_money_pot_of")
    private String isMoneyPotOf;

    @Nullable
    @Column(name = "bank_specific")
    private Map<String, String> bankSpecific;

    /**
     * Hidden has no meaning in the YTS domain; This is a left-over from the integration with the yolt-app.
     * Hidden always defaults to false for compatibility with the datascience keyspace.
     */
    @Deprecated
    @Column(name = "hidden")
    private boolean hidden;

    @Nullable
    @Column(name = "usage")
    private UsageType usage;

    @Nullable
    @Column(name = "balances")
    private List<Balance> balances;

    /**
     * Non nullable getter overridden at {@link Account#getCreatedAtOrDefault()}
     */
    @Nullable
    @Column(name = "created_at")
    private Instant createdAt;

    /**
     * Returns the date/time (as UTC instant) when this account was created within the YTS system.
     * <p/>
     * This value defaults to the EPOCH for accounts older then the 25th of March 2021
     */
    @Transient
    public @NonNull Instant getCreatedAtOrDefault() {
        return Optional.ofNullable(createdAt).orElse(Instant.EPOCH);
    }

    public enum Status {
        ENABLED, DELETED, BLOCKED;
    }

    /**
     * Return the {@link AccountNumber} for this {@link Account}. The account number is either IBAN or SORT CODE
     *
     * @return return the account-number
     */
    @Transient
    public @NonNull Optional<AccountNumber> getAccountNumber() {
        if (iban != null) {
            return some(new AccountNumber(maybe(accountHolder), some(AccountNumber.Scheme.IBAN), some(iban)));
        }

        if (sortCodeAccountNumber != null) {
            return some(new AccountNumber(maybe(accountHolder), some(AccountNumber.Scheme.SORTCODEACCOUNTNUMBER), some(sortCodeAccountNumber)));
        }

        return none();
    }

    /**
     * @return the available balance (@{link BalanceType.AVAILABLE} if available
     */
    @Transient
    public BigDecimal getAvailableBalance() {
        if (balances == null) {
            return null;
        }

        return balances.stream()
                .filter(b -> b.getBalanceType().equals(BalanceType.AVAILABLE))
                .map(Balance::getAmount)
                .findFirst()
                .orElse(null);
    }

    public static class AccountBuilder {
        public AccountBuilder accountNumber(final AccountNumber accountNumber) {

            accountNumber.scheme.ifPresent(scheme -> {
                this.accountHolder = accountNumber.holderName.orElse(null);

                // map the scheme to the correct field (either iban or sortCodeAccountNumber)
                switch (scheme) {
                    case IBAN: {
                        this.iban = accountNumber.identification.orElse(null);
                        break;
                    }
                    case SORTCODEACCOUNTNUMBER: {
                        this.sortCodeAccountNumber = accountNumber.identification.orElse(null);
                        break;
                    }
                    default:  // unsupported scheme; iban/sortCodeAccountNumber not set.
                }
            });

            return this;
        }
    }

    @ToString
    @EqualsAndHashCode
    @RequiredArgsConstructor
    public static class AccountNumber {

        @NonNull
        public final Optional<String> holderName;

        @NonNull
        public final Optional<Scheme> scheme;

        @NonNull
        public final Optional<String> identification;

        public enum Scheme {
            IBAN,
            SORTCODEACCOUNTNUMBER
        }
    }
}
