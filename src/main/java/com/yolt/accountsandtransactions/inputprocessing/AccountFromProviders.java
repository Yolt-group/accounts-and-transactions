package com.yolt.accountsandtransactions.inputprocessing;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;
import nl.ing.lovebird.extendeddata.account.ExtendedAccountDTO;
import nl.ing.lovebird.extendeddata.common.CurrencyCode;
import nl.ing.lovebird.providerdomain.*;

import java.math.BigDecimal;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

@Data
@EqualsAndHashCode(callSuper = true)
public class AccountFromProviders extends ProviderAccountDTO {

    private final UUID yoltUserId;
    private final UUID yoltUserSiteId;
    private final UUID yoltSiteId;
    private final String provider;

    @JsonCreator
    @Builder(builderMethodName = "accountsFromProvidersBuilder")
    public AccountFromProviders(
            @JsonProperty("yoltUserId") final UUID yoltUserId,
            @JsonProperty("yoltUserSiteId") final UUID yoltUserSiteId,
            @JsonProperty("yoltSiteId") final UUID yoltSiteId,
            @JsonProperty("yoltAccountType") final AccountType yoltAccountType,
            @JsonProperty("lastRefreshed") final ZonedDateTime lastRefreshed,
            @JsonProperty("availableBalance") final BigDecimal availableBalance,
            @JsonProperty("currentBalance") final BigDecimal currentBalance,
            @JsonProperty("currency") final CurrencyCode currency,
            @JsonProperty("accountId") final String accountId,
            @JsonProperty("name") final String name,
            @JsonProperty("bic") final String bic,
            @JsonProperty("closed") final Boolean closed,
            @JsonProperty("provider") final String provider,
            @JsonProperty("creditCardData") final ProviderCreditCardDTO creditCardData,
            @JsonProperty("accountNumber") final ProviderAccountNumberDTO accountNumber,
            @JsonProperty("transactions") final List<ProviderTransactionDTO> transactions,
            @JsonProperty("accountMaskedIdentification") final String accountMaskedIdentification,
            @JsonProperty("directDebits") final List<DirectDebitDTO> directDebits,
            @JsonProperty("standingOrders") final List<StandingOrderDTO> standingOrders,
            @JsonProperty("extendedAccount") final ExtendedAccountDTO extendedAccount,
            @JsonProperty("bankSpecific") final Map<String, String> bankSpecific,
            @JsonProperty("linkedAccount") final String linkedAccount) {
        super(yoltAccountType, lastRefreshed, availableBalance, currentBalance, accountId, accountMaskedIdentification,
                accountNumber, bic, name, currency, closed, creditCardData, transactions, directDebits, standingOrders,
                extendedAccount, bankSpecific, linkedAccount);
        this.yoltSiteId = yoltSiteId;
        this.provider = provider;
        this.yoltUserId = yoltUserId;
        this.yoltUserSiteId = yoltUserSiteId;
    }

    @Override
    public void validate() {
        Objects.requireNonNull(provider);
        Objects.requireNonNull(yoltSiteId);
        Objects.requireNonNull(yoltUserId);
        Objects.requireNonNull(yoltUserSiteId);
        super.validate();
    }
}
