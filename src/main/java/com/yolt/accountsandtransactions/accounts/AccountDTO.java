package com.yolt.accountsandtransactions.accounts;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonInclude;
import io.swagger.v3.oas.annotations.media.ArraySchema;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Builder;
import lombok.NonNull;
import lombok.Value;
import nl.ing.lovebird.extendeddata.account.BalanceType;
import nl.ing.lovebird.extendeddata.account.UsageType;
import nl.ing.lovebird.extendeddata.common.CurrencyCode;
import nl.ing.lovebird.providerdomain.AccountType;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;

@Value
@JsonInclude(JsonInclude.Include.NON_EMPTY)
@Builder
@Schema
public class AccountDTO {

    @Schema(required = true, description = "The identifier of the account.", example = "dacba0a6-2305-4359-b942-fea028602a7b")
    UUID id;

    @Schema(required = true, description = "The identifier of the account at the bank.", example = "14H8IY710471984729847")
    String externalId;

    @Schema(required = true, description = "The account type.", example = "CURRENT_ACCOUNT")
    AccountType type;

    @Schema(required = true)
    UserSiteDTO userSite;

    @Schema(required = true, description = "Currency of the balance.", example = "EUR")
    CurrencyCode currency;

    @Schema(required = true, description = "The amount.", example = "240.45")
    BigDecimal balance;

    @Schema(required = true, description = "Status of the account. Defaults to ENABLED.", example = "ENABLED")
    Account.Status status;

    @Schema(required = true, description = "The name of the account.", example = "John's Account")
    String name;

    @Schema(description = "The product type.", example = "Gold account.")
    String product;

    @Schema(description = "Name of the account holder.", example = "John Doe")
    String accountHolder;

    AccountReferencesDTO accountReferences;

    @Schema(description = "Bank specific fields. These are special bank specific fields that will be propagated on request only.")
    Map<String, String> bankSpecific;

    CreditCardAccountDTO creditCardAccount;

    CurrentAccountDTO currentAccount;

    SavingsAccountDTO savingsAccount;

    @Schema(description = "Usage type of the account", example = "PRIV")
    UsageType usage;

    @Schema(description = "If this account is linked to another one (e.g. is a wallet), " +
            "this value holds the external identifier the account that is linked to.")
    String linkedAccount;

    @Schema(description = "The last time account was successfully refreshed.")
    Instant lastDataFetchTime;

    @ArraySchema(arraySchema = @Schema(description = "A list of all balance details received from the bank in the last refresh."))
    @JsonInclude // Defaults to JsonInclude.Include.ALWAYS
    List<BalanceDTO> balances;

    @NonNull
    @JsonFormat(pattern = "yyyy-MM-dd'T'HH:mm:ssZ", timezone = "UTC")
    @Schema(required = true, description = "The date/time (as UTC instant) when this account was created within the YTS system. This value defaults to the EPOCH for accounts older then 25th of March 2021.", example = "2021-03-26T09:30:00.000Z")
    Instant createdAt;

    @Value
    @Schema(description = "The user-site to which this account is linked.")
    public static class UserSiteDTO {
        @Schema(required = true, description = "The identifier of the user site.", example = "0e62b40a-125a-49d4-a572-4925d51bc4f7")
        UUID userSiteId;
        @Schema(required = true, description = "The identifier of the site.", example = "be44b325-de6d-4993-81a9-2c67e6230253")
        UUID siteId;
    }

    @Value
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    @Schema(description = "Contains information specific to current accounts. May be present on accounts of type CURRENT_ACCOUNT.")
    static class CurrentAccountDTO {
        @Schema(description = "The BIC (Bank Identifier Code).", example = "RABONL2U")
        String bic;
        @Schema(description = "Credit limit", example = "1000")
        BigDecimal creditLimit;
    }

    @Value
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    @Schema(description = "Contains information specific to credit card accounts. May be present on accounts of type CREDIT_CARD.")
    static class CreditCardAccountDTO {
        @Schema(description = "Credit limit. This is the total preset borrowing limit that can be used at any time by the borrower. ", example = "1000")
        BigDecimal creditLimit;
        @Schema(description = "Available credit. This is the amount that the account holder can still withdraw/borrow.", example = "200")
        BigDecimal availableCredit;
        @Schema(description = "The account to which the card is linked. For credit cards this will always be a CURRENT_ACCOUNT. " +
                "Refers to the externalId of the current account", example = "14H8IY710471984729847")
        String linkedAccount;
    }

    @Value
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    @Schema(description = "Contains information specific to savings accounts.")
    public static class SavingsAccountDTO {
        @Schema(description = "The BIC (Bank Identifier Code).", example = "RABONL2U")
        String bic;
        @Schema(description = "Some banks provide 'savings goals' functionality. These 'pots' will be modeled as separate savings accounts." +
                "This savings account is a 'child' of another account. The other main account is referred to by this property. Please note that these are external references " +
                "provided by the bank, so it points to another account.externalId", example = "123455")
        String isMoneyPotOf;
    }

    @Value
    @Builder
    @JsonInclude(JsonInclude.Include.NON_NULL)
    @Schema(name = "Balance", description = "Contains information of one of the various types of balances available for the account.")
    public static class BalanceDTO {
        @Schema(description = "The currency of the balance.")
        String currency;
        @Schema(description = "The amount of the balance.")
        BigDecimal amount;
        @Schema(description = "The balance types are formatted according to the ISO 20022 standard.")
        BalanceType type;
    }
}
