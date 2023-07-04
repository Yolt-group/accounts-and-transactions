package com.yolt.accountsandtransactions.legacyaccounts;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import nl.ing.lovebird.extendeddata.account.ExtendedAccountDTO;

import javax.validation.Valid;
import javax.validation.constraints.Size;
import java.math.BigDecimal;
import java.util.Currency;
import java.util.Date;
import java.util.Map;
import java.util.UUID;

@Value
@AllArgsConstructor
@Builder(toBuilder = true)
@Slf4j
@JsonIgnoreProperties(ignoreUnknown = true)
public class LegacyAccountDTO {

    // ID of the account (required = true)
    UUID id;

    // ID of the account at the external data-provider (required = true)
    @Size(max = 256)
    String externalId;

    // Name of the account (required = true)
    @Size(max = 256)
    String name;

    // Nickname of the account.
    // Always null, there is no related field in a&t table
    @Size(max = 256)
    String nickname;

    // ISO code of the currency of the account (required = true)
    Currency currencyCode;

    /**
     * Either <code>balance</code> OR <code>availableBalance</code> or BOTH
     */
    // Current balance on the account
    BigDecimal balance;

    // Balance on the account after deducting pending transactions
    BigDecimal availableBalance;

    // Timestamp when the account data was refreshed
    Date lastRefreshed;

    // Timestamp of the last time this record was updated. Equal to lastRefreshed because there is no related field in a&t table
    Date updated;

    // Status of data refresh (required = true)
    LegacyAccountStatusCode status;

    // Reference to group of back accounts where this account belongs to (required = true)
    LegacyUserSiteDTO userSite;

    // The type of the account
    LegacyAccountType type;

    // Shows if the account is hidden (required = true)
    boolean hidden;

    // Shows if the account is closed (required = true)
    boolean closed;

    // Shows if the account is migrated (required = true)
    boolean migrated;

    // The details of the account number, if known
    @Valid
    LegacyAccountNumberDTO accountNumber;

    // The details of the custom account number, if specified by the user.
    // Always null, there is no related field in a&t table
    @Valid
    LegacyAccountNumberDTO customAccountNumber;

    // The masked details of the account number, if known
    @Size(max = 256)
    String accountMaskedIdentification;

    // The extended account fields. Only supplied if requested.
    @JsonInclude(JsonInclude.Include.NON_NULL)
    @Valid
    ExtendedAccountDTO extendedAccount;

    // Linked account type, eg. MAIN, MONEY_JAR or GENERIC_WALLET
    // Always null, there is no related field in a&t table
    String yoltAccountType;

    // Optional, this should refer to the external accountId of the account that is linked to
    String linkedAccount;

    // Bank specific properties
    Map<String, String> bankSpecific;

    // Links to related resources - HATEOAS format, (required = true, accessMode = ApiModelProperty.AccessMode.READ_ONLY)
    @JsonProperty("_links")
    @Valid
    LegacyAccountDTO.LinksDTO links;

    @Value
    @AllArgsConstructor
    public static class LinksDTO {

        // Link to hide/unhide multiple accounts, might expose other features on accounts later (required = true)
        LinkDTO hideAccounts;

        // Link to the next page with transactions (required = true)
        LinkDTO transactions;

//  These links were left out because they are app-specific. They are not exposed via client-proxy.
//
//        // Link to update an account, for now supports only nickname, but might expose other features on account later (required = true)
//        LinkDTO updateAccount;
//
//        // Link to the next page with transactions ordered by date/time
//        LinkDTO transactionsByDatetime;
//
//        // Link to the logo of the site
//        LinkDTO logo;
//
//        // Link to the icon of the site
//        LinkDTO icon;
//
//        // Link to the user site resource
//        LinkDTO userSite;
//
//        // Link to the saving goals (required = true)
//        LinkDTO savingsGoals;
    }
}
