package com.yolt.accountsandtransactions.insights.client.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.Builder;
import lombok.Value;
import lombok.experimental.Accessors;

import javax.validation.constraints.NotNull;
import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.util.UUID;

@JsonIgnoreProperties(ignoreUnknown = true)
@Value
@Builder
@Accessors(fluent = true) // Without this we'd get awkward accessors like `isHasSavingsAccount`
public class CustomerDnaDTO {
    @NotNull UUID userId;
    @NotNull OffsetDateTime userCreated;
    @NotNull UUID clientId;
    @NotNull String clientName;
    @NotNull OffsetDateTime accountLastRefreshDate;
    boolean hasCreditCardAccount;
    boolean hasPensionAccount;
    boolean hasSavingsAccount;
    boolean hasCurrentAccount;
    boolean hasInvestmentAccount;
    boolean hasMortgage;
    boolean hasPet;
    boolean hasRent;
    boolean hasCar;
    boolean hasCreditCardMasterCard;
    boolean hasCreditCardVisa;
    boolean hasCreditCardDiscovery;
    boolean hasCreditCardTypeGold;
    boolean hasCreditCardTypeSilver;
    boolean hasCreditCardTypePlatinum;
    boolean hasCreditCardTypeBlack;
    boolean hasCreditCardAmericanExpress;
    boolean hasSubEnergy;
    boolean hasSubMusic;
    boolean hasSubVideo;
    boolean hasSubInternetMobile;
    boolean hasSubGym;
    boolean hasTrxAtmWithdrawal;
    boolean hasTrxApplePay;
    boolean hasTrxSamsungPay;
    boolean hasTrxGooglePay;
    boolean hasTrxPayPal;
    OffsetDateTime lastTransactionDate;
    boolean hasSavingsDeposit;
    boolean hasSavingsWithdrawal;
    @NotNull LocalDate calculationDate;
    @NotNull OffsetDateTime runDate;
}
