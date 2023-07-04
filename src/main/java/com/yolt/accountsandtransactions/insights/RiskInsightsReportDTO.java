package com.yolt.accountsandtransactions.insights;

import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Builder;
import lombok.Value;

import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.util.UUID;

@Schema(description = "A risk insights report for a given user")
record RiskInsightsReportDTO(CustomerDnaReport customerDnaReport) {

    @Builder
    @Value
    @Schema(required = true, description = "A customer DNA report")
    static class CustomerDnaReport {
        @Schema(required = true, description = "The id of the user")
        UUID userId;
        @Schema(required = true, description = "The date and time when the user was created")
        OffsetDateTime userCreatedAt;
        @Schema(required = true, description = "The id of the client to which the user belongs")
        UUID clientId;
        @Schema(required = true, description = "The name of the client to which the user belongs")
        String clientName;
        @Schema(required = true, description = "The date and time of the most recent account refresh. Note: this does not mean that all accounts were refreshed on the given date and time; some account refreshes might have failed.")
        OffsetDateTime accountLastRefreshTime;
        @Schema(required = false, description = "The date and time of the latest transaction we have on file for the user. It can be null if user doesn't have any transaction for the last 1 year.")
        OffsetDateTime lastTransactionTime;
        @Schema(required = true, description = "If the user has a current account connected")
        boolean hasCurrentAccount;
        @Schema(required = true, description = "If the user has a savings account connected")
        boolean hasSavingsAccount;
        @Schema(required = true, description = "If the user has a credit card account connected")
        boolean hasCreditCardAccount;
        @Schema(required = true, description = "If the user has a pension account connected")
        boolean hasPensionAccount;
        @Schema(required = true, description = "If the user has an investment account connected")
        boolean hasInvestmentAccount;
        @Schema(required = true, description = "If the user has done at least two mortgage-related payments in the past 12 months, of which at least one was in the last 3 months")
        boolean hasMortgage;
        @Schema(required = true, description = "If the user has done at least four pet-related payments in the past 12 months, of which at least one was in the last 3 months")
        boolean hasPet;
        @Schema(required = true, description = "If the user has done at least four rent-related payments in the past 12 months, of which at least one was in the last 3 months. Note: this also includes rent of equipment, for example.")
        boolean hasRent;
        @Schema(required = true, description = "If the user has done at least four car-related transactions payments in the past 12 months, of which at least one was in the last 3 months.")
        boolean hasCar;
        @Schema(required = true, description = "If the user has a Mastercard credit card connected")
        boolean hasCreditCardMasterCard;
        @Schema(required = true, description = "If the user has a Visa credit card connected")
        boolean hasCreditCardVisa;
        @Schema(required = true, description = "If the user has a Discovery credit card connected")
        boolean hasCreditCardDiscovery;
        @Schema(required = true, description = "If the user has a Gold credit card connected")
        boolean hasCreditCardTypeGold;
        @Schema(required = true, description = "If the user has a Silver credit card connected")
        boolean hasCreditCardTypeSilver;
        @Schema(required = true, description = "If the user has a Platinum credit card connected")
        boolean hasCreditCardTypePlatinum;
        @Schema(required = true, description = "If the user has a Black credit card connected")
        boolean hasCreditCardTypeBlack;
        @Schema(required = true, description = "If the user has a American Express credit card connected")
        boolean hasCreditCardAmericanExpress;
        @Schema(required = true, description = "If the user has an energy subscription")
        boolean hasSubEnergy;
        @Schema(required = true, description = "If the user has a music subscription")
        boolean hasSubMusic;
        @Schema(required = true, description = "If the user has a video subscription")
        boolean hasSubVideo;
        @Schema(required = true, description = "If the user has an Internet and/or mobile subscription")
        boolean hasSubInternetMobile;
        @Schema(required = true, description = "If the user has a gym subscription")
        boolean hasSubGym;
        @Schema(required = true, description = "If the user has done ATM withdrawals in the past 12 months")
        boolean hasTrxAtmWithdrawal;
        @Schema(required = true, description = "If the user has done Apple Pay payments in the past 12 months")
        boolean hasTrxApplePay;
        @Schema(required = true, description = "If the user has done Samsung Pay payments in the past 12 months")
        boolean hasTrxSamsungPay;
        @Schema(required = true, description = "If the user has done Google Pay payments in the past 12 months")
        boolean hasTrxGooglePay;
        @Schema(required = true, description = "If the user has done PayPal payments in the past 12 months")
        boolean hasTrxPayPal;
        @Schema(required = true, description = "If the user has done at least one deposit to a savings account in the past 12 months")
        boolean hasSavingsDeposit;
        @Schema(required = true, description = "If the user has done at least one withdrawal from a savings account in the past 12 months")
        boolean hasSavingsWithdrawal;
        @Schema(required = true, description = "The date of source information on which this DNA report is based")
        LocalDate calculationDate;
        @Schema(required = true, description = "The date and time when this DNA report was generated")
        OffsetDateTime runTime;
    }
}
