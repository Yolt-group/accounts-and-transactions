package com.yolt.accountsandtransactions.insights;

import com.yolt.accountsandtransactions.insights.client.InsightsClient;
import com.yolt.accountsandtransactions.insights.client.dto.CustomerDnaDTO;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

import java.util.Optional;
import java.util.UUID;

@RequiredArgsConstructor
@Service
class RiskInsightsService {

    private final InsightsClient insightsClient;

    Optional<RiskInsightsReportDTO> getRiskInsightsReport(final UUID userId) {
        return insightsClient.getCustomerDna(userId).map(this::map);
    }

    private RiskInsightsReportDTO map(CustomerDnaDTO customerDnaDTO) {
        return new RiskInsightsReportDTO(RiskInsightsReportDTO.CustomerDnaReport.builder()
                .clientId(customerDnaDTO.clientId())
                .clientName(customerDnaDTO.clientName())
                .userId(customerDnaDTO.userId())
                .userCreatedAt(customerDnaDTO.userCreated())
                .accountLastRefreshTime(customerDnaDTO.accountLastRefreshDate())
                .lastTransactionTime(customerDnaDTO.lastTransactionDate())
                .hasCurrentAccount(customerDnaDTO.hasCurrentAccount())
                .hasSavingsAccount(customerDnaDTO.hasSavingsAccount())
                .hasPensionAccount(customerDnaDTO.hasPensionAccount())
                .hasInvestmentAccount(customerDnaDTO.hasInvestmentAccount())
                .hasCreditCardAccount(customerDnaDTO.hasCreditCardAccount())
                .hasCreditCardDiscovery(customerDnaDTO.hasCreditCardDiscovery())
                .hasCreditCardMasterCard(customerDnaDTO.hasCreditCardMasterCard())
                .hasCreditCardTypeBlack(customerDnaDTO.hasCreditCardTypeBlack())
                .hasCreditCardTypeGold(customerDnaDTO.hasCreditCardTypeGold())
                .hasCreditCardTypePlatinum(customerDnaDTO.hasCreditCardTypePlatinum())
                .hasCreditCardTypeSilver(customerDnaDTO.hasCreditCardTypeSilver())
                .hasCreditCardVisa(customerDnaDTO.hasCreditCardVisa())
                .hasCreditCardAmericanExpress(customerDnaDTO.hasCreditCardAmericanExpress())
                .hasSavingsDeposit(customerDnaDTO.hasSavingsDeposit())
                .hasSavingsWithdrawal(customerDnaDTO.hasSavingsWithdrawal())
                .hasMortgage(customerDnaDTO.hasMortgage())
                .hasRent(customerDnaDTO.hasRent())
                .hasCar(customerDnaDTO.hasCar())
                .hasPet(customerDnaDTO.hasPet())
                .hasSubEnergy(customerDnaDTO.hasSubEnergy())
                .hasSubGym(customerDnaDTO.hasSubGym())
                .hasSubInternetMobile(customerDnaDTO.hasSubInternetMobile())
                .hasSubMusic(customerDnaDTO.hasSubMusic())
                .hasSubVideo(customerDnaDTO.hasSubVideo())
                .hasTrxApplePay(customerDnaDTO.hasTrxApplePay())
                .hasTrxAtmWithdrawal(customerDnaDTO.hasTrxAtmWithdrawal())
                .hasTrxGooglePay(customerDnaDTO.hasTrxGooglePay())
                .hasTrxPayPal(customerDnaDTO.hasTrxPayPal())
                .hasTrxSamsungPay(customerDnaDTO.hasTrxSamsungPay())
                .calculationDate(customerDnaDTO.calculationDate())
                .runTime(customerDnaDTO.runDate())
                .build()
        );
    }

}
