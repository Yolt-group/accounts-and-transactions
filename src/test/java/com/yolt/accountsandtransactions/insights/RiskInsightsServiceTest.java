package com.yolt.accountsandtransactions.insights;

import com.yolt.accountsandtransactions.insights.client.InsightsClient;
import com.yolt.accountsandtransactions.insights.client.dto.CustomerDnaDTO;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.util.Optional;

import static java.util.UUID.randomUUID;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class RiskInsightsServiceTest {

    @Mock
    private InsightsClient insightsClient;

    @InjectMocks
    private RiskInsightsService riskInsightsService;

    @Test
    void getRiskInsightsReport_happyFlow_callsInsightsApiAndMapsResponse() {
        var customerDna = CustomerDnaDTO.builder()
                .clientId(randomUUID())
                .clientName("Some name")
                .userId(randomUUID())
                .userCreated(OffsetDateTime.now().minusWeeks(1))
                .accountLastRefreshDate(OffsetDateTime.now().minusHours(5))
                .lastTransactionDate(OffsetDateTime.now().minusDays(1))
                .hasCurrentAccount(true)
                .hasSavingsAccount(true)
                .hasPensionAccount(true)
                .hasInvestmentAccount(false)
                .hasSavingsWithdrawal(true)
                .hasSavingsDeposit(true)
                .hasTrxSamsungPay(false)
                .hasTrxPayPal(true)
                .hasTrxGooglePay(true)
                .hasTrxAtmWithdrawal(false)
                .hasTrxApplePay(true)
                .hasSubVideo(false)
                .hasSubMusic(true)
                .hasSubInternetMobile(true)
                .hasSubGym(false)
                .hasSubEnergy(true)
                .hasMortgage(true)
                .hasRent(false)
                .hasCar(false)
                .hasPet(true)
                .hasCreditCardVisa(true)
                .hasCreditCardTypeSilver(false)
                .hasCreditCardTypePlatinum(false)
                .hasCreditCardTypeGold(true)
                .hasCreditCardTypeBlack(false)
                .hasCreditCardMasterCard(true)
                .hasCreditCardDiscovery(false)
                .hasCreditCardAccount(true)
                .hasCreditCardAmericanExpress(false)
                .runDate(OffsetDateTime.now())
                .calculationDate(LocalDate.now().minusDays(1))
                .build();

        when(insightsClient.getCustomerDna(customerDna.userId())).thenReturn(Optional.of(customerDna));

        Optional<RiskInsightsReportDTO> optionalRiskInsightsReport = riskInsightsService.getRiskInsightsReport(customerDna.userId());
        assertThat(optionalRiskInsightsReport).isPresent();

        RiskInsightsReportDTO riskInsightsReport = optionalRiskInsightsReport.get();
        assertThat(riskInsightsReport.customerDnaReport()).isEqualTo(RiskInsightsReportDTO.CustomerDnaReport.builder()
                .clientId(customerDna.clientId())
                .clientName(customerDna.clientName())
                .userId(customerDna.userId())
                .userCreatedAt(customerDna.userCreated())
                .accountLastRefreshTime(customerDna.accountLastRefreshDate())
                .lastTransactionTime(customerDna.lastTransactionDate())
                .hasCurrentAccount(customerDna.hasCurrentAccount())
                .hasSavingsAccount(customerDna.hasSavingsAccount())
                .hasPensionAccount(customerDna.hasPensionAccount())
                .hasInvestmentAccount(customerDna.hasInvestmentAccount())
                .hasSavingsWithdrawal(customerDna.hasSavingsWithdrawal())
                .hasSavingsDeposit(customerDna.hasSavingsDeposit())
                .hasTrxSamsungPay(customerDna.hasTrxSamsungPay())
                .hasTrxPayPal(customerDna.hasTrxPayPal())
                .hasTrxGooglePay(customerDna.hasTrxGooglePay())
                .hasTrxAtmWithdrawal(customerDna.hasTrxAtmWithdrawal())
                .hasTrxApplePay(customerDna.hasTrxApplePay())
                .hasSubVideo(customerDna.hasSubVideo())
                .hasSubMusic(customerDna.hasSubMusic())
                .hasSubInternetMobile(customerDna.hasSubInternetMobile())
                .hasSubGym(customerDna.hasSubGym())
                .hasSubEnergy(customerDna.hasSubEnergy())
                .hasMortgage(customerDna.hasMortgage())
                .hasRent(customerDna.hasRent())
                .hasCar(customerDna.hasCar())
                .hasPet(customerDna.hasPet())
                .hasCreditCardVisa(customerDna.hasCreditCardVisa())
                .hasCreditCardTypeSilver(customerDna.hasCreditCardTypeSilver())
                .hasCreditCardTypePlatinum(customerDna.hasCreditCardTypePlatinum())
                .hasCreditCardTypeGold(customerDna.hasCreditCardTypeGold())
                .hasCreditCardTypeBlack(customerDna.hasCreditCardTypeBlack())
                .hasCreditCardMasterCard(customerDna.hasCreditCardMasterCard())
                .hasCreditCardDiscovery(customerDna.hasCreditCardDiscovery())
                .hasCreditCardAccount(customerDna.hasCreditCardAccount())
                .hasCreditCardAmericanExpress(customerDna.hasCreditCardAmericanExpress())
                .runTime(customerDna.runDate())
                .calculationDate(customerDna.calculationDate())
                .build()
        );
    }

}
