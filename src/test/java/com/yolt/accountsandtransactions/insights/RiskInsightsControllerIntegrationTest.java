package com.yolt.accountsandtransactions.insights;

import com.yolt.accountsandtransactions.datetime.exception.RiskInsightsClaimMissingException;
import nl.ing.lovebird.clienttokens.ClientUserToken;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

import java.util.Optional;

import static java.util.UUID.randomUUID;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class RiskInsightsControllerTest {

    @Mock
    private RiskInsightsService riskInsightsService;

    @Mock
    private ClientUserToken clientUserToken;

    @InjectMocks
    private RiskInsightsController riskInsightsController;

    @Test
    void getRiskInsights_withoutRiskInsightsClaim_throwsRiskInsightsClaimMissingException() {
        when(clientUserToken.hasRiskInsights()).thenReturn(false);

        assertThrows(RiskInsightsClaimMissingException.class, () -> riskInsightsController.getRiskInsights(clientUserToken, randomUUID()));
    }

    @Test
    void getRiskInsights_withMismatchBetweenUserIdInPathAndClientUserToken_returnsForbidden() {
        when(clientUserToken.hasRiskInsights()).thenReturn(true);
        when(clientUserToken.getUserIdClaim()).thenReturn(randomUUID());

        ResponseEntity<RiskInsightsReportDTO> response = riskInsightsController.getRiskInsights(clientUserToken, randomUUID());
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.FORBIDDEN);
    }

    @Test
    void getRiskInsights_noReportAvailable_returns204NoContent() {
        var userId = randomUUID();

        when(clientUserToken.hasRiskInsights()).thenReturn(true);
        when(clientUserToken.getUserIdClaim()).thenReturn(userId);
        when(riskInsightsService.getRiskInsightsReport(userId)).thenReturn(Optional.empty());

        ResponseEntity<RiskInsightsReportDTO> response = riskInsightsController.getRiskInsights(clientUserToken, userId);
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.NO_CONTENT);
    }

    @Test
    void getRiskInsights_happyFlow_returnsReportReturnedByService() {
        var userId = randomUUID();
        var report = new RiskInsightsReportDTO(RiskInsightsReportDTO.CustomerDnaReport.builder()
                .userId(userId)
                .clientId(randomUUID())
                .build());

        when(clientUserToken.hasRiskInsights()).thenReturn(true);
        when(clientUserToken.getUserIdClaim()).thenReturn(userId);
        when(riskInsightsService.getRiskInsightsReport(userId)).thenReturn(Optional.of(report));

        ResponseEntity<RiskInsightsReportDTO> response = riskInsightsController.getRiskInsights(clientUserToken, userId);
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(response.getBody()).isEqualTo(report);

    }
}
