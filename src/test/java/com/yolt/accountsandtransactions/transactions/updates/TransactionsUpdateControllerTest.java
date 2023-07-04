package com.yolt.accountsandtransactions.transactions.updates;

import com.yolt.accountsandtransactions.transactions.updates.api.TransactionCounterpartyUpdateRequestDTO;
import nl.ing.lovebird.clienttokens.ClientUserToken;
import org.jose4j.jwt.JwtClaims;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.HttpStatus;

import java.time.LocalDate;
import java.util.Optional;
import java.util.UUID;

import static java.lang.String.format;
import static java.util.UUID.randomUUID;
import static nl.ing.lovebird.clienttokens.constants.ClientTokenConstants.CLAIM_DATA_ENRICHMENT_CATEGORIZATION;
import static nl.ing.lovebird.clienttokens.constants.ClientTokenConstants.CLAIM_DATA_ENRICHMENT_MERCHANT_RECOGNITION;
import static org.apache.tomcat.util.codec.binary.Base64.encodeBase64String;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class TransactionsUpdateControllerTest {
    static final UUID USER_0 = randomUUID();
    static final UUID ACCOUNT_1 = randomUUID();
    static final LocalDate LOCAL_DATE_1 = LocalDate.parse("2016-05-01");
    static final String TRX_ID_1 = "TRX01";
    static final String MERCHANT_A = "Merchant_A";

    @Mock
    private CounterpartyAdjustmentService counterpartyAdjustmentService;

    @Mock
    private RecategorizationService recategorizationService;

    private TransactionsUpdateController transactionsUpdateController;

    @BeforeEach
    public void init() {
        transactionsUpdateController = new TransactionsUpdateController(counterpartyAdjustmentService, recategorizationService);
    }

    @Test
    public void testUpdateMerchant_HappyFlow() throws Exception {
        var payload = new TransactionCounterpartyUpdateRequestDTO(ACCOUNT_1, TRX_ID_1, LOCAL_DATE_1, MERCHANT_A);

        var activityId = randomUUID();
        when(counterpartyAdjustmentService.updateCounterpartyOnTransaction(any(), eq(payload)))
                .thenReturn(Optional.of(new CounterpartyAdjustmentService.CounterpartyFeedbackActivity(activityId, payload.getCounterpartyName(), true)));

        var response = transactionsUpdateController.updateCounterpartyOfSingleTransaction(USER_0, clientUserToken(USER_0), payload);

        assertThat(response).isNotNull();

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.ACCEPTED);
        assertThat(response.getBody().getActivityId()).isEqualTo(activityId);
        assertThat(response.getBody().getCounterpartyName()).isEqualTo(payload.getCounterpartyName());
        assertThat(response.getBody().isKnownMerchant()).isEqualTo(true);
    }


    private static ClientUserToken clientUserToken(UUID clientId) {
        var serialized = encodeBase64String(format("fake-client-token-for-%s", clientId.toString()).getBytes());

        var claims = new JwtClaims();
        claims.setClaim("client-id", clientId.toString());
        claims.setClaim(CLAIM_DATA_ENRICHMENT_MERCHANT_RECOGNITION, true);
        claims.setClaim(CLAIM_DATA_ENRICHMENT_CATEGORIZATION, true);

        return new ClientUserToken(serialized, claims);
    }
}
