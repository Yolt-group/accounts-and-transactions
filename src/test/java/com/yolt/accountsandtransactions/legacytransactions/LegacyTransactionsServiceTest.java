package com.yolt.accountsandtransactions.legacytransactions;

import com.yolt.accountsandtransactions.MutableClock;
import com.yolt.accountsandtransactions.transactions.TransactionDTO;
import com.yolt.accountsandtransactions.transactions.TransactionService;
import com.yolt.accountsandtransactions.transactions.TransactionsPageDTO;
import nl.ing.lovebird.clienttokens.ClientUserToken;
import com.yolt.accountsandtransactions.datetime.DateInterval;
import nl.ing.lovebird.extendeddata.common.CurrencyCode;
import nl.ing.lovebird.extendeddata.transaction.TransactionStatus;
import org.jose4j.jwt.JwtClaims;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Currency;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class LegacyTransactionsServiceTest {
    private static final UUID USER_ID = UUID.randomUUID();
    private static final UUID ACCOUNT_ID = UUID.randomUUID();
    private static final UUID CYCLE_ID = UUID.randomUUID();
    private static final Set<String> LABELS = Set.of("Label1");
    private static final MutableClock CLOCK = new MutableClock();

    @Mock
    TransactionService transactionService;

    ClientUserToken clientUserToken;
    @BeforeEach
    public void beforeEach() {
        JwtClaims jwtClaims = new JwtClaims();
        jwtClaims.setClaim("user-id", USER_ID.toString());
        clientUserToken = new ClientUserToken("", jwtClaims);
    }

    @Test
    void verifyDefaultDateTimeInterval() {
        CLOCK.asFixed(LocalDateTime.of(2021, 10, 3, 12, 0, 0, 0));
        var expectedDateInterval = new DateInterval(LocalDate.of(2011, 10, 3), LocalDate.of(2021, 10, 3));
        when(transactionService.getTransactions(USER_ID, List.of(ACCOUNT_ID), expectedDateInterval, null, 100)).thenReturn(new TransactionsPageDTO(List.of(), null));

        var legacyTransactionsService = new LegacyTransactionService(transactionService, CLOCK);

        legacyTransactionsService.getTransactions(clientUserToken, ACCOUNT_ID, null, null);

        verify(transactionService).getTransactions(USER_ID, List.of(ACCOUNT_ID), expectedDateInterval, null, 100);
    }

    @Test
    void verifyPropagatesSuppliedDateTimeInterval() {
        var dateInterval = new DateInterval(LocalDate.of(2021, 11, 3), LocalDate.of(2022, 9, 3));
        when(transactionService.getTransactions(USER_ID, List.of(ACCOUNT_ID), dateInterval, null, 100)).thenReturn(new TransactionsPageDTO(List.of(), null));

        var legacyTransactionsService = new LegacyTransactionService(transactionService, CLOCK);

        legacyTransactionsService.getTransactions(clientUserToken, ACCOUNT_ID, dateInterval, null);

        verify(transactionService).getTransactions(USER_ID, List.of(ACCOUNT_ID), dateInterval, null, 100);
    }

    @Test
    void verifyTypicalMapping() {
        var transaction = defaultTransactionBuilder().build();

        var actual = LegacyTransactionService.map(transaction);

        assertThat(actual).isNotNull();
        assertThat(actual.getAccountId()).isEqualTo(ACCOUNT_ID);
        assertThat(actual.getId()).isEqualTo("12345z");
        assertThat(actual.getDate()).isEqualTo(LocalDate.of(2017, 10, 3));
        assertThat(actual.getDateTime()).isEqualTo(ZonedDateTime.of(2017, 10, 3, 0, 0, 0, 0, ZoneId.of("UTC")));
        assertThat(actual.getAmount()).isEqualTo(BigDecimal.TEN);
        assertThat(actual.getCurrency()).isEqualTo(Currency.getInstance("EUR"));
        assertThat(actual.getDescription()).isEqualTo("DIRECT DEBIT Energies");
        assertThat(actual.getShortDescription()).isEqualTo("MerchantY");
        assertThat(actual.getCategory()).isEqualTo("Coffee");
        assertThat(actual.getMerchantObject()).isNotNull();
        assertThat(actual.getMerchantObject().getName()).isEqualTo("MerchantY");
        assertThat(actual.getMerchantObject().getLinks()).isNotNull();
        assertThat(actual.getMerchantObject().getLinks().getIcon().getHref()).isEqualTo("/content/images/merchants/icons/nl/MerchantY.png");
        assertThat(actual.getLabels()).isEqualTo(LABELS);
        assertThat(actual.getCycleId()).isEqualTo(CYCLE_ID);
        assertThat(actual.getExtendedTransaction().getStatus()).isEqualTo(TransactionStatus.BOOKED);
        assertThat(actual.getExtendedTransaction().getTransactionAmount().getAmount()).isEqualTo(BigDecimal.TEN);
        assertThat(actual.getExtendedTransaction().getTransactionAmount().getCurrency()).isEqualTo(CurrencyCode.EUR);
        assertThat(actual.getExtendedTransaction().getEndToEndId()).isEqualTo("12345z-EndToEndId");
        assertThat(actual.getExtendedTransaction().getRemittanceInformationStructured()).isEqualTo("DIRECT DEBIT Energies - structured");
        assertThat(actual.getExtendedTransaction().getRemittanceInformationUnstructured()).isEqualTo("DIRECT DEBIT Energies - unstructured");
        assertThat(actual.getExtendedTransaction().getPurposeCode()).isEqualTo("PURPOSE-999");
        assertThat(actual.getExtendedTransaction().getBankTransactionCode()).isEqualTo("12345z-BankTransactionCode");
    }

    @Test
    void verifyMappingWithUnknownMerchant() {
        var counterpartyDTO = new TransactionDTO.EnrichmentDTO.CounterpartyDTO("Joe", false);
        var enrichmentDTO = new TransactionDTO.EnrichmentDTO("Leisure", "LeisureSME", null, counterpartyDTO, null, Set.of());
        var transaction = defaultTransactionBuilder().enrichment(enrichmentDTO).build();

        var actual = LegacyTransactionService.map(transaction);

        assertThat(actual).isNotNull();
        assertThat(actual.getDescription()).isEqualTo("DIRECT DEBIT Energies");
        assertThat(actual.getShortDescription()).isEqualTo("Joe");
        assertThat(actual.getCategory()).isEqualTo("Leisure");
        assertThat(actual.getMerchantObject()).isNull();
        assertThat(actual.getLabels()).isEmpty();
        assertThat(actual.getCycleId()).isNull();
    }

    @Test
    void verifyMappingWithNullEnrichment() {
        var transaction = defaultTransactionBuilder().enrichment(null).build();

        var actual = LegacyTransactionService.map(transaction);

        assertThat(actual).isNotNull();
        assertThat(actual.getDescription()).isEqualTo("DIRECT DEBIT Energies");
        assertThat(actual.getShortDescription()).isEqualTo("DIRECT DEBIT Energies");
        assertThat(actual.getCategory()).isEqualTo("General");
        assertThat(actual.getMerchantObject()).isNull();
        assertThat(actual.getLabels()).isNull();
        assertThat(actual.getCycleId()).isNull();
    }

    @Test
    void verifyMappingPendingTransactionStatus() {
        var transaction = defaultTransactionBuilder().status(TransactionStatus.PENDING).build();

        var actual = LegacyTransactionService.map(transaction);

        assertThat(actual).isNotNull();
        assertThat(actual.getTransactionType()).isEqualTo(LegacyTransactionType.PENDING);
    }

    @Test
    void verifyMappingHoldTransactionStatus() {
        var transaction = defaultTransactionBuilder().status(TransactionStatus.HOLD).build();

        var actual = LegacyTransactionService.map(transaction);

        assertThat(actual).isNotNull();
        assertThat(actual.getTransactionType()).isEqualTo(LegacyTransactionType.PENDING);
    }

    private static TransactionDTO.TransactionDTOBuilder defaultTransactionBuilder() {
        var dateTime = ZonedDateTime.of(2017, 10, 3, 0, 0, 0, 0, ZoneId.of("UTC"));

        return TransactionDTO.builder()
                .id("12345z")
                .accountId(ACCOUNT_ID)
                .status(TransactionStatus.BOOKED)
                .date(dateTime.toLocalDate())
                .timestamp(dateTime)
                .createdAt(dateTime.toInstant())
                .currency(CurrencyCode.EUR)
                .amount(BigDecimal.TEN)
                .description("DIRECT DEBIT Energies")
                .enrichment(getDefaultTransactionEnrichment())
                .endToEndId("12345z-EndToEndId")
                .remittanceInformationStructured("DIRECT DEBIT Energies - structured")
                .remittanceInformationUnstructured("DIRECT DEBIT Energies - unstructured")
                .purposeCode("PURPOSE-999")
                .bankTransactionCode("12345z-BankTransactionCode");
    }

    public static TransactionDTO.EnrichmentDTO getDefaultTransactionEnrichment() {
        var counterpartyDTO = new TransactionDTO.EnrichmentDTO.CounterpartyDTO("MerchantY", true);

        return new TransactionDTO.EnrichmentDTO("Coffee", "CoffeeSME", null, counterpartyDTO, CYCLE_ID, LABELS);
    }
}
