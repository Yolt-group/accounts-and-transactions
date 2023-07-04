package com.yolt.accountsandtransactions.datascience;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.yolt.accountsandtransactions.inputprocessing.ProviderTransactionWithId;
import nl.ing.lovebird.extendeddata.common.AccountReferenceDTO;
import nl.ing.lovebird.extendeddata.common.CurrencyCode;
import nl.ing.lovebird.extendeddata.transaction.AccountReferenceType;
import nl.ing.lovebird.extendeddata.transaction.ExtendedTransactionDTO;
import nl.ing.lovebird.extendeddata.transaction.TransactionStatus;
import nl.ing.lovebird.providerdomain.ProviderTransactionDTO;
import nl.ing.lovebird.providerdomain.ProviderTransactionType;
import nl.ing.lovebird.providerdomain.YoltCategory;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Clock;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;


/**
 * Note: in practice {@link ExtendedTransactionDTO} will only ever have either creditor *or* debtor information, never both.
 * This test fills them both to make sure the right information is taken when mapping to a {@link DsTransaction} since the
 * {@link DsTransaction} does not distinguish between debtor and creditor information but calls it counterparty instead.
 */
public class DataScienceServiceDsTransactionMappingTest {

    private final ObjectMapper om = new ObjectMapper();
    DataScienceService dss = new DataScienceService(null, om, null, null, Clock.systemUTC());

    @Test
    public void given_noExtendedTransactionInformation_when_mapping_then_counterPartyInformationIsEmpty() {
        List<DsTransaction> result = dss.toDsTransactionList(UUID.randomUUID(), UUID.randomUUID(), CurrencyCode.EUR, List.of(
                new ProviderTransactionWithId(transactionBuilderWithRequiredFields()
                        .extendedTransaction(null)
                        .build(), "1")
        ));
        assertThat(result.get(0).getBankCounterpartyBban()).isNull();
        assertThat(result.get(0).getBankCounterpartyIban()).isNull();
        assertThat(result.get(0).getBankCounterpartyMaskedPan()).isNull();
        assertThat(result.get(0).getBankCounterpartyName()).isNull();
        assertThat(result.get(0).getBankCounterpartyPan()).isNull();
        assertThat(result.get(0).getBankCounterpartySortCodeAccountNumber()).isNull();
    }

    @Test
    public void given_creditTrx_when_mapping_then_counterPartyInformationIsFilledWithDebtorInformation_BBAN() {
        AccountReferenceType accountType = AccountReferenceType.BBAN;
        List<DsTransaction> result = dss.toDsTransactionList(UUID.randomUUID(), UUID.randomUUID(), CurrencyCode.EUR, List.of(
                new ProviderTransactionWithId(transactionBuilderWithRequiredFieldsAndCounterPartyInformation(accountType).build(), "1")
        ));
        assertThat(result.get(0).getBankCounterpartyName()).isEqualTo("debtorname");
        assertThat(result.get(0).getBankCounterpartyBban()).isEqualTo("debtorbban");
    }

    @Test
    public void given_creditTrx_when_mapping_then_counterPartyInformationIsFilledWithDebtorInformation_IBAN() {
        AccountReferenceType accountType = AccountReferenceType.IBAN;
        List<DsTransaction> result = dss.toDsTransactionList(UUID.randomUUID(), UUID.randomUUID(), CurrencyCode.EUR, List.of(
                new ProviderTransactionWithId(transactionBuilderWithRequiredFieldsAndCounterPartyInformation(accountType).build(), "1")
        ));
        assertThat(result.get(0).getBankCounterpartyName()).isEqualTo("debtorname");
        assertThat(result.get(0).getBankCounterpartyIban()).isEqualTo("debtoriban");
    }

    @Test
    public void given_creditTrx_when_mapping_then_counterPartyInformationIsFilledWithDebtorInformation_MASKEDPAN() {
        AccountReferenceType accountType = AccountReferenceType.MASKED_PAN;
        List<DsTransaction> result = dss.toDsTransactionList(UUID.randomUUID(), UUID.randomUUID(), CurrencyCode.EUR, List.of(
                new ProviderTransactionWithId(transactionBuilderWithRequiredFieldsAndCounterPartyInformation(accountType).build(), "1")
        ));
        assertThat(result.get(0).getBankCounterpartyName()).isEqualTo("debtorname");
        assertThat(result.get(0).getBankCounterpartyMaskedPan()).isEqualTo("debtormasked_pan");
    }

    @Test
    public void given_creditTrx_when_mapping_then_counterPartyInformationIsFilledWithDebtorInformation_PAN() {
        AccountReferenceType accountType = AccountReferenceType.PAN;
        List<DsTransaction> result = dss.toDsTransactionList(UUID.randomUUID(), UUID.randomUUID(), CurrencyCode.EUR, List.of(
                new ProviderTransactionWithId(transactionBuilderWithRequiredFieldsAndCounterPartyInformation(accountType).build(), "1")
        ));
        assertThat(result.get(0).getBankCounterpartyName()).isEqualTo("debtorname");
        assertThat(result.get(0).getBankCounterpartyPan()).isEqualTo("debtorpan");
    }

    @Test
    public void given_creditTrx_when_mapping_then_counterPartyInformationIsFilledWithDebtorInformation_SORTCODEACCOUNTNUMBER() {
        AccountReferenceType accountType = AccountReferenceType.SORTCODEACCOUNTNUMBER;
        List<DsTransaction> result = dss.toDsTransactionList(UUID.randomUUID(), UUID.randomUUID(), CurrencyCode.EUR, List.of(
                new ProviderTransactionWithId(transactionBuilderWithRequiredFieldsAndCounterPartyInformation(accountType).build(), "1")
        ));
        assertThat(result.get(0).getBankCounterpartyName()).isEqualTo("debtorname");
        assertThat(result.get(0).getBankCounterpartySortCodeAccountNumber()).isEqualTo("debtorsortcodeaccountnumber");
    }

    @Test
    public void given_debitTrx_when_mapping_then_counterPartyInformationIsFilledWithCreditorInformation_BBAN() {
        AccountReferenceType accountType = AccountReferenceType.BBAN;
        List<DsTransaction> result = dss.toDsTransactionList(UUID.randomUUID(), UUID.randomUUID(), CurrencyCode.EUR, List.of(
                new ProviderTransactionWithId(transactionBuilderWithRequiredFieldsAndCounterPartyInformation(accountType)
                        .type(ProviderTransactionType.DEBIT)
                        .build(), "1")
        ));
        assertThat(result.get(0).getBankCounterpartyName()).isEqualTo("creditorname");
        assertThat(result.get(0).getBankCounterpartyBban()).isEqualTo("creditorbban");
    }

    @Test
    public void given_debitTrx_when_mapping_then_counterPartyInformationIsFilledWithCreditorInformation_IBAN() {
        AccountReferenceType accountType = AccountReferenceType.IBAN;
        List<DsTransaction> result = dss.toDsTransactionList(UUID.randomUUID(), UUID.randomUUID(), CurrencyCode.EUR, List.of(
                new ProviderTransactionWithId(transactionBuilderWithRequiredFieldsAndCounterPartyInformation(accountType)
                        .type(ProviderTransactionType.DEBIT)
                        .build(), "1")
        ));
        assertThat(result.get(0).getBankCounterpartyName()).isEqualTo("creditorname");
        assertThat(result.get(0).getBankCounterpartyIban()).isEqualTo("creditoriban");
    }

    @Test
    public void given_debitTrx_when_mapping_then_counterPartyInformationIsFilledWithCreditorInformation_MASKEDPAN() {
        AccountReferenceType accountType = AccountReferenceType.MASKED_PAN;
        List<DsTransaction> result = dss.toDsTransactionList(UUID.randomUUID(), UUID.randomUUID(), CurrencyCode.EUR, List.of(
                new ProviderTransactionWithId(transactionBuilderWithRequiredFieldsAndCounterPartyInformation(accountType)
                        .type(ProviderTransactionType.DEBIT)
                        .build(), "1")
        ));
        assertThat(result.get(0).getBankCounterpartyName()).isEqualTo("creditorname");
        assertThat(result.get(0).getBankCounterpartyMaskedPan()).isEqualTo("creditormasked_pan");
    }

    @Test
    public void given_debitTrx_when_mapping_then_counterPartyInformationIsFilledWithCreditorInformation_PAN() {
        AccountReferenceType accountType = AccountReferenceType.PAN;
        List<DsTransaction> result = dss.toDsTransactionList(UUID.randomUUID(), UUID.randomUUID(), CurrencyCode.EUR, List.of(
                new ProviderTransactionWithId(transactionBuilderWithRequiredFieldsAndCounterPartyInformation(accountType)
                        .type(ProviderTransactionType.DEBIT)
                        .build(), "1")
        ));
        assertThat(result.get(0).getBankCounterpartyName()).isEqualTo("creditorname");
        assertThat(result.get(0).getBankCounterpartyPan()).isEqualTo("creditorpan");
    }

    @Test
    public void given_debitTrx_when_mapping_then_counterPartyInformationIsFilledWithCreditorInformation_SORTCODEACCOUNTNUMBER() {
        AccountReferenceType accountType = AccountReferenceType.SORTCODEACCOUNTNUMBER;
        List<DsTransaction> result = dss.toDsTransactionList(UUID.randomUUID(), UUID.randomUUID(), CurrencyCode.EUR, List.of(
                new ProviderTransactionWithId(transactionBuilderWithRequiredFieldsAndCounterPartyInformation(accountType)
                        .type(ProviderTransactionType.DEBIT)
                        .build(), "1")
        ));
        assertThat(result.get(0).getBankCounterpartyName()).isEqualTo("creditorname");
        assertThat(result.get(0).getBankCounterpartySortCodeAccountNumber()).isEqualTo("creditorsortcodeaccountnumber");
    }


    private ProviderTransactionDTO.ProviderTransactionDTOBuilder transactionBuilderWithRequiredFields() {
        return ProviderTransactionDTO.builder()
                .dateTime(ZonedDateTime.now())
                .amount(BigDecimal.ONE)
                .status(TransactionStatus.BOOKED)
                .type(ProviderTransactionType.CREDIT)
                .description("desc")
                .category(YoltCategory.GENERAL);
    }

    private ProviderTransactionDTO.ProviderTransactionDTOBuilder transactionBuilderWithRequiredFieldsAndCounterPartyInformation(AccountReferenceType type) {
        return transactionBuilderWithRequiredFields()
                .extendedTransaction(ExtendedTransactionDTO.builder()
                        .debtorName("debtorname")
                        .creditorName("creditorname")
                        .debtorAccount(AccountReferenceDTO.builder()
                                .type(type)
                                .value("debtor" + type.name().toLowerCase())
                                .build())
                        .creditorAccount(AccountReferenceDTO.builder()
                                .type(type)
                                .value("creditor" + type.name().toLowerCase())
                                .build())
                        .build());
    }

}
