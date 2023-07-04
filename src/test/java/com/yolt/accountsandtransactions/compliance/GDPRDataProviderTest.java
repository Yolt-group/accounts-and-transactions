package com.yolt.accountsandtransactions.compliance;

import com.yolt.accountsandtransactions.TestAccountBuilder;
import com.yolt.accountsandtransactions.TestBuilders;
import com.yolt.accountsandtransactions.accounts.AccountRepository;
import com.yolt.accountsandtransactions.transactions.TransactionRepository;
import com.yolt.accountsandtransactions.transactions.TransactionService;
import com.yolt.compliance.gdpr.client.spi.GDPRServiceDataProvider.FileMetaAndBytes;
import com.yolt.compliance.gdpr.client.spi.GDPRServiceDataProvider.FileMetadata;
import nl.ing.lovebird.extendeddata.transaction.TransactionStatus;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class GDPRDataProviderTest {

    private static final UUID USER_ID = new UUID(0, 1);
    private static final UUID ACCOUNT_ID = new UUID(2, 3);

    @Mock
    private AccountRepository accountRepository;

    @Mock
    private TransactionRepository transactionRepository;

    private GDPRDataProvider dataProvider;

    @BeforeEach
    void beforeEach() {
        this.dataProvider = new GDPRDataProvider(accountRepository, transactionRepository);
    }

    @Test
    void testGetDataFileAsBytes() {

        final String expectedOutput = """
                account_id,holder_name,identification,name,balance,currency\r
                B0F998E384D8BEFCE236EEE5EC9B31BE0FB13BD60AFC77677EDB5196454B37C4,Holder Name,IBAN1234567890,Account Name 1,1,GBP\r
                account_id,transaction_id,date,amount,currency,description\r
                B0F998E384D8BEFCE236EEE5EC9B31BE0FB13BD60AFC77677EDB5196454B37C4,id,1970-01-01,10,EUR,unstructured\r
                """;

        when(accountRepository.getAccounts(USER_ID)).thenReturn(
                List.of(TestAccountBuilder.builder()
                        .id(ACCOUNT_ID)
                        .accountHolder("Holder Name")
                        .iban("IBAN1234567890")
                        .build()
                )
        );

        when(transactionRepository.getTransactionsForUser(USER_ID)).thenReturn(
                List.of(TestBuilders.createTransactionTemplate(
                                new TransactionService.TransactionPrimaryKey(
                                        USER_ID,
                                        ACCOUNT_ID,
                                        LocalDate.EPOCH,
                                        "id",
                                        TransactionStatus.BOOKED)
                        ).toBuilder()
                        .build())
        );

        final var dataFileAsBytesOption = dataProvider.getDataFileAsBytes(USER_ID);

        final FileMetadata fileMetadata
                = dataFileAsBytesOption.map(FileMetaAndBytes::getMetadata).orElseThrow(AssertionError::new);
        final byte[] bytes
                = dataFileAsBytesOption.map(FileMetaAndBytes::getBytes).orElseThrow(AssertionError::new);

        assertThat(fileMetadata).isEqualTo(new FileMetadata("accounts-and-transactions", "csv"));
        assertThat(new String(bytes, StandardCharsets.UTF_8)).isEqualTo(expectedOutput);
    }
}