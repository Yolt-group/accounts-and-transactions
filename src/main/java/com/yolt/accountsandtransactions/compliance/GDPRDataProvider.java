package com.yolt.accountsandtransactions.compliance;

import com.yolt.accountsandtransactions.accounts.AccountRepository;
import com.yolt.accountsandtransactions.transactions.TransactionRepository;
import com.yolt.compliance.gdpr.client.spi.GDPRServiceDataProvider;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.springframework.stereotype.Component;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.util.Optional;
import java.util.UUID;

/**
 * A {@link GDPRServiceDataProvider} implementation that exports procured `accounts` data for a specific user.
 * <p>
 * Currently this implementation outputs a single CSV file as byte array. If the needs arises to export more than one file,
 * consider outputting a zip file (using apache commons compression for example).
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class GDPRDataProvider implements GDPRServiceDataProvider {

    private static final String SERVICE_NAME = "accounts-and-transactions";
    private static final String CSV_FORMAT = "csv";

    private static final String LABEL_ACC_ACCOUNT_ID = "account_id";
    private static final String LABEL_ACC_ACCOUNT_HOLDER_NAME = "holder_name";
    private static final String LABEL_ACC_ACCOUNT_IDENTIFICATION = "identification";
    private static final String LABEL_ACC_ACCOUNT_NAME = "name";
    private static final String LABEL_ACC_BALANCE = "balance";
    private static final String LABEL_ACC_CURRENCY = "currency";

    private static final String LABEL_TRX_ACCOUNT_ID = "account_id";
    private static final String LABEL_TRX_TRANSACTION_ID = "transaction_id";
    private static final String LABEL_TRX_DATE = "date";
    private static final String LABEL_TRX_CURRENCY = "currency";
    private static final String LABEL_TRX_AMOUNT = "amount";
    private static final String LABEL_TRX_DESCRIPTION = "description";

    private final AccountRepository accountRepository;
    private final TransactionRepository transactionRepository;

    @Override
    public Optional<FileMetaAndBytes> getDataFileAsBytes(final UUID userId) {

        final var output = new ByteArrayOutputStream();

        log.info("Adding accounts to GDPR export");

        try (final var writer = new OutputStreamWriter(output, StandardCharsets.UTF_8)) {
            final CSVPrinter csvPrinter;
            try {
                csvPrinter = new CSVPrinter(writer, CSVFormat.DEFAULT.withHeader(
                        LABEL_ACC_ACCOUNT_ID,
                        LABEL_ACC_ACCOUNT_HOLDER_NAME,
                        LABEL_ACC_ACCOUNT_IDENTIFICATION,
                        LABEL_ACC_ACCOUNT_NAME,
                        LABEL_ACC_BALANCE,
                        LABEL_ACC_CURRENCY
                ));
            } catch (IOException e) {
                throw new GDPRDataProviderException(e);
            }

            final var accounts = accountRepository.getAccounts(userId);
            accounts.forEach(account -> {
                try {
                    csvPrinter.printRecord(
                            hash(account.getId().toString()),
                            account.getAccountHolder(),
                            account.getAccountNumber().flatMap(an -> an.identification).orElse(""),
                            account.getName(),
                            account.getBalance(),
                            account.getCurrency());
                } catch (IOException e) {
                    throw new GDPRDataProviderException(e);
                }
            });

            final CSVPrinter csvPrinterTrx;
            try {
                csvPrinterTrx = new CSVPrinter(writer, CSVFormat.DEFAULT.withHeader(
                        LABEL_TRX_ACCOUNT_ID,
                        LABEL_TRX_TRANSACTION_ID,
                        LABEL_TRX_DATE,
                        LABEL_TRX_AMOUNT,
                        LABEL_TRX_CURRENCY,
                        LABEL_TRX_DESCRIPTION
                ));
            } catch (IOException e) {
                throw new GDPRDataProviderException(e);
            }

            log.info("Adding transactions to GDPR export");

            transactionRepository.getTransactionsForUser(userId).forEach(tx -> {
                try {
                    csvPrinterTrx.printRecord(
                            hash(tx.getAccountId().toString()),
                            tx.getId(),
                            tx.getDate(),
                            tx.getAmount(),
                            tx.getCurrency(),
                            tx.getRemittanceInformationUnstructured());
                } catch (IOException e) {
                    throw new GDPRDataProviderException(e);
                }
            });
        } catch (IOException e) {
            throw new GDPRDataProviderException(e);
        }

        log.info("Returning GDPR data file");

        return Optional.of(new FileMetaAndBytes(new FileMetadata(SERVICE_NAME, CSV_FORMAT), output.toByteArray()));
    }

    @Override
    public String getServiceId() {
        return SERVICE_NAME;
    }

    public static class GDPRDataProviderException extends RuntimeException {

        GDPRDataProviderException(Throwable cause) {
            super(cause);
        }
    }
}
