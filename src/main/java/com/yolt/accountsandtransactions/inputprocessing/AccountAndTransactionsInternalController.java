package com.yolt.accountsandtransactions.inputprocessing;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import nl.ing.lovebird.clienttokens.ClientUserToken;
import nl.ing.lovebird.clienttokens.annotations.VerifiedClientToken;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;

import javax.servlet.ServletInputStream;
import javax.servlet.ServletRequest;
import java.io.IOException;
import java.util.zip.GZIPInputStream;

import static org.springframework.http.MediaType.APPLICATION_JSON_VALUE;

@Slf4j
@RequiredArgsConstructor
@RestController
public class AccountAndTransactionsInternalController {

    private final ObjectMapper objectMapper;
    private final AccountsAndTransactionsRequestConsumer accountsAndTransactionsRequestConsumer;

    /**
     * This POST method accepts a batch of accounts and transactions from providers. Normally this is sent through kafka, but that has a limit of 10Mb. (uncompressed [1])
     * Pagination is cumbersome, due to the object structure of accounts with transactions. A transaction does not have a reference to an account yet,
     * because the accounts cannot be uniquely identified from the list. That is the job of A&T.
     * This would mean that we would need to send an indexed list of accounts, and transaction pages by accountIndex.
     * It would also cause us to temporarily store large json blobs, and clean them up.
     *
     * Therefore, we have chosen to let providers POST the data in the very unlikely case that a user has more than 'X' (big number) of transactions.
     *
     * We expect a zipped request body since we're dealing with a very large request body. Servers don't usually deal with this.
     * Note that headers like 'accept-encoding' are about the client accepting some encoding, not the server. In fact there is no standard
     * way to let the client know what the server can do. This is just by contract/documentation as it's an internal endpoint.
     * Note that jetty/undertow do have some functionality for this [2]. However, that has potential side effects and makes it less standard throughout yolt.
     * The code in this controller is very limited and also only uses standard java libs, therefore I preferred this solution.
     *
     * [1] Please see https://issues.apache.org/jira/browse/KAFKA-4169.
     * Or consult {@link org.apache.kafka.clients.producer.KafkaProducer}
     * <code>
     *     int serializedSize = AbstractRecords.estimateSizeInBytesUpperBound(this.apiVersions.maxUsableProduceMagic(), this.compressionType, serializedKey, serializedValue, headers); // The compression type does not decrease the estimate
     *     this.ensureValidRecordSize(serializedSize); // This is what throws the {@link org.apache.kafka.common.errors.RecordTooLargeException}
     * </code>
     * [2] https://www.baeldung.com/spring-resttemplate-compressing-requests
     *
     * @param servletRequest The request
     * @param clientUserToken The client user token
     * @throws IOException
     */
    @PostMapping(value = "/internal/users/{userId}/provider-accounts", produces = APPLICATION_JSON_VALUE)
    public void postProviderAccounts(final ServletRequest servletRequest, @VerifiedClientToken ClientUserToken clientUserToken) throws IOException {

        try (ServletInputStream servletInputStream = servletRequest.getInputStream();
                GZIPInputStream gzipInputStream = new GZIPInputStream(servletInputStream)) {

            AccountsAndTransactionsRequestDTO accountsAndTransactionsRequestDTO = objectMapper.readValue(gzipInputStream, AccountsAndTransactionsRequestDTO.class);
            log.info("Received provider accounts through HTTP with a total of {} transactions.", accountsAndTransactionsRequestDTO.getIngestionAccounts()
                    .stream()
                    .mapToInt(it -> it.getTransactions().size())
                    .sum());

            accountsAndTransactionsRequestConsumer.transactionsUpdate(accountsAndTransactionsRequestDTO, clientUserToken);
        }

    }
}
