package com.yolt.accountsandtransactions.legacytransactions;

import com.yolt.accountsandtransactions.transactions.TransactionDTO;
import com.yolt.accountsandtransactions.transactions.TransactionService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import nl.ing.lovebird.clienttokens.ClientUserToken;
import com.yolt.accountsandtransactions.datetime.DateInterval;
import nl.ing.lovebird.extendeddata.common.BalanceAmountDTO;
import nl.ing.lovebird.extendeddata.transaction.ExtendedTransactionDTO;
import org.springframework.stereotype.Service;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.springframework.web.util.UriComponents;
import org.springframework.web.util.UriComponentsBuilder;

import java.time.Clock;
import java.time.LocalDate;
import java.util.Currency;
import java.util.List;
import java.util.UUID;

import static java.util.Optional.ofNullable;
import static java.util.stream.Collectors.toList;

@RequiredArgsConstructor
@Service
@Slf4j
public class LegacyTransactionService {
    private static final String DEFAULT_CATEGORY = "General";

    private final TransactionService transactionService;
    private final Clock clock;

    public LegacyTransactionsByAccountDTO getTransactions(ClientUserToken clientUserToken, UUID accountId, DateInterval interval, final String pagingState) {
        var dateInterval = ofNullable(interval)
                .orElse(new DateInterval(LocalDate.now(clock).minusYears(10), LocalDate.now(clock)));
        var accountIds = List.of(accountId);

        var transactionsPage = transactionService.getTransactions(clientUserToken.getUserIdClaim(), accountIds, dateInterval, pagingState, 100);

        return LegacyTransactionsByAccountDTO.builder()
                .transactions(
                        transactionsPage.getTransactions().stream()
                                .map(LegacyTransactionService::map)
                                .collect(toList())
                )
                .links(
                        new LegacyTransactionsByAccountDTO.LinksDTO(createTransactionsByAccountLink(accountId, dateInterval, transactionsPage.getNext()))
                )
                .build();
    }

    static LegacyTransactionDTO map(final TransactionDTO transaction) {
        var builder = new LegacyTransactionDTO.LegacyTransactionDTOBuilder()
                .accountId(transaction.getAccountId())
                .id(transaction.getId())
                .transactionType(LegacyTransactionType.from(transaction.getStatus()))
                .date(transaction.getDate())
                .dateTime(transaction.getTimestamp())
                .amount(transaction.getAmount())
                .currency(Currency.getInstance(transaction.getCurrency().toString()))
                .description(transaction.getDescription())
                .shortDescription(transaction.getDescription())
                .extendedTransaction(mapExtendedTransaction(transaction));

        if (transaction.getEnrichment() != null) {
            var enrichment = transaction.getEnrichment();
            builder.category(enrichment.getCategory())
                    .cycleId(enrichment.getCycleId())
                    .labels(enrichment.getLabels());

            if (enrichment.getCounterparty() != null && enrichment.getCounterparty().getName() != null) {
                if (enrichment.getCounterparty().knownMerchant) {
                    builder.merchantObject(LegacyMerchantDTO.from(enrichment.getCounterparty().getName()));
                }
                builder.shortDescription(enrichment.getCounterparty().getName());
            }
        } else {
            builder.category(DEFAULT_CATEGORY);
        }

        return builder.build();
    }

    /**
     * Returns extended transaction data that is straightforward to copy
     *
     * @param transaction source transaction
     * @return extended transaction data
     */
    static ExtendedTransactionDTO mapExtendedTransaction(final TransactionDTO transaction) {
        return ExtendedTransactionDTO.builder()
                .status(transaction.getStatus())
                .endToEndId(transaction.getEndToEndId())
                .transactionAmount(new BalanceAmountDTO(transaction.getCurrency(), transaction.getAmount()))
                .remittanceInformationUnstructured(transaction.getRemittanceInformationUnstructured())
                .remittanceInformationStructured(transaction.getRemittanceInformationStructured())
                .purposeCode(transaction.getPurposeCode())
                .bankTransactionCode(transaction.getBankTransactionCode())
                .build();
    }

    /**
     Returns the next link as if transactions pod would return it, i.e. /transactions/transactions-by-account/me.
     client-proxy maps this link to /legacy-transactions/transactions-by-account/me
     *
     * @param accountId account Id
     * @param dateInterval date interval
     * @param nextPageRef reference to the next page
     * @return link to next page
     */
    static LinkDTO createTransactionsByAccountLink(
            final UUID accountId,
            final DateInterval dateInterval,
            final String nextPageRef) {
        if (nextPageRef == null) {
            return null;
        }

        MultiValueMap<String, String> queryParams = new LinkedMultiValueMap<>();
        queryParams.add("accountId", accountId.toString());
        if (dateInterval != null) queryParams.add("dateInterval", dateInterval.toString());
        queryParams.add("next", nextPageRef);

        UriComponents uriComponents = UriComponentsBuilder.fromPath("/transactions/transactions-by-account/me")
                .queryParams(queryParams)
                .encode()
                .build();
        return new LinkDTO(uriComponents.toString());
    }
}
