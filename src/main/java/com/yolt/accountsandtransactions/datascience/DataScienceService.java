package com.yolt.accountsandtransactions.datascience;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.yolt.accountsandtransactions.accounts.Account;
import com.yolt.accountsandtransactions.inputprocessing.AccountFromProviders;
import com.yolt.accountsandtransactions.inputprocessing.ProviderTransactionWithId;
import com.yolt.accountsandtransactions.transactions.TransactionService.TransactionPrimaryKey;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import nl.ing.lovebird.extendeddata.common.AccountReferenceDTO;
import nl.ing.lovebird.extendeddata.common.CurrencyCode;
import nl.ing.lovebird.extendeddata.transaction.AccountReferenceType;
import nl.ing.lovebird.providerdomain.ProviderTransactionType;
import org.springframework.stereotype.Service;

import java.time.Clock;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Slf4j
@RequiredArgsConstructor
@Service
public class DataScienceService {

    private final DsTransactionsRepository dsTransactionsRepository;
    private final ObjectMapper objectMapper;
    private final DsAccountsCurrentService dsAccountsCurrentService;
    private final DsCreditCardsCurrentService dsCreditCardsCurrentService;
    private final Clock clock;

    public List<DsTransaction> toDsTransactionList(final UUID accountId,
                                                   final UUID userId,
                                                   final CurrencyCode currencyCode,
                                                   final List<ProviderTransactionWithId> transactions) {
        return transactions.stream()
                .map(wpt -> {
                    var providerTransactionDTO = wpt.getProviderTransactionDTO();
                    final String transactionDate = providerTransactionDTO.getDateTime().format(DsTransaction.DATE_FORMAT);

                    String counterPartyName = null;
                    AccountReferenceDTO counterPartyAccount = null;
                    if (providerTransactionDTO.getExtendedTransaction() != null) {
                        counterPartyName = providerTransactionDTO.getType() == ProviderTransactionType.CREDIT
                                ? providerTransactionDTO.getExtendedTransaction().getDebtorName()
                                : providerTransactionDTO.getExtendedTransaction().getCreditorName();
                        counterPartyAccount = providerTransactionDTO.getType() == ProviderTransactionType.CREDIT
                                ? providerTransactionDTO.getExtendedTransaction().getDebtorAccount()
                                : providerTransactionDTO.getExtendedTransaction().getCreditorAccount();
                    }

                    return DsTransaction.builder()
                            .userId(userId)
                            .pending(PendingType.of(providerTransactionDTO.getStatus()))
                            .accountId(accountId)
                            .transactionId(wpt.getTransactionId())
                            .externalId(providerTransactionDTO.getExternalId())
                            .date(transactionDate)
                            .transactionTimestamp(providerTransactionDTO.getDateTime().toInstant())
                            .timeZone(providerTransactionDTO.getDateTime().getOffset().toString())
                            .transactionType(TransactionType.of(providerTransactionDTO.getType()))
                            .amount(providerTransactionDTO.getAmount())
                            .currency(currencyCode.name())
                            .mappedCategory(providerTransactionDTO.getCategory().getValue())
                            .description(providerTransactionDTO.getDescription())
                            .extendedTransaction(ServiceUtil.asByteBuffer(objectMapper, providerTransactionDTO.getExtendedTransaction()))
                            .bankSpecific(providerTransactionDTO.getBankSpecific())
                            .bankCounterpartyBban(counterPartyAccount != null && counterPartyAccount.getType() == AccountReferenceType.BBAN ? counterPartyAccount.getValue() : null)
                            .bankCounterpartyIban(counterPartyAccount != null && counterPartyAccount.getType() == AccountReferenceType.IBAN ? counterPartyAccount.getValue() : null)
                            .bankCounterpartyMaskedPan(counterPartyAccount != null && counterPartyAccount.getType() == AccountReferenceType.MASKED_PAN ? counterPartyAccount.getValue() : null)
                            .bankCounterpartyName(counterPartyName)
                            .bankCounterpartyPan(counterPartyAccount != null && counterPartyAccount.getType() == AccountReferenceType.PAN ? counterPartyAccount.getValue() : null)
                            .bankCounterpartySortCodeAccountNumber(counterPartyAccount != null && counterPartyAccount.getType() == AccountReferenceType.SORTCODEACCOUNTNUMBER ? counterPartyAccount.getValue() : null)
                            .lastUpdatedTime(Instant.now(clock))
                            .build();
                })
                .collect(Collectors.toList());
    }

    public Stream<String> getDatesPendingTransactions(final UUID userId, final List<UUID> accountId) {
        return dsTransactionsRepository.getDatesPendingTransactions(userId, accountId);
    }

    public void saveTransactionBatch(List<DsTransaction> transactions) {
        dsTransactionsRepository.saveTransactionBatch(transactions);
    }

    public void saveAccount(Account account, AccountFromProviders accountFromProviders) {
        dsAccountsCurrentService.saveDsAccountCurrent(account, accountFromProviders);
        if (accountFromProviders.getCreditCardData() != null) {
            dsCreditCardsCurrentService.saveDsCreditCardCurrent(account, accountFromProviders);
        }
    }

    public List<DsTransaction> getTransactionsForUser(UUID userId) {
        return dsTransactionsRepository.getTransactionsForUser(userId);
    }

    /**
     * Delete a batch (unlogged) of {@link com.yolt.accountsandtransactions.transactions.Transaction} identified by their primary key.
     *
     * @param transactionPrimaryKeys the transactions to delete identified by their primary key
     */
    public void deleteSpecificTransactions(final @NonNull List<TransactionPrimaryKey> transactionPrimaryKeys) {
        dsTransactionsRepository.deleteSpecificTransactions(transactionPrimaryKeys);
    }
}
