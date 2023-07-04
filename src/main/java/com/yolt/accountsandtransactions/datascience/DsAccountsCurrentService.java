package com.yolt.accountsandtransactions.datascience;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.yolt.accountsandtransactions.accounts.Account;
import com.yolt.accountsandtransactions.inputprocessing.AccountFromProviders;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import nl.ing.lovebird.providerdomain.ProviderAccountNumberDTO;
import org.springframework.stereotype.Service;

import java.util.Date;

import static java.util.Optional.ofNullable;

@Service
@AllArgsConstructor
@Slf4j
class DsAccountsCurrentService {

    private final DsAccountsCurrentRepository dsAccountsCurrentRepository;
    private final ObjectMapper objectMapper;

    public void saveDsAccountCurrent(Account account, AccountFromProviders accountsAndTransactionsAccountDTO) {
        final Date lastUpdated = Date.from(accountsAndTransactionsAccountDTO.getLastRefreshed().toInstant());
        final DsAccountCurrent dsAccountCurrent = DsAccountCurrent.builder()
                .userId(accountsAndTransactionsAccountDTO.getYoltUserId())
                .accountId(account.getId())
                .userSiteId(account.getUserSiteId())
                .siteId(account.getSiteId())
                .externalAccountId(account.getExternalId())
                // Python code expects a non-null value in externalSiteId, but doesn't do anything with it
                // With for refactoring in DS where unnecessary columns will be eliminated
                .externalSiteId("")
                .name(accountsAndTransactionsAccountDTO.getName())
                .accountType(accountsAndTransactionsAccountDTO.getYoltAccountType().toString())
                .currencyCode(account.getCurrency().name())
                .currentBalance(accountsAndTransactionsAccountDTO.getCurrentBalance())
                .availableBalance(accountsAndTransactionsAccountDTO.getAvailableBalance())
                .lastUpdatedTime(lastUpdated)
                .status("PROCESSING_TRANSACTIONS_FINISHED")
                .statusDetail("0_SUCCESS")
                .provider(accountsAndTransactionsAccountDTO.getProvider())
                .extendedAccount(ServiceUtil.asByteBuffer(objectMapper, accountsAndTransactionsAccountDTO.getExtendedAccount()))
                .bankSpecific(ServiceUtil.asJsonString(objectMapper, accountsAndTransactionsAccountDTO.getBankSpecific())) // Scala JsonString equivalent Map[String,String]
                .linkedAccount(accountsAndTransactionsAccountDTO.getLinkedAccount())
                .accountHolderName(ofNullable(accountsAndTransactionsAccountDTO.getAccountNumber())
                        .map(ProviderAccountNumberDTO::getHolderName)
                        .orElse(null))
                .accountScheme(ofNullable(accountsAndTransactionsAccountDTO.getAccountNumber())
                        .map(ProviderAccountNumberDTO::getScheme)
                        .map(Enum::name)
                        .orElse(null))
                .accountIdentification(ofNullable(accountsAndTransactionsAccountDTO.getAccountNumber())
                        .map(ProviderAccountNumberDTO::getIdentification)
                        .orElse(null))
                .accountSecondaryIdentification(ofNullable(accountsAndTransactionsAccountDTO.getAccountNumber())
                        .map(ProviderAccountNumberDTO::getSecondaryIdentification)
                        .orElse(null))
                .accountMaskedIdentification(accountsAndTransactionsAccountDTO.getAccountMaskedIdentification())
                .closed(ofNullable(accountsAndTransactionsAccountDTO.getClosed()).orElse(false))
                .build();

        dsAccountsCurrentRepository.save(dsAccountCurrent);
    }
}
