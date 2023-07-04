package com.yolt.accountsandtransactions.datascience;

import com.yolt.accountsandtransactions.accounts.Account;
import com.yolt.accountsandtransactions.inputprocessing.AccountFromProviders;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import nl.ing.lovebird.providerdomain.ProviderCreditCardDTO;
import org.springframework.stereotype.Service;

import java.util.Date;

@Service
@AllArgsConstructor
@Slf4j
class DsCreditCardsCurrentService {

    private final DsCreditCardsCurrentRepository dsCreditCardsCurrentRepository;

    public void saveDsCreditCardCurrent(Account account, AccountFromProviders accountsAndTransactionsAccountDTO) {
        final Date lastUpdated = Date.from(accountsAndTransactionsAccountDTO.getLastRefreshed().toInstant());
        final ProviderCreditCardDTO creditCardData = accountsAndTransactionsAccountDTO.getCreditCardData();
        final DSCreditCardCurrent creditCardCurrent = DSCreditCardCurrent.builder()
                .accountId(account.getId())
                .apr(creditCardData.getApr() != null ? creditCardData.getApr().doubleValue() : null)
                .availableCreditAmount(creditCardData.getAvailableCreditAmount())
                .cashApr(creditCardData.getCashApr() != null ? creditCardData.getCashApr().doubleValue() : null)
                .cashLimitAmount(creditCardData.getCashLimitAmount())
                .lastUpdatedTime(lastUpdated)
                .currencyCode(account.getCurrency().name())
                .dueAmount(creditCardData.getDueAmount())
                .dueDate(creditCardData.getDueDate() == null ? null : creditCardData.getDueDate().toLocalDate().toString())
                .externalAccountId(account.getExternalId())
                .externalSiteId("")
                .lastPaymentAmount(creditCardData.getLastPaymentAmount())
                .lastPaymentDate(creditCardData.getLastPaymentDate() == null ? null : creditCardData.getLastPaymentDate().toLocalDate().toString())
                .name(account.getName())
                .newChargesAmount(creditCardData.getNewChargesAmount())
                .runningBalanceAmount(creditCardData.getRunningBalanceAmount())
                .siteId(account.getSiteId())
                .totalCreditLineAmount(creditCardData.getTotalCreditLineAmount())
                .userId(accountsAndTransactionsAccountDTO.getYoltUserId())
                .userSiteId(account.getUserSiteId())
                .build();

        dsCreditCardsCurrentRepository.save(creditCardCurrent);
    }

}
