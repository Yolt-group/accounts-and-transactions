package com.yolt.accountsandtransactions;

import com.yolt.accountsandtransactions.accounts.Account;
import nl.ing.lovebird.extendeddata.common.CurrencyCode;
import nl.ing.lovebird.providerdomain.AccountType;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;


public class TestAccountBuilder {

    public static Account.AccountBuilder builder() {
        return Account.builder()
                .userId(UUID.randomUUID())
                .id(UUID.randomUUID())
                .userSiteId(UUID.randomUUID())
                .siteId(UUID.randomUUID())
                .externalId("externalId")
                .name("Account Name 1")
                .type(AccountType.CURRENT_ACCOUNT)
                .currency(CurrencyCode.GBP)
                .balance(new BigDecimal("1"))
                .lastDataFetchTime(Instant.now())
                .status(Account.Status.ENABLED)
                .iban("INGBNL00000000000");
    }
}
