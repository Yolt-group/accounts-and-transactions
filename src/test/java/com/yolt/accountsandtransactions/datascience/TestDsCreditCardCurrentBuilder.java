package com.yolt.accountsandtransactions.datascience;

import lombok.Setter;
import lombok.experimental.Accessors;
import nl.ing.lovebird.extendeddata.common.CurrencyCode;

import java.math.BigDecimal;
import java.sql.Date;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.UUID;

@Setter
@Accessors(fluent = true, chain = true)
public class TestDsCreditCardCurrentBuilder {

    private String externalSiteID = "externalSiteID";
    private BigDecimal lastPaymentAmount = new BigDecimal("1");
    private String lastPaymentDate = "2011-01-02";
    private BigDecimal newChargesAmount = new BigDecimal("2");
    private BigDecimal runningBalanceAmount = new BigDecimal("3");
    private UUID siteId = new UUID(0, 0);
    private BigDecimal totalCreditLineAmount = new BigDecimal("4");
    private UUID userId = new UUID(0, 1);
    private java.util.Date lastUpdatedTime = Date.from(ZonedDateTime.of(2011, 1, 1, 1, 1, 1, 1, ZoneId.of("UTC")).toInstant());
    private UUID userSiteId = new UUID(0, 3);
    private String currencyCode = CurrencyCode.GBP.name();
    private UUID accountId = new UUID(1, 1);

    private TestDsCreditCardCurrentBuilder() {
    }

    public static TestDsCreditCardCurrentBuilder testDsCreditCardCurrentBuilder() {
        return new TestDsCreditCardCurrentBuilder();
    }

    public DSCreditCardCurrent build() {
        return DSCreditCardCurrent.builder()
                .externalSiteId(externalSiteID)
                .lastPaymentAmount(lastPaymentAmount)
                .lastPaymentDate(lastPaymentDate)
                .newChargesAmount(newChargesAmount)
                .runningBalanceAmount(runningBalanceAmount)
                .siteId(siteId)
                .totalCreditLineAmount(totalCreditLineAmount)
                .userId(userId)
                .lastUpdatedTime(lastUpdatedTime)
                .userSiteId(userSiteId)
                .externalAccountId("externalAccountId")
                .dueDate("2012-02-03")
                .dueAmount(new BigDecimal("5"))
                .currencyCode(currencyCode)
                .accountId(accountId)
                .build();
    }
}
