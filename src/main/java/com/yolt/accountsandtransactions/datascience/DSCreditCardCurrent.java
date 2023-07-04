package com.yolt.accountsandtransactions.datascience;

import com.datastax.driver.mapping.annotations.Column;
import com.datastax.driver.mapping.annotations.Table;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.util.Date;
import java.util.UUID;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
@Table(name = DSCreditCardCurrent.TABLE_NAME)
public class DSCreditCardCurrent {
    public static final String TABLE_NAME = "creditcards_current";

    public static final String USER_ID_COLUMN = "user_id";
    private static final String ACCOUNT_ID_COLUMN = "account_id";
    private static final String USER_SITE_ID_COLUMN = "user_site_id";
    private static final String SITE_ID_COLUMN = "site_id";
    private static final String EXTERNAL_ACCOUNT_ID_COLUMN = "external_account_id";
    private static final String EXTERNAL_SITE_ID_COLUMN = "external_site_id";
    private static final String NAME_COLUMN = "name";
    private static final String CURRENCY_CODE_COLUMN = "currency_code";
    private static final String LAST_UPDATED_TIME_COLUMN = "last_updated_time";
    private static final String APR_COLUMN = "apr";
    private static final String CASH_APR_COLUMN = "cash_apr";
    private static final String DUE_AMOUNT_COLUMN = "due_amount";
    private static final String DUE_DATE_COLUMN = "due_date";
    private static final String AVAILABLE_CREDIT_AMOUNT_COLUMN = "available_credit_amount";
    private static final String RUNNING_BALANCE_AMOUNT_COLUMN = "running_balance_amount";
    private static final String MIN_PAYMENT_AMOUNT_COLUMN = "min_payment_amount";
    private static final String NEW_CHARGES_AMOUNT_COLUMN = "new_charges_amount";
    private static final String LAST_PAYMENT_AMOUNT_COLUMN = "last_payment_amount";
    private static final String LAST_PAYMENT_DATE_COLUMN = "last_payment_date";
    private static final String TOTAL_CREDIT_LINE_AMOUNT_COLUMN = "total_credit_line_amount";
    private static final String CASH_LIMIT_AMOUNT_COLUMN = "cash_limit_amount";

    @Column(name = USER_ID_COLUMN)
    private UUID userId;

    @Column(name = ACCOUNT_ID_COLUMN)
    private UUID accountId;

    @Column(name = USER_SITE_ID_COLUMN)
    private UUID userSiteId;

    @Column(name = SITE_ID_COLUMN)
    private UUID siteId;

    @Column(name = EXTERNAL_ACCOUNT_ID_COLUMN)
    private String externalAccountId;

    @Column(name = EXTERNAL_SITE_ID_COLUMN)
    private String externalSiteId;

    @Column(name = NAME_COLUMN)
    private String name;

    @Column(name = CURRENCY_CODE_COLUMN)
    private String currencyCode;

    @Column(name = LAST_UPDATED_TIME_COLUMN)
    private Date lastUpdatedTime;

    @Column(name = APR_COLUMN)
    private Double apr;

    @Column(name = CASH_APR_COLUMN)
    private Double cashApr;

    @Column(name = DUE_AMOUNT_COLUMN)
    private BigDecimal dueAmount;

    @Column(name = DUE_DATE_COLUMN)
    private String dueDate;

    @Column(name = AVAILABLE_CREDIT_AMOUNT_COLUMN)
    private BigDecimal availableCreditAmount;

    @Column(name = RUNNING_BALANCE_AMOUNT_COLUMN)
    private BigDecimal runningBalanceAmount;

    @Column(name = MIN_PAYMENT_AMOUNT_COLUMN)
    private BigDecimal minPaymentAmount;

    @Column(name = NEW_CHARGES_AMOUNT_COLUMN)
    private BigDecimal newChargesAmount;

    @Column(name = LAST_PAYMENT_AMOUNT_COLUMN)
    private BigDecimal lastPaymentAmount;

    @Column(name = LAST_PAYMENT_DATE_COLUMN)
    private String lastPaymentDate;

    @Column(name = TOTAL_CREDIT_LINE_AMOUNT_COLUMN)
    private BigDecimal totalCreditLineAmount;

    @Column(name = CASH_LIMIT_AMOUNT_COLUMN)
    private BigDecimal cashLimitAmount;
}
