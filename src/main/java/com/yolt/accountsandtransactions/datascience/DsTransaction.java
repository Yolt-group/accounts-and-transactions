package com.yolt.accountsandtransactions.datascience;

import com.datastax.driver.mapping.annotations.*;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Map;
import java.util.UUID;


@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder(toBuilder = true)
@Table(name = DsTransaction.TABLE_NAME)
public class DsTransaction {

    public static final DateTimeFormatter DATE_FORMAT = DateTimeFormatter.ISO_LOCAL_DATE;

    // This is temporarily a shadow table content of which will be compared with the master table
    public static final String TABLE_NAME = "transactions";

    public static final String USER_ID_COLUMN = "user_id";
    public static final String PENDING_COLUMN = "pending";
    public static final String ACCOUNT_ID_COLUMN = "account_id";
    public static final String TRANSACTION_ID_COLUMN = "transaction_id";
    public static final String EXTERNAL_ID_COLUMN = "external_id";
    public static final String TRANSACTION_TIME = "transaction_timestamp";
    public static final String TIME_ZONE = "time_zone";
    public static final String TRANSACTION_TYPE_COLUMN = "transaction_type";
    public static final String DATE_COLUMN = "date";
    public static final String AMOUNT_COLUMN = "amount";
    public static final String CURRENCY_COLUMN = "currency";
    public static final String DESCRIPTION_COLUMN = "description";
    public static final String MAPPED_CATEGORY_COLUMN = "mapped_category";
    public static final String EXTENDED_TRANSACTION_COLUMN = "extended_transaction";
    public static final String LAST_UPDATED_TIME = "last_updated_time";
    public static final String BANK_SPECIFIC = "bank_specific";
    public static final String BANK_COUNTERPARTY_BBAN = "bank_counterparty_bban";
    public static final String BANK_COUNTERPARTY_IBAN = "bank_counterparty_iban";
    public static final String BANK_COUNTERPARTY_MASKED_PAN = "bank_counterparty_masked_pan";
    public static final String BANK_COUNTERPARTY_NAME = "bank_counterparty_name";
    public static final String BANK_COUNTERPARTY_PAN = "bank_counterparty_pan";
    public static final String BANK_COUNTERPARTY_SORT_CODE_ACCOUNT_NUMBER = "bank_counterparty_sort_code_account_number";

    @PartitionKey
    @Column(name = USER_ID_COLUMN)
    private UUID userId;

    @ClusteringColumn()
    @Column(name = PENDING_COLUMN)
    private Integer pending;

    @ClusteringColumn(1)
    @Column(name = ACCOUNT_ID_COLUMN)
    private UUID accountId;

    @ClusteringColumn(2)
    @Column(name = DATE_COLUMN)
    private String date;

    @ClusteringColumn(3)
    @Column(name = TRANSACTION_ID_COLUMN)
    private String transactionId;

    @Column(name = EXTERNAL_ID_COLUMN)
    private String externalId;

    @Column(name = TRANSACTION_TIME)
    private Instant transactionTimestamp;

    @Column(name = TIME_ZONE)
    private String timeZone;

    @Column(name = TRANSACTION_TYPE_COLUMN)
    private String transactionType;

    @Column(name = AMOUNT_COLUMN)
    private BigDecimal amount;

    @Column(name = CURRENCY_COLUMN)
    private String currency;

    @Column(name = DESCRIPTION_COLUMN)
    private String description;

    /**
     * Only filled for scraping providers.
     */
    @Column(name = MAPPED_CATEGORY_COLUMN)
    private String mappedCategory;

    @Column(name = EXTENDED_TRANSACTION_COLUMN)
    private ByteBuffer extendedTransaction;

    @Column(name = LAST_UPDATED_TIME)
    private Instant lastUpdatedTime;

    @Column(name = BANK_SPECIFIC)
    private Map<String, String> bankSpecific;

    @Column(name = BANK_COUNTERPARTY_BBAN)
    private String bankCounterpartyBban;

    @Column(name = BANK_COUNTERPARTY_IBAN)
    private String bankCounterpartyIban;

    @Column(name = BANK_COUNTERPARTY_MASKED_PAN)
    private String bankCounterpartyMaskedPan;

    @Column(name = BANK_COUNTERPARTY_NAME)
    private String bankCounterpartyName;

    @Column(name = BANK_COUNTERPARTY_PAN)
    private String bankCounterpartyPan;

    @Column(name = BANK_COUNTERPARTY_SORT_CODE_ACCOUNT_NUMBER)
    private String bankCounterpartySortCodeAccountNumber;

    @Transient
    public String getExtendedTransactionAsString() {
        if (getExtendedTransaction() == null) {
            return null;
        } else {
            getExtendedTransaction().position(0);
            return StandardCharsets.UTF_8.decode(getExtendedTransaction()).toString();
        }
    }

    /**
     * @return the {@link DsTransaction#date} as {@link LocalDate}
     */
    @Transient
    public LocalDate getLocalDate() {
        return LocalDate.parse(this.date, DATE_FORMAT);
    }
}
