package com.yolt.accountsandtransactions.datascience;

import com.datastax.driver.mapping.annotations.*;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Date;
import java.util.Map;
import java.util.UUID;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
@Table(name = DsAccountCurrent.TABLE_NAME)
public class DsAccountCurrent {
    public static final String TABLE_NAME = "account_current";

    public static final String USER_ID_COLUMN = "user_id";
    public static final String ACCOUNT_ID_COLUMN = "account_id";
    public static final String USER_SITE_ID_COLUMN = "user_site_id";
    public static final String SITE_ID_COLUMN = "site_id";
    public static final String EXTERNAL_ACCOUNT_ID_COLUMN = "external_account_id";
    public static final String EXTERNAL_SITE_ID_COLUMN = "external_site_id";
    public static final String NAME_COLUMN = "name";
    public static final String ACCOUNT_TYPE_COLUMN = "account_type";
    public static final String CURRENCY_CODE_COLUMN = "currency_code";
    public static final String CURRENT_BALANCE_COLUMN = "current_balance";
    public static final String AVAILABLE_BALANCE_COLUMN = "available_balance";
    public static final String LAST_UPDATED_TIME_COLUMN = "last_updated_time";
    public static final String STATUS_COLUMN = "status";
    public static final String STATUS_DETAIL_COLUMN = "status_detail";
    public static final String PROVIDER_COLUMN = "provider";
    public static final String EXTENDED_ACCOUNT_COLUMN = "extended_account";
    public static final String HIDDEN_COLUMN = "hidden";
    public static final String LINKED_ACCOUNT_COLUMN = "linked_account";
    public static final String BANK_SPECIFIC_COLUMN = "bank_specific";
    public static final String ACCOUNT_HOLDER_NAME = "account_holder_name";
    public static final String ACCOUNT_SCHEME = "account_scheme";
    public static final String ACCOUNT_IDENTIFICATION = "account_identification";
    public static final String ACCOUNT_SECONDARY_IDENTIFICATION = "account_secondary_identification";
    public static final String ACCOUNT_MASKED_IDENTIFICATION = "account_masked_identification";
    public static final String CLOSED = "closed";

    @PartitionKey
    @Column(name = USER_ID_COLUMN)
    private UUID userId;

    @ClusteringColumn
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

    @Column(name = ACCOUNT_TYPE_COLUMN)
    private String accountType;

    @Column(name = CURRENCY_CODE_COLUMN)
    private String currencyCode;

    @Column(name = CURRENT_BALANCE_COLUMN)
    private BigDecimal currentBalance;

    @Column(name = AVAILABLE_BALANCE_COLUMN)
    private BigDecimal availableBalance;

    @Column(name = LAST_UPDATED_TIME_COLUMN)
    private Date lastUpdatedTime;

    @Column(name = STATUS_COLUMN)
    private String status;

    @Column(name = STATUS_DETAIL_COLUMN)
    private String statusDetail;

    @Column(name = PROVIDER_COLUMN)
    private String provider;

    @Column(name = EXTENDED_ACCOUNT_COLUMN)
    private ByteBuffer extendedAccount;

    @Column(name = HIDDEN_COLUMN)
    private Boolean hidden;

    @Column(name = LINKED_ACCOUNT_COLUMN)
    private String linkedAccount;

    @Column(name = BANK_SPECIFIC_COLUMN)
    private String bankSpecific;

    @Column(name = ACCOUNT_HOLDER_NAME)
    private String accountHolderName;

    @Column(name = ACCOUNT_SCHEME)
    private String accountScheme;

    @Column(name = ACCOUNT_IDENTIFICATION)
    private String accountIdentification;

    @Column(name = ACCOUNT_SECONDARY_IDENTIFICATION)
    private String accountSecondaryIdentification;

    @Column(name = ACCOUNT_MASKED_IDENTIFICATION)
    private String accountMaskedIdentification;

    @Column(name = CLOSED)
    private boolean closed;

    @Transient
    public String getExtendedAccountAsString() {
        if(getExtendedAccount()==null) {
            return null;
        } else {
            getExtendedAccount().position(0);
            return StandardCharsets.UTF_8.decode(getExtendedAccount()).toString();
        }
    }
}
