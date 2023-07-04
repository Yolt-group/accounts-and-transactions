package com.yolt.accountsandtransactions.offloading;

import com.yolt.accountsandtransactions.accounts.Account;
import com.yolt.accountsandtransactions.accounts.Balance;
import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NonNull;
import nl.ing.lovebird.extendeddata.account.UsageType;
import nl.ing.lovebird.extendeddata.common.CurrencyCode;
import nl.ing.lovebird.providerdomain.AccountType;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;

@Data
@Builder
@EqualsAndHashCode(callSuper = false)
class OffloadableAccount extends OffloadablePayload {
    private final String offloadType = "account";

    @NonNull
    private UUID userId;
    @NonNull
    private UUID id;
    @NonNull
    private String externalId;
    @NonNull
    private AccountType type;
    @NonNull
    private CurrencyCode currency;
    @NonNull
    private BigDecimal balance;
    @NonNull
    private Account.Status status;
    @NonNull
    private UUID userSiteId;
    @NonNull
    private UUID siteId;

    private UUID clientId;
    private String name;
    private String product;
    private String accountHolder;
    private String iban;
    private String maskedPan;
    private String pan;
    private String bban;
    private String sortCodeAccountNumber;
    private BigDecimal interestRate;
    private BigDecimal creditLimit;
    private BigDecimal availableCredit;
    private String linkedAccount;
    private String bic;
    private String isMoneyPotOf;
    private Map<String, String> bankSpecific;
    private boolean hidden;
    private UsageType usage;
    private Instant lastDataFetchTime;
    private List<Balance> balances;
    private Instant createdAt;
}
