package com.yolt.accountsandtransactions.inputprocessing;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

import java.util.List;
import java.util.UUID;

@Builder
@Data
@AllArgsConstructor
public class AccountsAndTransactionsRequestDTO {
    private UUID activityId;
    private List<AccountFromProviders> ingestionAccounts;
    private UUID userSiteId;
    private UUID siteId;
}
