package com.yolt.accountsandtransactions.accounts.event;

import lombok.AllArgsConstructor;
import lombok.Data;

import javax.validation.constraints.NotNull;
import java.util.UUID;

@Data
@AllArgsConstructor
public class AccountEvent {

    @NotNull
    AccountEventType type;

    @NotNull
    UUID userId;

    @NotNull
    UUID userSiteId;

    @NotNull
    UUID accountId;

    @NotNull
    UUID siteId;

    String accountHolderName;
}
