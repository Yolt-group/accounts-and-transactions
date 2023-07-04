package com.yolt.accountsandtransactions.legacyaccounts;

import lombok.AllArgsConstructor;
import lombok.Value;

import java.util.UUID;

@Value
@AllArgsConstructor
public class LegacyUserSiteDTO {

    // The id of the user-site
    UUID id;

    // The site-id of the user-site, referring to its parent
    UUID siteId;
}
