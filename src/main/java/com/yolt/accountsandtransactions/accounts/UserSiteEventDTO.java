package com.yolt.accountsandtransactions.accounts;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

import java.time.ZonedDateTime;
import java.util.UUID;

@Data
@Builder
@AllArgsConstructor
public class UserSiteEventDTO {
    private UUID userId;
    private UUID userSiteId;
    private UUID siteId;
    private ZonedDateTime time;
    private EventType type;

    public enum EventType {
        UPDATE_USER_SITE,
        DELETE_USER_SITE
    }
}
