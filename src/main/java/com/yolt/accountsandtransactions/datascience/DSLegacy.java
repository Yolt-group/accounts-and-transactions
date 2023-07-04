package com.yolt.accountsandtransactions.datascience;

import lombok.experimental.UtilityClass;

import java.util.Collections;
import java.util.UUID;

@UtilityClass
public class DSLegacy {

    /**
     * We don't use UserContext anymore, however, datascience still relies on it, although the usercontext doesn't have any
     * meaningfull fields anymore. Fields they rely on are country-code and an enabledFeatures list. (it crashes if the enabledFeatures = null)
     *
     * See https://git.yolt.io/datascience/datascience-commons/-/blob/master/commons/src/main/scala/com/yolt/datascience/infrastructure/tracing/UserContext.scala
     */
    public UserContext fakeUserContext(UUID userId) {
        return UserContext.builder()
                .userId(userId)
                .countryCode("NL")
                .enabledFeatures(Collections.emptyList())
                .build();
    }
}
