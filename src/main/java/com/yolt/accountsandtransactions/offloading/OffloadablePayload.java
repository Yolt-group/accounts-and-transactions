package com.yolt.accountsandtransactions.offloading;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

@JsonTypeInfo(
        use = JsonTypeInfo.Id.NAME,
        include = JsonTypeInfo.As.EXISTING_PROPERTY,
        property = "offloadType")
@JsonSubTypes({
        @JsonSubTypes.Type(value = OffloadableAccount.class, name = "account"),
        @JsonSubTypes.Type(value = OffloadableTransaction.class, name = "transaction")
})
abstract class OffloadablePayload {
}
