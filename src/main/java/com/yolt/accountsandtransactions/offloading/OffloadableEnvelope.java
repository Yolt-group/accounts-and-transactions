package com.yolt.accountsandtransactions.offloading;

import com.fasterxml.jackson.databind.PropertyNamingStrategy;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import lombok.Builder;
import lombok.Data;
import lombok.NonNull;

import java.util.Optional;

import static java.util.Optional.empty;

@Data
@Builder
@JsonNaming(PropertyNamingStrategy.SnakeCaseStrategy.class) // DS uses snake-case for their properties.
class OffloadableEnvelope<K> {
    @NonNull
    private Boolean delete;
    @NonNull
    private Integer schemaVersion;
    @NonNull
    private K entityId;
    @NonNull
    private Optional<OffloadablePayload> payload;

    public static <K> OffloadableEnvelope<K> createInsertOrUpdate(int schemaVersion, @NonNull K entityId, @NonNull OffloadablePayload offloadablePayload) {
        return new OffloadableEnvelope<>(false, schemaVersion, entityId, Optional.of(offloadablePayload));
    }

    public static <K> OffloadableEnvelope<K> createDelete(int schemaVersion, @NonNull K entityId) {
        return new OffloadableEnvelope<>(true, schemaVersion, entityId, empty());
    }
}
