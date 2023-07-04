package com.yolt.accountsandtransactions.datascience;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.experimental.UtilityClass;
import org.springframework.lang.Nullable;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Optional;

@UtilityClass
public class ServiceUtil {

    public static ByteBuffer asByteBuffer(final ObjectMapper objectMapper, final Object dtoObject) {
        if (dtoObject == null) {
            return null;
        } else {
            try {
                String jsonString = objectMapper.writeValueAsString(dtoObject);
                return StandardCharsets.UTF_8.encode(jsonString);
            } catch (JsonProcessingException e) {
                throw new ObjectMappingException(e.getMessage(), e);
            }
        }
    }

    @Nullable
    public static String asJsonString(final ObjectMapper objectMapper, final @Nullable Map<String, String> object) {
        return Optional.ofNullable(object).map(m -> {
            try {
                return objectMapper.writeValueAsString(m);
            } catch (JsonProcessingException e) {
                throw new ObjectMappingException(e.getMessage(), e);
            }
        }).orElse(null);
    }
}
