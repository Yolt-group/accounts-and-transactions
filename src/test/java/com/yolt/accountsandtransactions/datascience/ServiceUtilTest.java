package com.yolt.accountsandtransactions.datascience;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class ServiceUtilTest {

    private final static ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    @Test
    void asByteBuffer() {
        assertThat(ServiceUtil.asByteBuffer(OBJECT_MAPPER, Map.of("Hello", "World")))
                .isEqualTo(ByteBuffer.wrap("{\"Hello\":\"World\"}".getBytes(StandardCharsets.UTF_8)));
    }

    @Test
    void asJsonString() {
        assertThat(ServiceUtil.asJsonString(OBJECT_MAPPER, Map.of("Hello", "World"))).isEqualTo("{\"Hello\":\"World\"}");
    }
}