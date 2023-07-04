package com.yolt.accountsandtransactions.offloading;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.yolt.accountsandtransactions.BaseIntegrationTest;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import static org.assertj.core.api.Assertions.assertThat;

class OffloadableEnvelopeTest extends BaseIntegrationTest {

    @Autowired
    ObjectMapper objectMapper;

    @Test
    void testSnakeCaseSerialization() throws JsonProcessingException {
        OffloadableEnvelope<String> envelope
                = OffloadableEnvelope.createDelete(1, "entity-id");

        String s = objectMapper.writeValueAsString(envelope);
        assertThat(s).isEqualTo("{\"delete\":true,\"schema_version\":1,\"entity_id\":\"entity-id\",\"payload\":null}");
    }
}