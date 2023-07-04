package com.yolt.accountsandtransactions.metrics.kafka;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.NotImplementedException;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

@Slf4j
class JsonKeyMetricKafkaRecordInterceptorTest {

    @Test
    @Disabled
    void walk() {
        throw new NotImplementedException();
    }

    @Test
    void shorten() {
        String shorten = JsonKeyMetricKafkaRecordInterceptor.shorten("com.yolt.accountsandtransactions.inputprocessing.AccountsAndTransactionsRequestDTO");
        assertThat(shorten).isEqualTo("c.y.a.i.AccountsAndTransactionsRequestDTO");
    }
}