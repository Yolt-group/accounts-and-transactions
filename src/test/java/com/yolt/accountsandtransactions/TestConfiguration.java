package com.yolt.accountsandtransactions;

import com.github.tomakehurst.wiremock.client.ResponseDefinitionBuilder;
import com.github.tomakehurst.wiremock.common.FileSource;
import com.github.tomakehurst.wiremock.extension.Parameters;
import com.github.tomakehurst.wiremock.extension.ResponseDefinitionTransformer;
import com.github.tomakehurst.wiremock.http.Request;
import com.github.tomakehurst.wiremock.http.ResponseDefinition;
import lombok.extern.slf4j.Slf4j;
import org.springframework.cloud.contract.wiremock.WireMockConfigurationCustomizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.HttpHeaders;

import java.time.LocalDateTime;
import org.springframework.context.annotation.Primary;

@Configuration
@Slf4j
public class TestConfiguration {
    public static final String INPUT_TOPIC = "ingestionRequest";

    /**
     * Our tests run too fast for wiremock to keep up with elegantly closing and reusing resources.
     * This leads to tests relying on Wiremock to become flaky because Wiremock can't open new connections and the tests
     * will fail with a "failed to respond" exception when calling Wiremock.
     *
     * By setting the Connection header to "close" we ensure all connections are closed immediately and the resources
     * can then be reused for following tests.
     * For context, see https://github.com/tomakehurst/wiremock/issues/97
     */
    @Bean
    WireMockConfigurationCustomizer optionsCustomizer() {
        return options -> options.extensions(new NoKeepAliveTransformer());
    }

    public static class NoKeepAliveTransformer extends ResponseDefinitionTransformer {
        @Override
        public ResponseDefinition transform(Request request, ResponseDefinition responseDefinition, FileSource files, Parameters parameters) {
            return ResponseDefinitionBuilder.like(responseDefinition)
                    .withHeader(HttpHeaders.CONNECTION, "close")
                    .build();
        }

        @Override
        public String getName() {
            return "keep-alive-disabler";
        }
    }

    @Bean
    @Primary
    public MappingAccountIdProvider mappingAccountIdProvider() {
        return new MappingAccountIdProvider();
    }

    @Bean
    public MutableClock clock() {
        return new MutableClock();
    }
}
