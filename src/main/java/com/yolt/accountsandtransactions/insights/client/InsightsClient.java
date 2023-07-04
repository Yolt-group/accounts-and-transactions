package com.yolt.accountsandtransactions.insights.client;

import com.yolt.accountsandtransactions.insights.client.dto.CustomerDnaDTO;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClient;

import java.time.Duration;
import java.util.Optional;
import java.util.UUID;

import static java.time.temporal.ChronoUnit.SECONDS;
import static org.springframework.http.MediaType.APPLICATION_JSON;
import static org.springframework.http.MediaType.TEXT_PLAIN;

@Component
@Slf4j
public class InsightsClient {
    private static final int DEFAULT_TIMEOUT_IN_SECONDS = 10;

    private final WebClient webClient;

    public InsightsClient(final WebClient.Builder webClientBuilder,
                          @Value("${yolt.service.insights-api.url:https://insights-api/insights-api/}") final String insightsUrl) {
        this.webClient = webClientBuilder.baseUrl(insightsUrl).build();
    }

    public Optional<CustomerDnaDTO> getCustomerDna(final UUID userId) {
        return this.webClient.get()
                .uri("/dna/{userId}", userId)
                .accept(APPLICATION_JSON, TEXT_PLAIN)
                .retrieve()
                .bodyToMono(CustomerDnaDTO.class)
                .blockOptional(Duration.of(DEFAULT_TIMEOUT_IN_SECONDS, SECONDS));
    }
}
