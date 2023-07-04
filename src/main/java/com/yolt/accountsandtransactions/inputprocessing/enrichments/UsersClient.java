package com.yolt.accountsandtransactions.inputprocessing.enrichments;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.web.client.RestTemplateBuilder;
import org.springframework.stereotype.Service;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.RestTemplate;

import java.util.Optional;
import java.util.UUID;

import static java.util.Optional.empty;

@Service
@Slf4j
public class UsersClient {
    private final RestTemplate restTemplate;

    public UsersClient(
            @Value("${service.users.url:https://users}") String usersServiceUrl,
            RestTemplateBuilder restTemplateBuilder) {
        this.restTemplate = restTemplateBuilder
                .rootUri(usersServiceUrl)
                .build();
    }

    public Optional<UserDTO> getUser(UUID userId) {
        try {
            return Optional.ofNullable(restTemplate.getForObject("/users/users/{userId}", UserDTO.class, userId));
        } catch (HttpClientErrorException.NotFound ex) {
            log.warn("Unable to find user for userId={}.", userId);
        }
        return empty();
    }

    public Optional<UserContext> getUserContext(UUID userId) {
        return getUser(userId).map(userDTO -> new UserContext(userDTO.clientId(), userDTO.userId()));
    }
}
