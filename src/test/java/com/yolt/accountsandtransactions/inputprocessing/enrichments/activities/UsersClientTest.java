package com.yolt.accountsandtransactions.inputprocessing.enrichments.activities;

import com.yolt.accountsandtransactions.inputprocessing.enrichments.UserContext;
import com.yolt.accountsandtransactions.inputprocessing.enrichments.UsersClient;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.client.RestClientTest;
import org.springframework.http.HttpStatus;
import org.springframework.test.web.client.MockRestServiceServer;
import org.springframework.web.client.RestClientResponseException;
import org.testcontainers.shaded.com.fasterxml.jackson.core.JsonProcessingException;
import org.testcontainers.shaded.com.fasterxml.jackson.databind.ObjectMapper;

import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.springframework.http.MediaType.APPLICATION_JSON;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.requestTo;
import static org.springframework.test.web.client.response.MockRestResponseCreators.withStatus;
import static org.springframework.test.web.client.response.MockRestResponseCreators.withSuccess;

@RestClientTest(UsersClient.class)
class UsersClientTest {

    final UUID clientId = UUID.randomUUID();
    final UUID userId = UUID.randomUUID();

    @Autowired
    MockRestServiceServer server;

    @Autowired
    UsersClient client;

    @AfterEach
    void verify() {
        server.verify();
    }

    @Test
    void shouldGetUserContext() throws JsonProcessingException {
        UserContext userContext = new UserContext(clientId, userId);
        server.expect(requestTo("/users/users/" + userId))
                .andRespond(withSuccess(new ObjectMapper().writeValueAsString(userContext), APPLICATION_JSON));
        Optional<UserContext> actual = client.getUserContext(userId);
        assertThat(actual).contains(userContext);
    }

    @Test
    void shouldReturnEmptyWhenUserCouldNotBeFound() {
        server.expect(requestTo("/users/users/" + userId))
                .andRespond(withStatus(HttpStatus.NOT_FOUND)
                        .contentType(APPLICATION_JSON)
                        .body("{\"code\":{\"US002\"}}"));
        Optional<UserContext> actual = client.getUserContext(userId);
        assertThat(actual).isEmpty();
    }

    @Test
    void shouldThrowRestClientResponseException() {
        server.expect(requestTo("/users/users/" + userId))
                .andRespond(withStatus(HttpStatus.BAD_REQUEST)
                        .contentType(APPLICATION_JSON)
                        .body("{\"code\":{\"US001\"}}"));
        assertThatThrownBy(() -> client.getUserContext(userId))
                .isInstanceOf(RestClientResponseException.class);
    }
}
