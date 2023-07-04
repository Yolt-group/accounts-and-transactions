package com.yolt.accountsandtransactions.datascience.counterparties;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.tomakehurst.wiremock.client.WireMock;
import com.yolt.accountsandtransactions.BaseIntegrationTest;
import com.yolt.accountsandtransactions.datascience.counterparties.client.dto.DsCounterpartiesAdjustedDTO;
import com.yolt.accountsandtransactions.datascience.counterparties.client.dto.MerchantsInCountriesDTO;
import lombok.SneakyThrows;
import nl.ing.lovebird.clienttokens.ClientUserToken;
import nl.ing.lovebird.clienttokens.test.TestClientTokens;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;
import org.springframework.web.util.UriComponentsBuilder;

import java.net.URI;
import java.util.Arrays;
import java.util.Collections;
import java.util.UUID;

import static com.github.tomakehurst.wiremock.client.WireMock.*;
import static java.util.UUID.randomUUID;
import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

class MerchantsControllerIntegrationTest extends BaseIntegrationTest {
    private static final String MERCHANT = "easyJet";
    private static final String COUNTRY = "NL";
    private final UUID clientGroupId = randomUUID();
    private final UUID clientId = randomUUID();
    private final UUID userId = randomUUID();

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private MerchantsService merchantsService;

    @Autowired
    private ObjectMapper objectMapper;

    @Autowired
    private TestClientTokens testClientTokens;

    private ClientUserToken clientUserToken;

    @BeforeEach
    void setupClientToken(){
        clientUserToken = testClientTokens.createClientUserToken(clientGroupId, clientId, userId);
    }

    @Test
    void shouldBeCaseInsensitive() {
        mockMultipleCountryMerchants();
        assertSuggestionsResponse("UK", "MAR", "Marie Curie Cancer Charity", "Marks & Spencer", "Marriott");
        assertSuggestionsResponse("UK", "mar", "Marie Curie Cancer Charity", "Marks & Spencer", "Marriott");
    }

    @Test
    void shouldIgnoreDiacriticsInRequest() {
        mockMultipleCountryMerchants();
        assertSuggestionsResponse("UK", "mär", "Marie Curie Cancer Charity", "Marks & Spencer", "Marriott");
    }

    @Test
    void shouldReturnMerchantsStartingWithNumbers() {
        mockMultipleCountryMerchants();
        assertSuggestionsResponse("UK", "3", "32Red", "365online.com");
    }

    @Test
    void shouldReturnMerchantsWithDashes() {
        mockMultipleCountryMerchants();
        assertSuggestionsResponse("UK", "G-Star", "G-Star Raw");
    }

    @Test
    void shouldReturnMerchantsWithSpaces() {
        mockMultipleCountryMerchants();
        assertSuggestionsResponse("UK", "Apple Pay", "Apple Pay");
    }

    @Test
    void shouldReturnMerchantsWithDots() {
        mockMultipleCountryMerchants();
        assertSuggestionsResponse("UK", "Amazon.", "Amazon.com");
    }

    @Test
    void shouldReturnMerchantsWithDiacritics() {
        mockMultipleCountryMerchants();
        assertSuggestionsResponse("FR", "32R", "32Rougè");
    }

    @Test
    void shouldReturnAdjustedMerchantsForEmptySearchText() throws Exception {
        DsCounterpartiesAdjustedDTO adjusted = DsCounterpartiesAdjustedDTO.builder()
                .counterpartyNames(Arrays.asList("32Red", "365online.com"))
                .build();

        WireMock.stubFor(WireMock.get(urlPathMatching("/counterparties/users/" + clientUserToken.getUserIdClaim() + "/feedback/counterparty-names"))
                .willReturn(okJson(objectMapper.writeValueAsString(adjusted)))
        );

        assertSuggestionsResponse("UK", "", "32Red", "365online.com");
    }

    @Test
    void shouldReturnAdjustedMerchantsWhenCounterpartiesFail() {
        WireMock.stubFor(WireMock.get(urlPathMatching("/counterparties/users/" + clientUserToken.getUserIdClaim() + "/feedback/counterparty-names"))
                .willReturn(badRequest())
        );

        assertSuggestionsResponse("UK", "");
    }

    @Test
    void shouldGetMerchantsForSearchText() throws Exception {
        mockSingleCountryMerchant();
        final MerchantsController.MerchantSuggestionsDTO response = makeCallExpectingOK(MERCHANT, COUNTRY);
        assertMerchantSuggestionsDTO(response, MERCHANT);
    }

    @Test
    void shouldGetAdjustedCounterpartiesIfNoSearchText() throws Exception {
        DsCounterpartiesAdjustedDTO value = DsCounterpartiesAdjustedDTO.builder()
                .counterpartyNames(Collections.singletonList(MERCHANT))
                .build();

        WireMock.stubFor(WireMock.get(urlPathMatching("/counterparties/users/" + clientUserToken.getUserIdClaim() + "/feedback/counterparty-names"))
                .willReturn(okJson(objectMapper.writeValueAsString(value)))
        );

        final MerchantsController.MerchantSuggestionsDTO response = makeCallExpectingOK("", COUNTRY);
        assertMerchantSuggestionsDTO(response, MERCHANT);
    }

    @Test
    void shouldRejectRequestWithoutRequestParams() throws Exception {
        mockSingleCountryMerchant();
        makeCallExpectingBadRequest(null, null);
    }

    @Test
    void shouldRejectRequestWithoutSearchText() throws Exception {
        mockSingleCountryMerchant();
        makeCallExpectingBadRequest(null, COUNTRY);
    }

    @Test
    void shouldRejectRequestWithoutCountry() throws Exception {
        mockSingleCountryMerchant();
        makeCallExpectingBadRequest(MERCHANT, null);
    }

    @SneakyThrows
    private void mockMultipleCountryMerchants() {
        MerchantsInCountriesDTO value = MerchantsInCountriesDTO.builder().merchantsByCountries(Arrays.asList(
                MerchantsInCountriesDTO.MerchantsInCountryDTO.builder()
                        .country("UK")
                        .merchants(Arrays.asList("32Red", "365online.com", "Amazon.com", "Apple Pay", "G-Star Raw",
                                "Marie Curie Cancer Charity", "Marks & Spencer", "Marriott"))
                        .mappings(Collections.singletonList(MerchantsInCountriesDTO.MerchantsInCountryDTO.MappingDTO.builder().alternative("M & S").name("Marks & Spencer").build()))
                        .build(),
                MerchantsInCountriesDTO.MerchantsInCountryDTO.builder()
                        .country("FR")
                        .merchants(Collections.singletonList("32Rougè"))
                        .mappings(Collections.emptyList())
                        .build()))
                .build();

        WireMock.stubFor(WireMock.get(urlPathMatching("/counterparties/merchants-by-countries"))
                .willReturn(okJson(objectMapper.writeValueAsString(value)))
        );

        merchantsService.reloadMerchants();
    }

    private void mockSingleCountryMerchant() throws JsonProcessingException {
        MerchantsInCountriesDTO mer = MerchantsInCountriesDTO.builder().merchantsByCountries(Collections.singletonList(
                MerchantsInCountriesDTO.MerchantsInCountryDTO.builder()
                        .country(COUNTRY)
                        .merchants(Collections.singletonList(MERCHANT))
                        .mappings(Collections.emptyList())
                        .build()))
                .build();

        WireMock.stubFor(WireMock.get(urlPathMatching("/counterparties/merchants-by-countries"))
                .willReturn(okJson(objectMapper.writeValueAsString(mer)))
        );
        merchantsService.reloadMerchants();
    }

    private MerchantsController.MerchantSuggestionsDTO makeCallExpectingOK(String searchText, String country) throws Exception {
        MvcResult mvcResult = mockMvc.perform(get(buildMerchantSuggestionsUri(searchText, country))
                .header("user-id", clientUserToken.getUserIdClaim())
                .header("client-token", clientUserToken.getSerialized()))
                .andExpect(status().isOk())
                .andReturn();

        return objectMapper.readValue(mvcResult.getResponse().getContentAsByteArray(), MerchantsController.MerchantSuggestionsDTO.class);
    }

    private void makeCallExpectingBadRequest(String searchText, String country) throws Exception {
        mockMvc.perform(get(buildMerchantSuggestionsUri(searchText, country))
                .header("user-id", clientUserToken.getUserIdClaim())
                .header("client-token", clientUserToken.getSerialized()))
                .andExpect(status().isBadRequest());
    }

    private URI buildMerchantSuggestionsUri(String searchText, String country) {
        UriComponentsBuilder builder = UriComponentsBuilder.fromPath("/v1/users/{userId}/merchants/suggestions");
        if (country != null) {
            builder.queryParam("country", country);
        }
        if (searchText != null) {
            builder.queryParam("searchText", searchText);
        }
        return builder.buildAndExpand(userId).toUri();
    }

    private void assertMerchantSuggestionsDTO(final MerchantsController.MerchantSuggestionsDTO actualDTO, final String... expectedMerchants) {
        assertThat(actualDTO.getMerchantSuggestions()).hasSize(expectedMerchants.length);
        assertThat(actualDTO.getMerchantSuggestions()).containsExactlyInAnyOrder(expectedMerchants);
    }

    @SneakyThrows
    protected void assertSuggestionsResponse(final String country, final String searchText, final String... expectedMerchants) {
        mockMvc.perform(get("/v1/users/{userId}/merchants/suggestions", userId)
                .queryParam("country", country)
                .queryParam("searchText", searchText)
                .headers(getHeaders())
                .contentType(MediaType.APPLICATION_JSON))
                .andExpect(jsonPath("$.merchantSuggestions[*]").value(Matchers.containsInAnyOrder(expectedMerchants)));
    }

    private HttpHeaders getHeaders() {
        var httpHeaders = new HttpHeaders();
        httpHeaders.add("user-id", clientUserToken.getUserIdClaim().toString());
        httpHeaders.add("client-token", clientUserToken.getSerialized());
        return httpHeaders;
    }
}