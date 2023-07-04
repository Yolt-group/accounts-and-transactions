package com.yolt.accountsandtransactions.datascience.counterparties;

import com.yolt.accountsandtransactions.datascience.counterparties.client.dto.MerchantsInCountriesDTO;
import com.yolt.accountsandtransactions.datascience.preprocessing.PreProcessingServiceClient;
import nl.ing.lovebird.clienttokens.ClientUserToken;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import reactor.core.publisher.Mono;

import java.util.List;

import static com.google.common.collect.Lists.newArrayList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class MerchantsServiceTest {
    private static final String UNITED_KINGDOM = "UK";
    private static final String GERMANY = "DE";

    @Mock
    private PreProcessingServiceClient preprocessingServiceClient;

    private MerchantsService merchantsService;

    @Mock
    private ClientUserToken clientUserToken;

    @BeforeEach
    public void setup() {
        merchantsService = new MerchantsService(preprocessingServiceClient);
    }

    @AfterEach
    public void tearDown() {
        verifyNoMoreInteractions(preprocessingServiceClient);
    }

    @Test
    public void shouldWorkWithCapitalsOnly() {
        assertResult(createMerchantsInCountries(), "MAR", UNITED_KINGDOM, "Marie Curie Cancer Charity", "Marks & Spencer", "Marriott");
    }

    @Test
    public void shouldWorkWithSmallLettersOnly() {
        assertResult(createMerchantsInCountries(), "mar", UNITED_KINGDOM, "Marie Curie Cancer Charity", "Marks & Spencer", "Marriott");
    }

    @Test
    public void shouldIgnoreDiacritics() {
        assertResult(createMerchantsInCountries(), "mär", UNITED_KINGDOM, "Marie Curie Cancer Charity", "Marks & Spencer", "Marriott");
    }

    @Test
    public void shouldReturnMerchantsStartingWithNumbers() {
        assertResult(createMerchantsInCountries(), "3", UNITED_KINGDOM, "32Red", "365online.com");
    }

    @Test
    public void shouldReturnMerchantsWithDashes() {
        assertResult(createMerchantsInCountries(), "G-Star", UNITED_KINGDOM, "G-Star Raw");
    }

    @Test
    public void shouldReturnMerchantsWithSpaces() {
        assertResult(createMerchantsInCountries(), "Apple Pay", UNITED_KINGDOM, "Apple Pay");
    }

    @Test
    public void shouldWorkWithNonBreakingSpaces() {
        assertResult(createMerchantsInCountries(), "non breaking", GERMANY, "non\u00a0breaking\u00a0spaces");
    }

    @Test
    public void shouldReturnMerchantsWithDots() {
        assertResult(createMerchantsInCountries(), "Amazon.", UNITED_KINGDOM, "Amazon.com");
    }

    @Test
    public void shouldReturnMerchantsWithSharpSForSearchTextWithS() {
        assertResult(createMerchantsInCountries(), "Wasserschlos", GERMANY, "Wasserschloß");
    }

    @Test
    public void shouldReturnMerchantsWithSharpSForSearchTextWithSharpS() {
        assertResult(createMerchantsInCountries(), "Wasserschloß", GERMANY, "Wasserschloß");
    }

    @Test
    public void shouldLimitNumberOfMerchants() {
        MerchantsInCountriesDTO merchantsInCountriesDTO = MerchantsInCountriesDTO.builder()
                .merchantsByCountries(
                        newArrayList(
                                MerchantsInCountriesDTO.MerchantsInCountryDTO.builder()
                                        .country(GERMANY)
                                        .merchants(
                                                newArrayList("AA", "AB", "AC", "AD", "AE", "AF", "AG", "AH", "AI", "AJ", "AK", "AL", "AM", "AM", "AO", "AP")
                                        )
                                        .mappings(
                                                newArrayList()
                                        )
                                        .build()
                        )
                ).build();
        assertResult(merchantsInCountriesDTO, "A", GERMANY, "AA", "AB", "AC", "AD", "AE", "AF", "AG", "AH", "AI", "AJ");
    }

    private MerchantsInCountriesDTO createMerchantsInCountries() {
        return MerchantsInCountriesDTO.builder()
                .merchantsByCountries(
                        newArrayList(
                                MerchantsInCountriesDTO.MerchantsInCountryDTO.builder()
                                        .country(UNITED_KINGDOM)
                                        .merchants(
                                                newArrayList("32Red", "365online.com", "Amazon.com", "Apple Pay", "G-Star Raw", "Marie Curie Cancer Charity", "Marks & Spencer", "Marriott")
                                        )
                                        .mappings(
                                                newArrayList(
                                                        MerchantsInCountriesDTO.MerchantsInCountryDTO.MappingDTO.builder()
                                                                .alternative("M & S")
                                                                .name("Marks & Spencer")
                                                                .build()
                                                )
                                        )
                                        .build(),
                                MerchantsInCountriesDTO.MerchantsInCountryDTO.builder()
                                        .country(GERMANY)
                                        .merchants(
                                                newArrayList("32Rot", "non\u00a0breaking\u00a0spaces", "Wasserschloß")
                                        )
                                        .mappings(
                                                newArrayList()
                                        )
                                        .build()
                        )
                ).build();
    }

    private void assertResult(final MerchantsInCountriesDTO merchantsInCountries, final String searchText, final String country, final String... expectedMerchants) {
        when(preprocessingServiceClient.getMerchantsByCountries()).thenReturn(Mono.just( merchantsInCountries));

        merchantsService.reloadMerchants();
        final List<String> result = merchantsService.searchMerchants(clientUserToken, searchText, country);

        assertThat(result).containsExactly(expectedMerchants);

        verify(preprocessingServiceClient).getMerchantsByCountries();
    }
}