package com.yolt.accountsandtransactions.datascience.counterparties.client.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.Builder;
import lombok.Value;

import java.util.List;

@Value
@Builder
@JsonIgnoreProperties(ignoreUnknown = true)
public class MerchantsInCountriesDTO {
    List<MerchantsInCountryDTO> merchantsByCountries;

    @Value
    @Builder
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class MerchantsInCountryDTO {
        String country;
        List<String> merchants;
        List<MerchantsInCountryDTO.MappingDTO> mappings;

        @Value
        @Builder
        @JsonIgnoreProperties(ignoreUnknown = true)
        public static class MappingDTO {
            String alternative;
            String name;
        }
    }
}
