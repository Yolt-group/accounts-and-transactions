package com.yolt.accountsandtransactions.datascience.counterparties;

import com.yolt.accountsandtransactions.datascience.counterparties.client.dto.MerchantsInCountriesDTO;
import com.yolt.accountsandtransactions.datascience.preprocessing.PreProcessingServiceClient;
import lombok.Builder;
import lombok.RequiredArgsConstructor;
import lombok.Value;
import nl.ing.lovebird.clienttokens.ClientUserToken;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.text.Normalizer;
import java.time.Duration;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.time.temporal.ChronoUnit.SECONDS;
import static java.util.Collections.emptyMap;
import static java.util.stream.Collectors.toMap;
import static org.springframework.util.StringUtils.isEmpty;

@Service
@RequiredArgsConstructor
public class MerchantsService {
    private static final int DEFAULT_TIMEOUT_IN_SECONDS = 10;

    private static final long FIVE_MINUTES_IN_MILLIS = 5 * 60 * 1000L;
    private static final int MAX_MERCHANT_COUNT = 10;

    private Map<String, TreeSet<MerchantEntry>> merchantsCountryMap = new HashMap<>();

    private final PreProcessingServiceClient preprocessingServiceClient;

    @Scheduled(fixedDelay = FIVE_MINUTES_IN_MILLIS)
    public void reloadMerchants() {
        getMerchants();
    }

    public List<String> searchMerchants(final ClientUserToken clientUserToken, final String searchText, final String country) {
        if (isEmpty(searchText)) {
            // When there is no search-text to filter on, get the list of counterparties this user has renamed to before.
            return preprocessingServiceClient.getAdjustedCounterpartiesIgnoreErrors(clientUserToken).getCounterpartyNames();
        }

        Set<String> matchedMerchants = new HashSet<>();

        final String searchTerm = generateSearchTerm(searchText);
        final int searchTermLength = searchTerm.length();

        for (MerchantEntry me : merchantsCountryMap.getOrDefault(country, new TreeSet<>())) {
            if (searchTermLength <= me.getSearchTerm().length()) {
                int comp = searchTerm.compareTo(me.getSearchTerm().substring(0, searchTermLength));

                if (comp < 0) {
                    break;
                } else if (comp == 0) {
                    matchedMerchants.add(me.getMerchantName());
                    if (matchedMerchants.size() >= MAX_MERCHANT_COUNT) {
                        break;
                    }
                }
            }
        }

        return matchedMerchants.stream().sorted().collect(Collectors.toList());
    }

    private void getMerchants() {
        merchantsCountryMap = preprocessingServiceClient.getMerchantsByCountries()
                .map(merchantsInCountries -> merchantsInCountries.getMerchantsByCountries().stream()
                        .collect(toMap(MerchantsInCountriesDTO.MerchantsInCountryDTO::getCountry, c -> {
                            Stream<MerchantEntry> merchantEntries = c.getMerchants().stream()
                                    .map(s -> MerchantEntry.builder()
                                            .searchTerm(generateSearchTerm(s))
                                            .merchantName(s)
                                            .build());

                            Stream<MerchantEntry> alternativeMerchantEntries = c.getMappings().stream()
                                    .map(m -> MerchantEntry.builder()
                                            .searchTerm(generateSearchTerm(m.getAlternative()))
                                            .merchantName(m.getName())
                                            .build());

                            List<MerchantEntry> entries = Stream.concat(merchantEntries, alternativeMerchantEntries)
                                    .collect(Collectors.toList());

                            TreeSet<MerchantEntry> sorted = new TreeSet<>(Comparator.comparing(MerchantEntry::getSearchTerm));
                            sorted.addAll(entries);
                            return sorted;
                        })))
                .blockOptional(Duration.of(DEFAULT_TIMEOUT_IN_SECONDS, SECONDS))
                .orElse(emptyMap());
    }

    private static String generateSearchTerm(String searchText) {
        if (searchText == null) {
            return "";
        }

        String normalized = Normalizer.normalize(searchText.toLowerCase(), Normalizer.Form.NFD);

        StringBuilder searchTerm = new StringBuilder();
        for (char c : normalized.toCharArray()) {
            if (c == 0xdf) {
                // German sharp s
                searchTerm.append("s");
            } else if (Character.isLetter(c) || Character.isDigit(c) || Character.getType(c) == Character.OTHER_PUNCTUATION) {
                searchTerm.append(c);
            } else if (Character.getType(c) == Character.SPACE_SEPARATOR) {
                searchTerm.append(" ");
            } else if (Character.getType(c) == Character.DASH_PUNCTUATION) {
                searchTerm.append("-");
            }
        }

        return searchTerm.toString();
    }

    @Value
    @Builder
    private static class MerchantEntry {
        String searchTerm;
        String merchantName;
    }
}
