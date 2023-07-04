package com.yolt.accountsandtransactions.inputprocessing.matching;

import com.yolt.accountsandtransactions.inputprocessing.BookingDateSyncWindowSelector;
import com.yolt.accountsandtransactions.inputprocessing.UnboundedSyncWindowSelector;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.yolt.accountsandtransactions.inputprocessing.matching.AttributeSelectors.*;

public class Matchers {

    public static final EqualityAttributeTransactionMatcher EXTERNAL_ID_AMOUNT_STRICT =
            new EqualityAttributeTransactionMatcher("ExternalIdAmountStrict", List.of(
                    notBlank(EXTERNAL_ID),
                    notNull(AMOUNT_IN_CENTS)
            ));

    public static final EqualityAttributeTransactionMatcher BOOKING_DATE_AMOUNT_STRICT =
            new EqualityAttributeTransactionMatcher("BookingDateAmountStrict", List.of(
                    notNull(BOOKING_DATE),
                    notNull(AMOUNT_IN_CENTS),
                    CREDITOR_NAME,
                    DEBTOR_NAME,
                    DESCRIPTION,
                    EXTERNAL_ID,
                    TIMESTAMP
            ));

    public static final EqualityAttributeTransactionMatcher DATE_AMOUNT_STRICT =
            new EqualityAttributeTransactionMatcher("DateAmountStrict", List.of(
                    notNull(DATE),
                    notNull(AMOUNT_IN_CENTS),
                    CREDITOR_NAME,
                    DEBTOR_NAME,
                    DESCRIPTION,
                    EXTERNAL_ID,
                    TIMESTAMP
            ));

    public static final EqualityAttributeTransactionMatcher OPEN_BANKING =
            new EqualityAttributeTransactionMatcher("OpenBanking", List.of(
                    notNull(AMOUNT_IN_CENTS),
                    notNull(BOOKING_DATE),
                    CREDITOR_NAME,
                    DEBTOR_NAME,
                    DESCRIPTION,
                    EXTERNAL_ID,
                    TIMESTAMP
            ));

    public static final Map<String, ProviderConfiguration> ACTIVATED_ATTR_MATCHERS = new HashMap<>();

    static {
        List.of("ASN_BANK", "BUNQ", "RABOBANK", "REGIO_BANK", "SNS_BANK")
                .forEach(provider -> ACTIVATED_ATTR_MATCHERS.put(provider,
                        ProviderConfiguration.builder()
                                .provider(provider)
                                .syncWindowSelector(new UnboundedSyncWindowSelector())
                                .matchers(List.of(
                                        EXTERNAL_ID_AMOUNT_STRICT
                                ))
                                .build()));

        List.of("TRIODOS_BANK_NL")
                .forEach(provider -> ACTIVATED_ATTR_MATCHERS.put(provider,
                        ProviderConfiguration.builder()
                                .provider(provider)
                                .syncWindowSelector(new UnboundedSyncWindowSelector())
                                .matchers(List.of(
                                        EXTERNAL_ID_AMOUNT_STRICT,
                                        BOOKING_DATE_AMOUNT_STRICT
                                ))
                                .build()));

        // See YCO-2035 for details on which problem this configuration solves. Once the selector has shown that it covers
        // the problem, we can use it for other banks as well.
        ACTIVATED_ATTR_MATCHERS.put("KNAB_BANK_NL", ProviderConfiguration.builder()
                .provider("KNAB_BANK_NL")
                .syncWindowSelector(new BookingDateSyncWindowSelector())
                .matchers(List.of(
                        EXTERNAL_ID_AMOUNT_STRICT,
                        BOOKING_DATE_AMOUNT_STRICT
                ))
                .build());

        // The ABN AMRO implementation details of the datetime and booking-date changed over time.
        // Previously both datetime and booking-date were populated with the booking-date(time)
        // At some point (end 2020) this got replaced but the execution datetime of the transaction.
        // If the transaction window overlaps with these changes (e.a. via re-consent) then
        // we are not able to match the upstream and the stored transactions.
        // To solve this problem we match on booking-date, date and timestamp individually
        // If this does not yield any matches we try to reconsile on the amount + non-temporal metadata
        // -- TEMPORARY DISABLED. The non-narrow matchers will cause duplicates in the upstream.
        List.of("ABN_AMRO")
                .forEach(provider -> ACTIVATED_ATTR_MATCHERS.put(provider,
                        ProviderConfiguration.builder()
                                .provider(provider)
                                .syncWindowSelector(new UnboundedSyncWindowSelector())
                                .matchers(List.of(
                                        EXTERNAL_ID_AMOUNT_STRICT,
                                        BOOKING_DATE_AMOUNT_STRICT,
                                        DATE_AMOUNT_STRICT
                                ))
                                .build()));

        // ING is known to shift execution date-times on transactions from foreign currency accounts.
        // We are adding a modified selector which does not take the timestamp into account
        // to resolve these cases.
        List.of("ING_NL")
                .forEach(provider -> ACTIVATED_ATTR_MATCHERS.put(provider,
                        ProviderConfiguration.builder()
                                .provider(provider)
                                .syncWindowSelector(new UnboundedSyncWindowSelector())
                                .matchers(List.of(
                                        EXTERNAL_ID_AMOUNT_STRICT,
                                        BOOKING_DATE_AMOUNT_STRICT,
                                        BOOKING_DATE_AMOUNT_STRICT.withoutSelector("noTs", TIMESTAMP)
                                ))
                                .build()));
    }

    public static boolean isActivatedAttributeMatcher(final String provider) {
        return ACTIVATED_ATTR_MATCHERS.containsKey(provider);
    }
}
