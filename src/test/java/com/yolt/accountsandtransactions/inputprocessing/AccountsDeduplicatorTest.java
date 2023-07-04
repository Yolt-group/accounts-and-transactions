package com.yolt.accountsandtransactions.inputprocessing;

import nl.ing.lovebird.extendeddata.common.CurrencyCode;
import nl.ing.lovebird.providerdomain.ProviderAccountNumberDTO;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;

import static com.yolt.accountsandtransactions.inputprocessing.AccountsDeduplicator.deduplicateAccounts;
import static org.assertj.core.api.Assertions.assertThat;

public class AccountsDeduplicatorTest {

    @Test
    public void whenUpstreamAccountsNull_returnNull() {
        assertThat(deduplicateAccounts(null)).isNull();
    }

    @Test
    public void whenUpstreamAccountsEmpty_returnEmpty() {
        assertThat(deduplicateAccounts(Collections.emptyList())).isEmpty();
    }

    @Test
    public void whenUpstreamAccountsSingleAccount_returnThat() {
        AccountFromProviders account = prepareAccount("id",
                new ProviderAccountNumberDTO(ProviderAccountNumberDTO.Scheme.IBAN, "identification"),
                CurrencyCode.EUR);

        assertThat(deduplicateAccounts(Collections.singletonList(account))).isEqualTo(Collections.singletonList(account));
    }

    @Test
    public void whenUpstreamAccountsDoubleDifferentId_returnBoth() {
        AccountFromProviders left = prepareAccount("id1",
                new ProviderAccountNumberDTO(ProviderAccountNumberDTO.Scheme.IBAN, "identification"),
                CurrencyCode.EUR);
        AccountFromProviders right = prepareAccount("id2",
                new ProviderAccountNumberDTO(ProviderAccountNumberDTO.Scheme.IBAN, "identification"),
                CurrencyCode.EUR);

        assertThat(deduplicateAccounts(Arrays.asList(left, right)))
                .containsExactlyInAnyOrder(left, right);
    }

    @Test
    public void whenUpstreamAccountsDoubleDifferentSchema_returnBoth() {
        AccountFromProviders left = prepareAccount("id",
                new ProviderAccountNumberDTO(ProviderAccountNumberDTO.Scheme.IBAN, "identification"),
                CurrencyCode.EUR);
        AccountFromProviders right = prepareAccount("id",
                new ProviderAccountNumberDTO(ProviderAccountNumberDTO.Scheme.SORTCODEACCOUNTNUMBER, "identification"),
                CurrencyCode.EUR);

        assertThat(deduplicateAccounts(Arrays.asList(left, right)))
                .containsExactlyInAnyOrder(left, right);
    }

    @Test
    public void whenUpstreamAccountsDoubleDifferentIdentity_returnBoth() {
        AccountFromProviders left = prepareAccount("id",
                new ProviderAccountNumberDTO(ProviderAccountNumberDTO.Scheme.IBAN, "identification1"),
                CurrencyCode.EUR);
        AccountFromProviders right = prepareAccount("id",
                new ProviderAccountNumberDTO(ProviderAccountNumberDTO.Scheme.IBAN, "identification2"),
                CurrencyCode.EUR);

        assertThat(deduplicateAccounts(Arrays.asList(left, right)))
                .containsExactlyInAnyOrder(left, right);
    }

    @Test
    public void whenUpstreamAccountsDoubleDifferentCurrency_returnBoth() {
        AccountFromProviders left = prepareAccount("id",
                new ProviderAccountNumberDTO(ProviderAccountNumberDTO.Scheme.IBAN, "identification"),
                CurrencyCode.EUR);
        AccountFromProviders right = prepareAccount("id",
                new ProviderAccountNumberDTO(ProviderAccountNumberDTO.Scheme.IBAN, "identification"),
                CurrencyCode.GBP);

        assertThat(deduplicateAccounts(Arrays.asList(left, right)))
                .containsExactlyInAnyOrder(left, right);
    }

    @Test
    public void whenUpstreamAccountsDoubleDuplicates_returnOne() {
        AccountFromProviders left = prepareAccount("id",
                new ProviderAccountNumberDTO(ProviderAccountNumberDTO.Scheme.IBAN, "identification"),
                CurrencyCode.EUR);
        AccountFromProviders right = prepareAccount("id",
                new ProviderAccountNumberDTO(ProviderAccountNumberDTO.Scheme.IBAN, "identification"),
                CurrencyCode.GBP);

        assertThat(deduplicateAccounts(Arrays.asList(left, right)))
                .containsExactlyInAnyOrder(left, right);
    }

    @Test
    public void whenUpstreamAccountsMultipleDuplicates_dedupThem() {
        AccountFromProviders a1 = prepareAccount("id1",
                new ProviderAccountNumberDTO(ProviderAccountNumberDTO.Scheme.IBAN, "identification"),
                CurrencyCode.EUR);

        AccountFromProviders b1 = prepareAccount("id2",
                new ProviderAccountNumberDTO(ProviderAccountNumberDTO.Scheme.IBAN, "identification"),
                CurrencyCode.EUR);

        AccountFromProviders a2 = prepareAccount("id1",
                new ProviderAccountNumberDTO(ProviderAccountNumberDTO.Scheme.IBAN, "identification"),
                CurrencyCode.EUR);

        AccountFromProviders c1 = prepareAccount("id3",
                new ProviderAccountNumberDTO(ProviderAccountNumberDTO.Scheme.IBAN, "identification"),
                CurrencyCode.EUR);

        AccountFromProviders c2 = prepareAccount("id3",
                new ProviderAccountNumberDTO(ProviderAccountNumberDTO.Scheme.IBAN, "identification"),
                CurrencyCode.EUR);

        assertThat(deduplicateAccounts(Arrays.asList(a1, b1, a2, c1, c2)))
                .containsExactlyInAnyOrder(a1, b1, c1);
    }

    private static AccountFromProviders prepareAccount(String id, ProviderAccountNumberDTO number, CurrencyCode currencyCode) {
        return AccountFromProviders.accountsFromProvidersBuilder()
                .accountId(id)
                .accountNumber(number)
                .currency(currencyCode)
                .build();
    }
}
