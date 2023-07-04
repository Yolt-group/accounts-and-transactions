package com.yolt.accountsandtransactions.legacyaccounts;

import com.yolt.accountsandtransactions.accounts.Account;
import org.junit.jupiter.api.Test;

import java.util.UUID;

import static com.yolt.accountsandtransactions.TestBuilders.createAllFieldsRandomAccount;
import static org.assertj.core.api.Assertions.assertThat;

class LegacyAccountNumberDTOTest {
    private static final UUID userId = UUID.randomUUID();
    private static final UUID accountId = UUID.randomUUID();

    @Test
    void testIbanScheme() {
        Account account = createAllFieldsRandomAccount(userId, accountId);

        final LegacyAccountNumberDTO actual = LegacyAccountNumberDTO.from(account);

        assertThat(actual.getScheme()).isEqualTo(LegacyAccountNumberDTO.Scheme.IBAN);
        assertThat(actual.getIdentification()).isEqualTo("iban");
        assertThat(actual.getHolderName()).isEqualTo("account-holder");
    }

    @Test
    void testSortCodeScheme() {
        Account account = createAllFieldsRandomAccount(userId, accountId).toBuilder()
                .iban(null)
                .build();

        final LegacyAccountNumberDTO actual = LegacyAccountNumberDTO.from(account);

        assertThat(actual.getScheme()).isEqualTo(LegacyAccountNumberDTO.Scheme.SORTCODEACCOUNTNUMBER);
        assertThat(actual.getIdentification()).isEqualTo("sort-code");
        assertThat(actual.getHolderName()).isEqualTo("account-holder");
    }

    @Test
    void testNoScheme() {
        Account account = createAllFieldsRandomAccount(userId, accountId).toBuilder()
                .iban(null)
                .sortCodeAccountNumber(null)
                .build();

        final LegacyAccountNumberDTO actual = LegacyAccountNumberDTO.from(account);

        assertThat(actual).isNull();
    }
}