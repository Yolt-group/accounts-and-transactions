package com.yolt.accountsandtransactions.legacyaccounts;

import com.yolt.accountsandtransactions.accounts.Account;
import com.yolt.accountsandtransactions.accounts.Balance;
import nl.ing.lovebird.extendeddata.account.BalanceType;
import nl.ing.lovebird.extendeddata.common.CurrencyCode;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.List;
import java.util.UUID;

import static com.yolt.accountsandtransactions.TestBuilders.createAllFieldsRandomAccount;
import static org.assertj.core.api.Assertions.assertThat;

class LegacyAccountMapperTest {

    @Test
    void testExtendedAccountWithNullValues() {
        final UUID userId = UUID.randomUUID();
        final UUID accountId = UUID.randomUUID();
        final Account account = createAllFieldsRandomAccount(userId, accountId).toBuilder()
                .iban(null)
                .maskedPan(null)
                .pan(null)
                .bban(null)
                .sortCodeAccountNumber(null)
                .balances(List.of(new Balance(
                        BalanceType.INTERIM_BOOKED,
                        CurrencyCode.EUR,
                        BigDecimal.TEN,
                        null
                )))
                .build();

        final LegacyAccountMapper mapper = new LegacyAccountMapper();
        final LegacyAccountDTO actual = mapper.fromAccount(account);

        assertThat(actual.getExtendedAccount().getAccountReferences()).isEmpty();
        assertThat(actual.getExtendedAccount().getBalances()).hasSize(1);
        assertThat(actual.getExtendedAccount().getBalances().get(0).getReferenceDate()).isNull();
    }

}