package com.yolt.accountsandtransactions.legacyaccounts;

import com.yolt.accountsandtransactions.ValidationException;
import com.yolt.accountsandtransactions.accounts.Account;
import com.yolt.accountsandtransactions.accounts.AccountRepository;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.List;
import java.util.UUID;

import static com.yolt.accountsandtransactions.TestBuilders.createAllFieldsRandomAccount;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class LegacyAccountServiceTest {
    private static final UUID userId = UUID.randomUUID();
    private static final UUID accountId = UUID.randomUUID();

    @Mock
    AccountRepository accountRepository;

    @Mock
    LegacyAccountMapper legacyAccountMapper;

    @Mock
    LegacyAccountGroupsService legacyAccountGroupsService;

    @InjectMocks
    LegacyAccountService legacyAccountService;

    @Test
    void testNullAccountId() {
        final LegacyAccountDTO accountDTO = LegacyAccountDTO.builder().build();

        assertThatExceptionOfType(ValidationException.class)
                .isThrownBy(() -> legacyAccountService.updateAccountHiddenStatusForUser(userId, List.of(accountDTO)))
                .withMessage("Account ID missing");
    }

    @Test
    void testInvalidAccountId() {
        final Account account = createAllFieldsRandomAccount(userId, UUID.randomUUID());
        when(accountRepository.getAccounts(userId)).thenReturn(List.of(account));

        final LegacyAccountDTO accountDTO = LegacyAccountDTO.builder().id(accountId).hidden(false).build();

        assertThatExceptionOfType(ValidationException.class)
                .isThrownBy(() -> legacyAccountService.updateAccountHiddenStatusForUser(userId, List.of(accountDTO)))
                .withMessage("Invalid account ID " + accountId);
    }
}