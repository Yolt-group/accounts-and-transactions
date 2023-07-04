package com.yolt.accountsandtransactions.accounts;

import com.yolt.accountsandtransactions.BaseIntegrationTest;
import com.yolt.accountsandtransactions.TestBuilders;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.List;

import static java.util.UUID.randomUUID;
import static org.assertj.core.api.Assertions.assertThat;

public class AccountRepositoryTest extends BaseIntegrationTest {

    @Autowired
    AccountRepository accountRepository;

    @Test
    public void outputShouldEqualInput() {
        var userId = randomUUID();
        var accountId = randomUUID();

        var input = TestBuilders.createAllFieldsRandomAccount(userId, accountId);
        accountRepository.saveBatch(List.of(input), 1);

        var output = accountRepository.getAccounts(userId);
        assertThat(output).containsExactly(input);
    }
}