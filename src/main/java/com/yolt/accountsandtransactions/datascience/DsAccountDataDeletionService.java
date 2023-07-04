package com.yolt.accountsandtransactions.datascience;

import com.yolt.accountsandtransactions.datascience.cycles.DsTransactionCyclesClient;
import com.yolt.accountsandtransactions.datascience.preprocessing.PreProcessingServiceClient;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.lang.NonNull;
import org.springframework.stereotype.Service;

import javax.validation.constraints.NotNull;
import java.util.List;
import java.util.UUID;

@Service
@RequiredArgsConstructor
@Slf4j
public class DsAccountDataDeletionService {
    private final PreProcessingServiceClient preprocessingServiceClient;
    private final DsTransactionCyclesClient transactionCyclesClient;

    public void deleteAccountData(@NonNull final UUID userId,
                                  @NotNull final List<UUID> accountIds) {
        accountIds.forEach(accountId -> {
            log.info("Requesting to delete data for account {} in datascience", accountId);

            preprocessingServiceClient.deleteAccountData(userId, accountId);
            transactionCyclesClient.deleteAccountData(userId, accountId);
        });
    }
}
