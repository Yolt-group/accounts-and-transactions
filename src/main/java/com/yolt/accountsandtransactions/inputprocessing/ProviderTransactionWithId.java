package com.yolt.accountsandtransactions.inputprocessing;

import com.yolt.accountsandtransactions.transactions.Transaction.FillType;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NonNull;
import lombok.ToString;
import nl.ing.lovebird.providerdomain.ProviderTransactionDTO;

/**
 * Holds the computed {@link #transactionId} for a {@link #providerTransactionDTO}.
 * <p>
 * History: {@link #transactionId} has been dropped from {@link #providerTransactionDTO}
 */
@Getter
@ToString
@EqualsAndHashCode
public class ProviderTransactionWithId {

    @NonNull
    private final ProviderTransactionDTO providerTransactionDTO;
    @NonNull
    private final String transactionId;
    @NonNull
    private final FillType fillType;

    public ProviderTransactionWithId(@NonNull ProviderTransactionDTO providerTransactionDTO, @NonNull String transactionId) {
        this.providerTransactionDTO = providerTransactionDTO;
        this.transactionId = transactionId;
        this.fillType = FillType.REGULAR;
    }

    public ProviderTransactionWithId(@NonNull ProviderTransactionDTO providerTransactionDTO, @NonNull String transactionId, @NonNull FillType fillType) {
        this.providerTransactionDTO = providerTransactionDTO;
        this.transactionId = transactionId;
        this.fillType = fillType;
    }
}
