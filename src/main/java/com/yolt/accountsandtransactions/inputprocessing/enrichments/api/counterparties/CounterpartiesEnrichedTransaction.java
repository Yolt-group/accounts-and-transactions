package com.yolt.accountsandtransactions.inputprocessing.enrichments.api.counterparties;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.yolt.accountsandtransactions.inputprocessing.enrichments.api.base.EnrichedTransaction;
import com.yolt.accountsandtransactions.inputprocessing.enrichments.api.base.EnrichedTransactionKey;
import lombok.Builder;
import lombok.Data;
import lombok.NonNull;
import org.springframework.lang.Nullable;

@Data
public class CounterpartiesEnrichedTransaction extends EnrichedTransaction {
    private final String counterparty;
    private final boolean merchant;
    @NonNull
    private final String counterpartySource;

    @Builder
    @JsonCreator
    public CounterpartiesEnrichedTransaction(
            @NonNull @JsonProperty("key") final EnrichedTransactionKey key,
            @Nullable @JsonProperty("counterparty") final String counterparty,
            @JsonProperty("isMerchant") final boolean merchant,
            @NonNull @JsonProperty("counterpartySource") final String counterpartySource) {
        super(key);
        this.counterparty = counterparty;
        this.merchant = merchant;
        this.counterpartySource = counterpartySource;
    }

    @Override
    public String toString() {
        return "CounterpartiesEnrichedTransaction";
    }
}
