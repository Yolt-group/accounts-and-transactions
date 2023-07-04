package com.yolt.accountsandtransactions.inputprocessing.matching;

import com.yolt.accountsandtransactions.inputprocessing.SyncWindowSelector;
import lombok.AccessLevel;
import lombok.Builder;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;

import java.util.List;

@Builder
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public class ProviderConfiguration {

    /**
     * The provider name
     */
    @NonNull
    public String provider;

    /**
     * This {@link SyncWindowSelector} to use for this provider
     */
    @NonNull
    public final SyncWindowSelector syncWindowSelector;

    /**
     * The set of {@link AttributeTransactionMatcher}s to use for this provider
     */
    @NonNull
    public final List<? extends AttributeTransactionMatcher> matchers;
}
