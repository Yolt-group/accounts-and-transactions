package com.yolt.accountsandtransactions.inputprocessing;

import com.yolt.accountsandtransactions.inputprocessing.matching.GeneralizedTransaction;

import java.time.LocalDate;
import java.util.List;
import java.util.Optional;

/**
 * Transaction sync window selector that always selects an unbounded window.
 */
public class UnboundedSyncWindowSelector implements SyncWindowSelector {

    private static final SyncWindow UNBOUNDED = new SyncWindow() {

        @Override
        public Optional<LocalDate> getLowerBound() {
            return Optional.empty();
        }

        @Override
        public boolean isBounded() {
            return false;
        }

        @Override
        public boolean isUnbounded() {
            return true;
        }

        @Override
        public <U extends GeneralizedTransaction> List<U> truncate(List<U> transactions) {
            return transactions;
        }
    };

    @Override
    public SyncWindow selectWindow(List<GeneralizedTransaction.ProviderGeneralizedTransaction> upstream, List<GeneralizedTransaction.StoredGeneralizedTransaction> stored) {
        return UNBOUNDED;
    }
}
