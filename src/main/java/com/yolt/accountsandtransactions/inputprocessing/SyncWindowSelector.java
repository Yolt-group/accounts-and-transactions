package com.yolt.accountsandtransactions.inputprocessing;

import com.yolt.accountsandtransactions.inputprocessing.matching.GeneralizedTransaction;

import java.time.LocalDate;
import java.util.List;
import java.util.Optional;

/**
 * Selector for window boundaries for transaction sync.
 */
public interface SyncWindowSelector {

    /**
     * Selects a limit for the matching window for both the upstream and stored transactions. This window will be chosen
     * such that only full days (days for which we [think we] have all transactions in both lists) are included in the
     * window, except for new upstream transactions.
     * <p>
     * Whenever such window cannot be determined, the result will be unbounded.
     * <p>
     * This method does **NOT** guarantee that the upstream and stored transactions match. There may still be
     * transactions missing from either list on days we don't expect them to. This indicates a problem upstream or with
     * our storage. This is the responsibility of the matcher to pick up and report.
     *
     * @param upstream the generalized upstream transactions from providers
     * @param stored   the generalized stored transactions from the database
     * @return the selected window, may be unbounded
     */
    SyncWindow selectWindow(List<GeneralizedTransaction.ProviderGeneralizedTransaction> upstream, List<GeneralizedTransaction.StoredGeneralizedTransaction> stored);

    /**
     * Representation of a (limited) window.
     * <p>
     * A window can be bounded or unbounded. An unbounded window means that no selection could be made. This usually
     * indicates an issue with the input data.
     */
    interface SyncWindow {

        Optional<LocalDate> getLowerBound();

        /**
         * @return whether the window is bounded
         */
        default boolean isBounded() {
            return !isUnbounded();
        }

        /**
         * @return whether no window could be selected
         */
        boolean isUnbounded();

        /**
         * Truncates the provided list of transactions to the bounds of this window. Returns the original list when the
         * window is unbounded.
         *
         * @param transactions list of transactions to truncate
         * @param <U>          type of generalized transaction
         * @return list of transactions truncated to this window
         */
        <U extends GeneralizedTransaction> List<U> truncate(List<U> transactions);
    }
}
