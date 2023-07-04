package com.yolt.accountsandtransactions.inputprocessing.matching;

import com.yolt.accountsandtransactions.inputprocessing.matching.AttributeTransactionMatcher.AttributeExtractor;
import lombok.EqualsAndHashCode;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import org.springframework.util.Assert;

import java.util.Set;

/**
 * The {@link AttributeSelector} is responsible for determining if an {@link Attribute},
 * extracted via the <code>attributeExtractor</code>, can be used for transaction matching.
 * <p/>
 * The <code>selectAttribute</code> methods return a set of {@link Result}s
 * which provides the extracted {@link Attribute}s and the answer to the question
 * if this attribute can be used or not when matching transactions.
 *
 * @param <A> the of the attribute value
 */
public interface AttributeSelector<A> {

    /**
     * The {@link AttributeExtractor} this selector gets the {@link Attribute} from.
     */
    AttributeExtractor<A> attributeExtractor();

    /**
     * Test a {@link GeneralizedTransaction} to retrieve its attribute and to check if it can be used
     * in matching. The method returns a {@link Set} of {@link Result}s which contain the
     * attribute (ea. external-id -> '123') and a flag if this attribute can be used in matching.
     * <p/>
     * For example, if this selector implemented a restriction on the attribute value, such as that the
     * external-id should never be null, it will contain an attribute external-id -> null and an accepted flag of false
     * if the external-id  on the transactions was indeed null.
     * If it was not null it would return attribute external-id -> 'xyz', accepted = true.
     * <p/>
     * This method returns a {@link Set} which exists only for future extensibility and should always return
     * a singleton set.
     */
    Set<Result<A>> selectAttribute(final GeneralizedTransaction generalizedTransaction);

    @EqualsAndHashCode
    @RequiredArgsConstructor
    class Result<A> {
        @NonNull
        public final Attribute<A> attribute;
        public final boolean usable;

        static boolean isUsable(final Set<? extends Result<?>> selectorResults) {
            Assert.notEmpty(selectorResults, "The list of attribute selector results cannot be empty.");
            return selectorResults.stream()
                    .allMatch(selectorResult -> selectorResult.usable);
        }
    }

    /**
     * The {@link IdentityAttributeSelector} is a base implementation of the {@link AttributeSelector}.
     * <p/>
     * This selector takes the extracted attribute at face value (its identity) and by default allows this attribute to be used in the comparison.
     *
     * @param <A> the of the attribute value
     */
    @RequiredArgsConstructor
    class IdentityAttributeSelector<A> implements AttributeSelector<A> {

        protected final AttributeExtractor<A> attributeExtractor;

        @Override
        public AttributeExtractor<A> attributeExtractor() {
            return attributeExtractor;
        }

        @Override
        public Set<Result<A>> selectAttribute(GeneralizedTransaction transaction) {
            return Set.of(new Result<>(attributeExtractor().extract(transaction), true));
        }
    }
}
