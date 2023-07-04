package com.yolt.accountsandtransactions.inputprocessing.matching.decorators;

import com.yolt.accountsandtransactions.inputprocessing.matching.Attribute;
import com.yolt.accountsandtransactions.inputprocessing.matching.AttributeSelector;
import com.yolt.accountsandtransactions.inputprocessing.matching.AttributeTransactionMatcher;
import com.yolt.accountsandtransactions.inputprocessing.matching.GeneralizedTransaction;
import lombok.NonNull;

import java.time.Clock;
import java.time.Instant;
import java.util.Set;

/**
 * A selector which only marks an attribute usable after a certain date/time.
 */
public final class UseAfterOrNullAttributeSelectorDecorator<A> extends BaseAttributeSelectorDecorator<A> {

    private final Clock clock;
    private final Instant useAfter;

    public UseAfterOrNullAttributeSelectorDecorator(@NonNull AttributeSelector<A> selector, @NonNull Clock clock, @NonNull Instant useAfter) {
        super(selector);
        this.clock = clock;
        this.useAfter = useAfter;
    }

    @Override
    public AttributeTransactionMatcher.AttributeExtractor<A> attributeExtractor() {
        return selector.attributeExtractor();
    }

    @Override
    public Set<Result<A>> selectAttribute(GeneralizedTransaction transaction) {
        if (Instant.now(clock).isBefore(useAfter)) {
            // replace the attribute from the transaction with a null value
            // basically disabling the field instead of rejecting it.
            // we return usable = true to indicate that the attribute is still usable
            // to not fail the matching process by rejecting it (usable = false)
            var replacement = new Attribute<A>(attributeExtractor().extract(transaction).name, null);
            return Set.of(new Result<>(replacement, true));
        }

        return super.selectAttribute(transaction);
    }
}
