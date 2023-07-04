package com.yolt.accountsandtransactions.inputprocessing.matching.decorators;

import com.yolt.accountsandtransactions.inputprocessing.matching.AttributeSelector;
import com.yolt.accountsandtransactions.inputprocessing.matching.AttributeTransactionMatcher;
import com.yolt.accountsandtransactions.inputprocessing.matching.GeneralizedTransaction;

import java.util.Set;

/**
 * A selector decorator which only accepts transactions for which the attribute is not null;
 */
public final class NotNullAttributeSelectorDecorator<A> extends BaseAttributeSelectorDecorator<A> {

    public NotNullAttributeSelectorDecorator(AttributeSelector<A> selector) {
        super(selector);
    }

    @Override
    public AttributeTransactionMatcher.AttributeExtractor<A> attributeExtractor() {
        return selector.attributeExtractor();
    }

    @Override
    public Set<Result<A>> selectAttribute(GeneralizedTransaction transaction) {
        var attribute = attributeExtractor().extract(transaction);

        if (attribute.value != null) {
            return super.selectAttribute(transaction);
        }

        return Set.of(new Result<>(attribute, false));
    }
}
