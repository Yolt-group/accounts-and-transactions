package com.yolt.accountsandtransactions.inputprocessing.matching.decorators;

import com.yolt.accountsandtransactions.inputprocessing.matching.AttributeSelector;
import com.yolt.accountsandtransactions.inputprocessing.matching.GeneralizedTransaction;
import lombok.RequiredArgsConstructor;

import java.util.Set;

/**
 * Base decorator
 *
 * @param <A> the attribute type
 */
@RequiredArgsConstructor
public abstract class BaseAttributeSelectorDecorator<A> implements AttributeSelector<A> {

    protected final AttributeSelector<A> selector;

    @Override
    public Set<Result<A>> selectAttribute(GeneralizedTransaction transaction) {
        return selector.selectAttribute(transaction);
    }
}
