package com.yolt.accountsandtransactions.inputprocessing.matching.decorators;

import com.yolt.accountsandtransactions.inputprocessing.matching.AttributeSelector;
import com.yolt.accountsandtransactions.inputprocessing.matching.AttributeTransactionMatcher;
import com.yolt.accountsandtransactions.inputprocessing.matching.GeneralizedTransaction;
import org.apache.commons.lang3.StringUtils;

import java.util.Set;

/**
 * A selector decorator which only accepts transactions for which the attribute
 * is not equal to {@link NotPlaceholderAttributeSelectorDecorator#PLACEHOLDER}
 */
public final class NotPlaceholderAttributeSelectorDecorator extends BaseAttributeSelectorDecorator<String> {

    public static final String PLACEHOLDER = "Not Provided";

    public NotPlaceholderAttributeSelectorDecorator(AttributeSelector<String> selector) {
        super(selector);
    }

    @Override
    public AttributeTransactionMatcher.AttributeExtractor<String> attributeExtractor() {
        return selector.attributeExtractor();
    }


    @Override
    public Set<Result<String>> selectAttribute(GeneralizedTransaction transaction) {
        var attribute = attributeExtractor().extract(transaction);

        if (StringUtils.isNotBlank(attribute.value) && attribute.value.equalsIgnoreCase(PLACEHOLDER)) {
            return Set.of(new Result<>(attribute, false));
        }
        return super.selectAttribute(transaction);
    }
}