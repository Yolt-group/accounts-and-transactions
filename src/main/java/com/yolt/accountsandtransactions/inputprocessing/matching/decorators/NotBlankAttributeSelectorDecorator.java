package com.yolt.accountsandtransactions.inputprocessing.matching.decorators;

import com.yolt.accountsandtransactions.inputprocessing.matching.AttributeSelector;
import com.yolt.accountsandtransactions.inputprocessing.matching.AttributeTransactionMatcher;
import com.yolt.accountsandtransactions.inputprocessing.matching.GeneralizedTransaction;
import org.apache.commons.lang3.StringUtils;

import java.util.Set;

/**
 * A selector decorator which only accepts transactions for which the attribute is not null and not blank.
 */
public final class NotBlankAttributeSelectorDecorator extends BaseAttributeSelectorDecorator<String> {

    public NotBlankAttributeSelectorDecorator(AttributeSelector<String> selector) {
        super(selector);
    }

    @Override
    public AttributeTransactionMatcher.AttributeExtractor<String> attributeExtractor() {
        return selector.attributeExtractor();
    }

    @Override
    public Set<Result<String>> selectAttribute(GeneralizedTransaction transaction) {
        var attribute = attributeExtractor().extract(transaction);

        if (StringUtils.isNotBlank(attribute.value)) {
            return super.selectAttribute(transaction);
        }

        return Set.of(new Result<>(attribute, false));
    }
}
