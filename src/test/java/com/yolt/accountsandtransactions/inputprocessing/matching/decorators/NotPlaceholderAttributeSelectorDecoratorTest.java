package com.yolt.accountsandtransactions.inputprocessing.matching.decorators;

import com.yolt.accountsandtransactions.inputprocessing.matching.AttributeSelectors;
import com.yolt.accountsandtransactions.inputprocessing.matching.GeneralizedTransaction;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.ZonedDateTime;

import static com.yolt.accountsandtransactions.TestBuilders.transactionWithExternalIdAmountAndDate;
import static org.assertj.core.api.Assertions.assertThat;

class NotPlaceholderAttributeSelectorDecoratorTest {

    @Test
    void shouldNotAcceptSelectorIfAttributeValueIsPlaceholder() {
        var now = ZonedDateTime.now();
        var upstreamTransaction = transactionWithExternalIdAmountAndDate("Not Provided", new BigDecimal("100"), now);

        var results =
                new NotPlaceholderAttributeSelectorDecorator(AttributeSelectors.EXTERNAL_ID).selectAttribute(GeneralizedTransaction.toGeneralized(upstreamTransaction));

        assertThat(results)
                .allSatisfy(result -> assertThat(result.usable).isFalse());
    }

    @Test
    void shouldAcceptSelectorIfAttributeValueIsNotPlaceholder() {
        var now = ZonedDateTime.now();
        var upstreamTransaction = transactionWithExternalIdAmountAndDate("a", new BigDecimal("100"), now);

        var results =
                new NotPlaceholderAttributeSelectorDecorator(AttributeSelectors.EXTERNAL_ID).selectAttribute(GeneralizedTransaction.toGeneralized(upstreamTransaction));

        assertThat(results)
                .allSatisfy(result -> assertThat(result.usable).isTrue());
    }

}