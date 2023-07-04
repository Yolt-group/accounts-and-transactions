package com.yolt.accountsandtransactions.inputprocessing.matching;

import com.yolt.accountsandtransactions.inputprocessing.matching.AttributeSelector.IdentityAttributeSelector;
import com.yolt.accountsandtransactions.inputprocessing.matching.decorators.NotBlankAttributeSelectorDecorator;
import com.yolt.accountsandtransactions.inputprocessing.matching.decorators.NotNullAttributeSelectorDecorator;
import com.yolt.accountsandtransactions.inputprocessing.matching.decorators.NotPlaceholderAttributeSelectorDecorator;

import java.time.Instant;
import java.time.LocalDate;

/**
 * {@link AttributeSelector} companion object.
 */
public class AttributeSelectors {

    public static <A> AttributeSelector<A> notNull(AttributeSelector<A> selector) {
        return new NotNullAttributeSelectorDecorator<>(selector);
    }

    public static AttributeSelector<String> notBlank(AttributeSelector<String> selector) {
        return new NotBlankAttributeSelectorDecorator(selector);
    }

    public static AttributeSelector<String> notPlaceholder(AttributeSelector<String> selector) {
        return new NotPlaceholderAttributeSelectorDecorator(selector);
    }

    public static final IdentityAttributeSelector<String> EXTERNAL_ID
            = new IdentityAttributeSelector<>(transaction -> new Attribute<>("external-id", transaction.getExternalId()));

    public static final IdentityAttributeSelector<String> END_TO_END_ID
            = new IdentityAttributeSelector<>(transaction -> new Attribute<>("end-to-end-id", transaction.getEndToEndId()));

    public static final IdentityAttributeSelector<Long> AMOUNT_IN_CENTS
            = new IdentityAttributeSelector<>(transaction -> new Attribute<>("amount", transaction.getAmountInCents()));

    public static final IdentityAttributeSelector<LocalDate> DATE
            = new IdentityAttributeSelector<>(transaction -> new Attribute<>("date", transaction.getDate()));

    public static final IdentityAttributeSelector<LocalDate> BOOKING_DATE
            = new IdentityAttributeSelector<>(transaction -> new Attribute<>("booking-date", transaction.getBookingDate()));

    public static final IdentityAttributeSelector<Instant> TIMESTAMP
            = new IdentityAttributeSelector<>(transaction -> new Attribute<>("timestamp", transaction.getTimestamp()));

    public static final IdentityAttributeSelector<String> CREDITOR_NAME
            = new IdentityAttributeSelector<>(transaction -> new Attribute<>("creditor-name", transaction.getCreditorName()));

    public static final IdentityAttributeSelector<String> CREDITOR_ACCOUNT_NR
            = new IdentityAttributeSelector<>(transaction -> new Attribute<>("creditor-account-nr", transaction.getCreditorAccountNr()));

    public static final IdentityAttributeSelector<String> DEBTOR_NAME
            = new IdentityAttributeSelector<>(transaction -> new Attribute<>("debtor-name", transaction.getDebtorName()));

    public static final IdentityAttributeSelector<String> DEBTOR_ACCOUNT_NR
            = new IdentityAttributeSelector<>(transaction -> new Attribute<>("debtor-account-nr", transaction.getDebtorAccountNr()));

    public static final IdentityAttributeSelector<String> DESCRIPTION
            = new IdentityAttributeSelector<>(transaction -> new Attribute<>("description", transaction.getDescription()));
}
