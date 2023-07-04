package com.yolt.accountsandtransactions.inputprocessing.matching;

import lombok.EqualsAndHashCode;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.ToString;
import org.springframework.lang.Nullable;

/**
 * A structure which defines an {@link Attribute} of type <code>A</code> containing a {@link Attribute#name} and {@link Attribute#value}
 * <p/>
 * The {@link Attribute#name} represents the name of the attribute, for example, external-id, booking-date, etc.
 * while the {@link Attribute#value} contains the associated transaction value (booking-date -> '1970-01-01')
 * <p/>
 * In short, it represents a field extracted from an upstream or stored transaction.
 * <p/>
 * Note: The attribute type A *must* implement a proper equals and hashcode.
 * If this is not the case, the matching might go haywire.
 *
 * @param <A> the type of the attribute value
 */
@ToString
@EqualsAndHashCode
@RequiredArgsConstructor
public
class Attribute<A> {
    @NonNull
    public final String name;
    @Nullable
    public final A value;
}
