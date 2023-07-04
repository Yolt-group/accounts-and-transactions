package com.yolt.accountsandtransactions.datascience;

import com.yolt.accountsandtransactions.ValidationException;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class CountryTest {

    @Test
    public void testEqualsAndHashcode() {
        assertThat(new Country("GB")).isEqualTo(new Country("GB"));
    }

    @Test
    public void testInvalidCountryCode() {
        assertThatThrownBy(() -> new Country("foobar"))
            .isInstanceOf(ValidationException.class).hasMessageContaining("Invalid country code foobar");
    }
}
