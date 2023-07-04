package com.yolt.accountsandtransactions.datascience;

import com.yolt.accountsandtransactions.ValidationException;
import lombok.Data;

import java.util.Arrays;
import java.util.List;
import java.util.Locale;

@Data
public class Country {
    private static final List<String> COUNTRIES = Arrays.asList(Locale.getISOCountries());

    private final String code;

    public Country(final String code) {
        if (!COUNTRIES.contains(code)) {
            throw new ValidationException(String.format("Invalid country code %s", code));
        }
        this.code = code;
    }
}
