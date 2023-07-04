package com.yolt.accountsandtransactions.validation;

import org.hibernate.validator.constraints.CompositionType;
import org.hibernate.validator.constraints.ConstraintComposition;

import javax.validation.Constraint;
import javax.validation.constraints.Negative;
import javax.validation.constraints.Positive;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@ConstraintComposition(CompositionType.OR)
@Negative
@Positive
@Target({ElementType.TYPE, ElementType.FIELD, ElementType.PARAMETER})
@Retention(RetentionPolicy.RUNTIME)
@Constraint(validatedBy = {})

public @interface NotZero {
    String message() default "The value cannot be zero.";

    Class<?>[] groups() default {};

    Class<?>[] payload() default {};
}
