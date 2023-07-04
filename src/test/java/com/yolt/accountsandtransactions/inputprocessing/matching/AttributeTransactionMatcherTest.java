package com.yolt.accountsandtransactions.inputprocessing.matching;

import org.junit.jupiter.api.Test;

import static com.yolt.accountsandtransactions.inputprocessing.matching.AttributeTransactionMatcher.Unmatched.Reason.*;
import static org.assertj.core.api.Assertions.assertThat;

class AttributeTransactionMatcherTest {

    @Test
    void testIsEqualToOrLowerWeight() {
        assertThat(UNPROCESSED.hasEqualOrLowerWeightThan(UNPROCESSED)).isTrue();
        assertThat(UNPROCESSED.hasEqualOrLowerWeightThan(REJECTED)).isTrue();
        assertThat(UNPROCESSED.hasEqualOrLowerWeightThan(DUPLICATE)).isTrue();
        assertThat(UNPROCESSED.hasEqualOrLowerWeightThan(PEERLESS)).isTrue();

        assertThat(REJECTED.hasEqualOrLowerWeightThan(UNPROCESSED)).isFalse();
        assertThat(REJECTED.hasEqualOrLowerWeightThan(REJECTED)).isTrue();
        assertThat(REJECTED.hasEqualOrLowerWeightThan(DUPLICATE)).isTrue();
        assertThat(REJECTED.hasEqualOrLowerWeightThan(PEERLESS)).isTrue();

        assertThat(DUPLICATE.hasEqualOrLowerWeightThan(UNPROCESSED)).isFalse();
        assertThat(DUPLICATE.hasEqualOrLowerWeightThan(REJECTED)).isFalse();
        assertThat(DUPLICATE.hasEqualOrLowerWeightThan(DUPLICATE)).isTrue();
        assertThat(DUPLICATE.hasEqualOrLowerWeightThan(PEERLESS)).isTrue();

        assertThat(PEERLESS.hasEqualOrLowerWeightThan(UNPROCESSED)).isFalse();
        assertThat(PEERLESS.hasEqualOrLowerWeightThan(REJECTED)).isFalse();
        assertThat(PEERLESS.hasEqualOrLowerWeightThan(DUPLICATE)).isFalse();
        assertThat(PEERLESS.hasEqualOrLowerWeightThan(PEERLESS)).isTrue();
    }

    @Test
    void testIsHigherWeight() {
        assertThat(UNPROCESSED.hasHigherWeightThan(UNPROCESSED)).isFalse();
        assertThat(UNPROCESSED.hasHigherWeightThan(REJECTED)).isFalse();
        assertThat(UNPROCESSED.hasHigherWeightThan(DUPLICATE)).isFalse();
        assertThat(UNPROCESSED.hasHigherWeightThan(PEERLESS)).isFalse();

        assertThat(REJECTED.hasHigherWeightThan(UNPROCESSED)).isTrue();
        assertThat(REJECTED.hasHigherWeightThan(REJECTED)).isFalse();
        assertThat(REJECTED.hasHigherWeightThan(DUPLICATE)).isFalse();
        assertThat(REJECTED.hasHigherWeightThan(PEERLESS)).isFalse();

        assertThat(DUPLICATE.hasHigherWeightThan(UNPROCESSED)).isTrue();
        assertThat(DUPLICATE.hasHigherWeightThan(REJECTED)).isTrue();
        assertThat(DUPLICATE.hasHigherWeightThan(DUPLICATE)).isFalse();
        assertThat(DUPLICATE.hasHigherWeightThan(PEERLESS)).isFalse();

        assertThat(PEERLESS.hasHigherWeightThan(UNPROCESSED)).isTrue();
        assertThat(PEERLESS.hasHigherWeightThan(REJECTED)).isTrue();
        assertThat(PEERLESS.hasHigherWeightThan(DUPLICATE)).isTrue();
        assertThat(PEERLESS.hasHigherWeightThan(PEERLESS)).isFalse();
    }
}