package com.yolt.accountsandtransactions.batch;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

class Counters {
    final Map<String, Integer> counters = new HashMap<>();

    public void increment(String name) {
        counters.compute(name, (k, v) -> v == null ? 1 : v + 1);
    }

    public void increment(String name, int amount) {
        counters.compute(name, (k, v) -> v == null ? amount : v + amount);
    }

    public int get(String name) {
        return counters.getOrDefault(name, 0);
    }

    public void set(String name, int value) {
        counters.put(name, value);
    }

    /**
     * @return string representation of all counters
     */
    @Override
    public String toString() {
        return "(" + counters.entrySet().stream()
                .map(e -> e.getKey() + "=" + e.getValue())
                .collect(Collectors.joining(", ")) + ")";
    }

    public boolean isEmpty() {
        return counters.isEmpty();
    }
}
