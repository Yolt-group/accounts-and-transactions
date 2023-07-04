package com.yolt.accountsandtransactions.inputprocessing;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Value;

import java.util.ArrayList;
import java.util.List;

public class DurationLogger {
    private long lastSample = System.currentTimeMillis();

    @Getter
    private List<DurationEntry> entries = new ArrayList<>();

    void addEntry(final String name) {
        long now = System.currentTimeMillis();
        entries.add(new DurationEntry(name, now - lastSample));
        lastSample = now;
    }

    @Value
    @RequiredArgsConstructor
    public static class DurationEntry {
        String name;
        long duration;
    }
}
