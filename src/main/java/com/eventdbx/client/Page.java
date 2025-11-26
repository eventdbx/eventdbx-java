package com.eventdbx.client;

import java.util.List;
import java.util.Objects;

public final class Page<T> {
    private final List<T> items;
    private final String nextCursor;

    public Page(List<T> items, String nextCursor) {
        this.items = List.copyOf(Objects.requireNonNull(items, "items"));
        this.nextCursor = nextCursor;
    }

    public List<T> items() {
        return items;
    }

    public String nextCursor() {
        return nextCursor;
    }
}
