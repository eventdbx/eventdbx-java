package com.eventdbx.client;

import java.util.Objects;

public final class PublishTarget {
    private final String plugin;
    private final String mode;
    private final String priority;

    public PublishTarget(String plugin, String mode, String priority) {
        if (plugin == null || plugin.isBlank()) {
            throw new IllegalArgumentException("plugin is required for publish targets");
        }
        this.plugin = plugin;
        this.mode = mode;
        this.priority = priority;
    }

    public static PublishTarget of(String plugin) {
        return new PublishTarget(plugin, null, null);
    }

    public static PublishTarget fromString(String spec) {
        if (spec == null) {
            throw new IllegalArgumentException("publish target spec cannot be null");
        }
        String[] parts = spec.split(":", -1);
        String plugin = parts.length > 0 ? parts[0] : "";
        String mode = parts.length > 1 && !parts[1].isBlank() ? parts[1] : null;
        String priority = parts.length > 2 && !parts[2].isBlank() ? parts[2] : null;
        return new PublishTarget(plugin, mode, priority);
    }

    public String plugin() {
        return plugin;
    }

    public String mode() {
        return mode;
    }

    public String priority() {
        return priority;
    }
}
