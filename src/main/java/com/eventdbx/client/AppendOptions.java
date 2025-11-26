package com.eventdbx.client;

import com.fasterxml.jackson.databind.JsonNode;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public final class AppendOptions {
    private final JsonNode payload;
    private final JsonNode metadata;
    private final String note;
    private final String token;
    private final List<PublishTarget> publishTargets;

    private AppendOptions(Builder builder) {
        this.payload = builder.payload;
        this.metadata = builder.metadata;
        this.note = builder.note;
        this.token = builder.token;
        this.publishTargets = List.copyOf(builder.publishTargets);
    }

    public Optional<JsonNode> payload() {
        return Optional.ofNullable(payload);
    }

    public Optional<JsonNode> metadata() {
        return Optional.ofNullable(metadata);
    }

    public Optional<String> note() {
        return Optional.ofNullable(note);
    }

    public Optional<String> token() {
        return Optional.ofNullable(token);
    }

    public List<PublishTarget> publishTargets() {
        return publishTargets;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {
        private JsonNode payload;
        private JsonNode metadata;
        private String note;
        private String token;
        private final List<PublishTarget> publishTargets = new ArrayList<>();

        public Builder payload(JsonNode payload) {
            this.payload = payload;
            return this;
        }

        public Builder metadata(JsonNode metadata) {
            this.metadata = metadata;
            return this;
        }

        public Builder note(String note) {
            this.note = note;
            return this;
        }

        public Builder token(String token) {
            this.token = token;
            return this;
        }

        public Builder publishTarget(String plugin) {
            if (plugin != null && !plugin.isBlank()) {
                this.publishTargets.add(PublishTarget.fromString(plugin));
            }
            return this;
        }

        public Builder publishTarget(PublishTarget target) {
            if (target != null) {
                this.publishTargets.add(target);
            }
            return this;
        }

        public Builder publishTargets(List<PublishTarget> targets) {
            this.publishTargets.clear();
            if (targets != null) {
                this.publishTargets.addAll(targets);
            }
            return this;
        }

        public AppendOptions build() {
            return new AppendOptions(this);
        }
    }
}
