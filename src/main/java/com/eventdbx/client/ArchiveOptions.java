package com.eventdbx.client;

import java.util.Optional;

public final class ArchiveOptions {
    private final String note;
    private final String token;

    private ArchiveOptions(Builder builder) {
        this.note = builder.note;
        this.token = builder.token;
    }

    public Optional<String> note() {
        return Optional.ofNullable(note);
    }

    public Optional<String> token() {
        return Optional.ofNullable(token);
    }

    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {
        private String note;
        private String token;

        public Builder note(String note) {
            this.note = note;
            return this;
        }

        public Builder token(String token) {
            this.token = token;
            return this;
        }

        public ArchiveOptions build() {
            return new ArchiveOptions(this);
        }
    }
}
