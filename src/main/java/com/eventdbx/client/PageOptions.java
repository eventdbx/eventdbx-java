package com.eventdbx.client;

import java.util.Optional;

public final class PageOptions {
    private final String cursor;
    private final Integer take;
    private final boolean includeArchived;
    private final boolean archivedOnly;
    private final String token;
    private final String filter;
    private final String sort;

    private PageOptions(Builder builder) {
        this.cursor = builder.cursor;
        this.take = builder.take;
        this.includeArchived = builder.includeArchived;
        this.archivedOnly = builder.archivedOnly;
        this.token = builder.token;
        this.filter = builder.filter;
        this.sort = builder.sort;
    }

    public Optional<String> cursor() {
        return Optional.ofNullable(cursor);
    }

    public Optional<Integer> take() {
        return Optional.ofNullable(take);
    }

    public boolean includeArchived() {
        return includeArchived || archivedOnly;
    }

    public boolean archivedOnly() {
        return archivedOnly;
    }

    public Optional<String> token() {
        return Optional.ofNullable(token);
    }

    public Optional<String> filter() {
        return Optional.ofNullable(filter);
    }

    public Optional<String> sort() {
        return Optional.ofNullable(sort);
    }

    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {
        private String cursor;
        private Integer take;
        private boolean includeArchived;
        private boolean archivedOnly;
        private String token;
        private String filter;
        private String sort;

        public Builder cursor(String cursor) {
            this.cursor = cursor;
            return this;
        }

        public Builder take(Integer take) {
            this.take = take;
            return this;
        }

        public Builder includeArchived(boolean includeArchived) {
            this.includeArchived = includeArchived;
            return this;
        }

        public Builder archivedOnly(boolean archivedOnly) {
            this.archivedOnly = archivedOnly;
            return this;
        }

        public Builder token(String token) {
            this.token = token;
            return this;
        }

        public Builder filter(String filter) {
            this.filter = filter;
            return this;
        }

        public Builder sort(String sort) {
            this.sort = sort;
            return this;
        }

        public PageOptions build() {
            return new PageOptions(this);
        }
    }
}
