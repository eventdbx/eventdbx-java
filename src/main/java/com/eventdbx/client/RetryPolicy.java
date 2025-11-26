package com.eventdbx.client;

import java.time.Duration;
import java.util.Objects;

/**
 * Simple exponential backoff policy matching the EventDBX JS client defaults.
 */
public final class RetryPolicy {
    private final int maxAttempts;
    private final Duration initialDelay;
    private final Duration maxDelay;

    private RetryPolicy(Builder builder) {
        this.maxAttempts = builder.maxAttempts;
        this.initialDelay = builder.initialDelay;
        this.maxDelay = builder.maxDelay.compareTo(builder.initialDelay) < 0 ? builder.initialDelay : builder.maxDelay;
    }

    public static RetryPolicy defaultPolicy() {
        return builder().build();
    }

    public static Builder builder() {
        return new Builder();
    }

    public int maxAttempts() {
        return maxAttempts;
    }

    public Duration initialDelay() {
        return initialDelay;
    }

    public Duration maxDelay() {
        return maxDelay;
    }

    /**
     * Calculate the backoff delay for a given attempt number (1-indexed).
     */
    public Duration delayForAttempt(int attempt) {
        if (attempt <= 1 || initialDelay.isZero() || initialDelay.isNegative()) {
            return Duration.ZERO;
        }
        int exponent = Math.max(0, attempt - 2);
        long scaled = initialDelay.toMillis() * (1L << Math.min(exponent, 31));
        long clamped = Math.min(scaled, maxDelay.toMillis());
        return Duration.ofMillis(clamped);
    }

    public static final class Builder {
        private int maxAttempts = 1;
        private Duration initialDelay = Duration.ofMillis(50);
        private Duration maxDelay = Duration.ofMillis(1000);

        public Builder maxAttempts(int maxAttempts) {
            this.maxAttempts = Math.max(1, maxAttempts);
            return this;
        }

        public Builder initialDelay(Duration initialDelay) {
            this.initialDelay = Objects.requireNonNull(initialDelay, "initialDelay");
            return this;
        }

        public Builder maxDelay(Duration maxDelay) {
            this.maxDelay = Objects.requireNonNull(maxDelay, "maxDelay");
            return this;
        }

        public RetryPolicy build() {
            Objects.requireNonNull(initialDelay, "initialDelay");
            Objects.requireNonNull(maxDelay, "maxDelay");
            return new RetryPolicy(this);
        }
    }
}
