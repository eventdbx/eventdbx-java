package com.eventdbx.client;

import java.time.Duration;
import java.util.Objects;

/**
 * Client configuration for the EventDBX control socket (Noise + Cap'n Proto).
 */
public final class EventDbxConfig {
    private final String host;
    private final int port;
    private final Duration connectTimeout;
    private final Duration requestTimeout;
    private final String token;
    private final String tenantId;
    private final boolean verbose;
    private final boolean noNoise;
    private final RetryPolicy retryPolicy;

    private EventDbxConfig(Builder builder) {
        this.host = builder.host;
        this.port = builder.port;
        this.connectTimeout = builder.connectTimeout;
        this.requestTimeout = builder.requestTimeout;
        this.token = builder.token;
        this.tenantId = builder.tenantId;
        this.verbose = builder.verbose;
        this.noNoise = builder.noNoise;
        this.retryPolicy = builder.retryPolicy;
    }

    public String host() {
        return host;
    }

    public int port() {
        return port;
    }

    public Duration connectTimeout() {
        return connectTimeout;
    }

    public Duration requestTimeout() {
        return requestTimeout;
    }

    public String token() {
        return token;
    }

    public String tenantId() {
        return tenantId;
    }

    public boolean verbose() {
        return verbose;
    }

    public boolean noNoise() {
        return noNoise;
    }

    public RetryPolicy retryPolicy() {
        return retryPolicy;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {
        private String host = defaultHost();
        private int port = defaultPort();
        private String token = defaultToken();
        private String tenantId = defaultTenantId();
        private boolean verbose = defaultVerbose();
        private boolean noNoise = defaultNoNoise();
        private Duration connectTimeout = Duration.ofSeconds(3);
        private Duration requestTimeout = Duration.ofSeconds(10);
        private RetryPolicy retryPolicy = RetryPolicy.defaultPolicy();

        private static String defaultHost() {
            return System.getenv().getOrDefault("EVENTDBX_HOST", "127.0.0.1");
        }

        private static int defaultPort() {
            try {
                String env = System.getenv("EVENTDBX_PORT");
                if (env != null && !env.isBlank()) {
                    return Integer.parseInt(env.trim());
                }
            } catch (NumberFormatException ignored) {
            }
            return 6363;
        }

        private static String defaultToken() {
            return System.getenv().getOrDefault("EVENTDBX_TOKEN", "");
        }

        private static String defaultTenantId() {
            return System.getenv().getOrDefault("EVENTDBX_TENANT_ID", "");
        }

        private static boolean defaultVerbose() {
            String env = System.getenv("EVENTDBX_VERBOSE");
            if (env == null) {
                return false;
            }
            String normalized = env.trim().toLowerCase();
            return normalized.equals("1") || normalized.equals("true") || normalized.equals("yes");
        }

        private static boolean defaultNoNoise() {
            String env = System.getenv("EVENTDBX_NO_NOISE");
            if (env == null) {
                return false;
            }
            String normalized = env.trim().toLowerCase();
            return normalized.equals("1") || normalized.equals("true") || normalized.equals("yes");
        }

        public Builder host(String host) {
            this.host = Objects.requireNonNull(host, "host");
            return this;
        }

        public Builder port(int port) {
            if (port <= 0 || port > 65535) {
                throw new IllegalArgumentException("port must be in range 1-65535");
            }
            this.port = port;
            return this;
        }

        public Builder token(String token) {
            this.token = token == null ? "" : token.trim();
            return this;
        }

        public Builder tenantId(String tenantId) {
            this.tenantId = tenantId == null ? "" : tenantId.trim();
            return this;
        }

        public Builder verbose(boolean verbose) {
            this.verbose = verbose;
            return this;
        }

        public Builder noNoise(boolean noNoise) {
            this.noNoise = noNoise;
            return this;
        }

        public Builder connectTimeout(Duration connectTimeout) {
            this.connectTimeout = Objects.requireNonNull(connectTimeout, "connectTimeout");
            return this;
        }

        public Builder requestTimeout(Duration requestTimeout) {
            this.requestTimeout = Objects.requireNonNull(requestTimeout, "requestTimeout");
            return this;
        }

        public Builder retryPolicy(RetryPolicy retryPolicy) {
            this.retryPolicy = Objects.requireNonNull(retryPolicy, "retryPolicy");
            return this;
        }

        public EventDbxConfig build() {
            Objects.requireNonNull(host, "host");
            Objects.requireNonNull(connectTimeout, "connectTimeout");
            Objects.requireNonNull(requestTimeout, "requestTimeout");
            Objects.requireNonNull(retryPolicy, "retryPolicy");
            return new EventDbxConfig(this);
        }
    }
}
