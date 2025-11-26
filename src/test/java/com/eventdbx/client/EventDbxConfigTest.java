package com.eventdbx.client;

import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class EventDbxConfigTest {

    @Test
    void defaultsAreReasonable() {
        EventDbxConfig config = EventDbxConfig.builder().build();

        assertEquals("127.0.0.1", config.host());
        int expectedPort = 6363;
        String envPort = System.getenv("EVENTDBX_PORT");
        if (envPort != null && !envPort.isBlank()) {
            try {
                expectedPort = Integer.parseInt(envPort.trim());
            } catch (NumberFormatException ignored) {
                expectedPort = 6363;
            }
        }
        assertEquals(expectedPort, config.port());
        assertFalse(config.verbose());
        assertEquals(Duration.ofSeconds(3), config.connectTimeout());
        assertEquals(Duration.ofSeconds(10), config.requestTimeout());
        assertEquals(1, config.retryPolicy().maxAttempts());
        assertEquals(Duration.ofMillis(50), config.retryPolicy().initialDelay());
        assertEquals(Duration.ofMillis(1000), config.retryPolicy().maxDelay());
    }

    @Test
    void rejectsInvalidPort() {
        assertThrows(IllegalArgumentException.class, () -> EventDbxConfig.builder().port(0));
        assertThrows(IllegalArgumentException.class, () -> EventDbxConfig.builder().port(70000));
    }

    @Test
    void buildsCustomValues() {
        EventDbxConfig config = EventDbxConfig.builder()
                .host("api.eventdbx.test")
                .port(9443)
                .token("token")
                .tenantId("tenant-a")
                .verbose(true)
                .retryPolicy(RetryPolicy.builder().maxAttempts(3).initialDelay(Duration.ofMillis(10)).maxDelay(Duration.ofMillis(20)).build())
                .connectTimeout(Duration.ofSeconds(1))
                .requestTimeout(Duration.ofSeconds(2))
                .build();

        assertEquals("api.eventdbx.test", config.host());
        assertEquals(9443, config.port());
        assertTrue(config.verbose());
        assertEquals("token", config.token());
        assertEquals("tenant-a", config.tenantId());
        assertEquals(Duration.ofSeconds(1), config.connectTimeout());
        assertEquals(Duration.ofSeconds(2), config.requestTimeout());
        assertEquals(3, config.retryPolicy().maxAttempts());
        assertEquals(Duration.ofMillis(10), config.retryPolicy().initialDelay());
        assertEquals(Duration.ofMillis(20), config.retryPolicy().maxDelay());
    }
}
