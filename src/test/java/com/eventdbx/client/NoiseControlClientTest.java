package com.eventdbx.client;

import org.junit.jupiter.api.Test;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class NoiseControlClientTest {

    @Test
    void derivePskMatchesSha256() throws NoSuchAlgorithmException {
        EventDbxConfig config = EventDbxConfig.builder().token("secret-token").build();
        NoiseControlClient client = new NoiseControlClient(config);

        byte[] expected = MessageDigest.getInstance("SHA-256").digest("secret-token".getBytes());
        byte[] actual = client.derivePsk("secret-token");

        assertArrayEquals(expected, actual, "PSK should be SHA-256 hash of token");
    }

    @Test
    void connectRequiresToken() {
        EventDbxConfig config = EventDbxConfig.builder().token("").build();
        NoiseControlClient client = new NoiseControlClient(config);

        assertThrows(EventDbxException.class, client::connect);
    }
}
