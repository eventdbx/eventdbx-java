package com.eventdbx.client;

import com.sun.jna.Library;
import com.sun.jna.Native;
import com.sun.jna.NativeLibrary;
import com.sun.jna.Pointer;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * JNA bridge to the local Rust-based Noise binding (native/snownoise).
 */
interface SnowNative extends Library {
    SnowNative INSTANCE = load();

    Pointer snow_init(byte[] psk, int pskLen);

    long snow_write_handshake(Pointer state, byte[] out, long outCap);

    long snow_read_handshake(Pointer state, byte[] incoming, long len);

    long snow_write(Pointer state, byte[] plaintext, long len, byte[] out, long outCap);

    long snow_read(Pointer state, byte[] ciphertext, long len, byte[] out, long outCap);

    void snow_free(Pointer state);

    static SnowNative load() {
        try {
            // Allow direct override first.
            String override = System.getenv("SNOWNOISE_LIB");
            if (override != null && !override.isBlank()) {
                return Native.load(override, SnowNative.class);
            }

            // Allow an explicit search path via environment or system property.
            String libPath = firstNonBlank(
                    System.getenv("SNOWNOISE_LIB_PATH"),
                    System.getProperty("snownoise.libpath"));
            if (libPath == null || libPath.isBlank()) {
                // Default to the built native artifact under the repo.
                Path defaultPath = Paths.get(System.getProperty("user.dir"),
                        "native", "snownoise", "target", "release");
                if (Files.isDirectory(defaultPath)) {
                    libPath = defaultPath.toString();
                }
            }
            if (libPath != null && !libPath.isBlank()) {
                NativeLibrary.addSearchPath("snownoise", libPath);
            }
            return Native.load("snownoise", SnowNative.class);
        } catch (UnsatisfiedLinkError e) {
            return null;
        }
    }

    private static String firstNonBlank(String... candidates) {
        for (String c : candidates) {
            if (c != null && !c.isBlank()) {
                return c;
            }
        }
        return null;
    }
}
