# EventDBX Java Client

Java control-socket client for EventDBX, mirroring the API exposed by [`eventdbxjs`](../eventdbxjs). The control plane is Cap’n Proto over TCP, encrypted with Noise (`Noise_NNpsk0_25519_ChaChaPoly_SHA256`) by default; a future flag will allow requesting plaintext for lab setups when the server is configured to permit `noNoise`.

## Status
- Library surface, models, and retry/configuration options match the Node client.
- Noise transport scaffold is in place, but Cap’n Proto bindings still need to be generated and wired. The `proto/control.capnp` schema is vendored here for codegen.
- The handshake schema now includes a `noNoise` flag in both hello/response; surface it in `EventDbxConfig` once the bindings are wired if you need plaintext for testing against servers that allow it.

## Quick start (API scaffold)
```java
import com.eventdbx.client.*;

EventDbxConfig config = EventDbxConfig.builder()
        .host("127.0.0.1")
        .port(6363)
        .token(System.getenv("EVENTDBX_TOKEN"))
        .tenantId(System.getenv("EVENTDBX_TENANT_ID"))
        .verbose(false)
        // .noNoise(true) // opt into plaintext only when the server is configured to allow it
        .build();

try (EventDbxClient client = new EventDbxClient(config)) {
    client.connect(); // will throw until Cap'n Proto bindings are generated

    client.list("person", PageOptions.builder().take(50).build());
    client.create("person", "p-1", "person_registered", CreateAggregateOptions.builder()
        .publishTarget("search-indexer") // publish targets require the plugin to be installed & running
        .build());
    client.apply("person", "p-1", "person_updated", AppendOptions.builder()
        .note("email updated")
        .publishTarget("analytics-engine:event-only")
        .build());
    client.events("person", "p-1", PageOptions.builder().build());
}
```

## Control protocol
- Schema: `proto/control.capnp` (same as other EventDBX clients).
- Transport: Noise with PSK derived from the control token (`SHA-256(token)`), then Cap’n Proto messages framed with a 4-byte length prefix.
- Required tooling: `capnpc-java` to generate Java bindings from the schema. Once installed, run `capnp compile -ojava:src/main/java proto/control.capnp` and wire the generated types into `NoiseControlClient`.

## Building
- Maven coordinates are defined in `pom.xml` (noise-java + capnproto runtime included). Install Maven locally to build: `mvn test` / `mvn package`.

## Next steps
- Generate Cap’n Proto bindings and implement the request/response mapping in `NoiseControlClient`.
- Port the retry/backoff logic from `eventdbxjs` and add integration tests against a running EventDBX control socket (`ControlIntegrationTest` is scaffolded; enable via `EVENTDBX_INT_TEST=1` and set `EVENTDBX_TOKEN`).
- Add streaming/subscription support once exposed by the control protocol.
