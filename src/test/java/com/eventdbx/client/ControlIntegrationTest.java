package com.eventdbx.client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.util.List;
import java.util.UUID;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * Integration scaffold for exercising the control socket verbs against a live EventDBX instance.
 * <p>
 * Requires a running control server and a valid token. Set EVENTDBX_INT_TEST=1 and EVENTDBX_TOKEN
 * (plus optional EVENTDBX_HOST/EVENTDBX_PORT/EVENTDBX_TENANT_ID) to enable.
 */
@EnabledIfEnvironmentVariable(named = "EVENTDBX_INT_TEST", matches = "1|true")
class ControlIntegrationTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Test
    void roundTripCrudOperations() {
        String token = System.getenv("EVENTDBX_TOKEN");
        assumeTrue(token != null && !token.isBlank(), "requires EVENTDBX_TOKEN to run");

        String host = System.getenv().getOrDefault("EVENTDBX_HOST", "127.0.0.1");
        int port = parsePort(System.getenv("EVENTDBX_PORT"), 6363);
        String tenant = System.getenv().getOrDefault("EVENTDBX_TENANT_ID", "default");

        EventDbxConfig config = EventDbxConfig.builder()
                .host(host)
                .port(port)
                .token(token)
                .tenantId(tenant)
                .build();

        String aggregateType = "itest-person";
        String aggregateId = "p-" + UUID.randomUUID();

        try (EventDbxClient client = new EventDbxClient(config)) {
            client.connect();

            ObjectNode payload = MAPPER.createObjectNode().put("name", "Jane Doe");
            AggregateSnapshot created = client.create(
                    aggregateType,
                    aggregateId,
                    "person_registered",
                    CreateAggregateOptions.builder().payload(payload).note("integration create").build());
            assertNotNull(created);
            assertEquals(aggregateId, created.aggregateId());

            AggregateSnapshot fetched = client.get(aggregateType, aggregateId);
            assertNotNull(fetched);
            assertEquals(created.version(), fetched.version());

            EventRecord applied = client.apply(
                    aggregateType,
                    aggregateId,
                    "person_updated",
                    AppendOptions.builder().payload(MAPPER.createObjectNode().put("name", "Janet")).note("apply").build());
            assertNotNull(applied);

            Page<EventRecord> history = client.events(aggregateType, aggregateId, PageOptions.builder().take(10).build());
            assertTrue(history.items().size() >= 2, "expected at least two events");

            // Patch the state
            JsonNode patchValue = MAPPER.createObjectNode().put("status", "active");
            AggregateSnapshot patched = client.patch(
                    aggregateType,
                    aggregateId,
                    "person_status_updated",
                    List.of(new JsonPatchOperation("add", "/status", null, patchValue)),
                    PatchOptions.builder().note("patch").build());
            assertNotNull(patched);

            // Select a subset of fields
            JsonNode selection = client.select(aggregateType, aggregateId, List.of("status"));
            assertNotNull(selection);

            AggregateSnapshot archived = client.archive(
                    aggregateType,
                    aggregateId,
                    ArchiveOptions.builder().note("archive").build());
            assertTrue(archived.archived());

            AggregateSnapshot restored = client.restore(
                    aggregateType,
                    aggregateId,
                    ArchiveOptions.builder().note("restore").build());
            assertFalse(restored.archived());
        } catch (EventDbxException ex) {
            fail("Control client operations are not yet fully implemented: " + ex.getMessage(), ex);
        }
    }

    private static int parsePort(String env, int fallback) {
        if (env == null || env.isBlank()) {
            return fallback;
        }
        try {
            return Integer.parseInt(env.trim());
        } catch (NumberFormatException e) {
            return fallback;
        }
    }
}
