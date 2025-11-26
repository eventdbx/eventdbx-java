package com.eventdbx.client;

import com.fasterxml.jackson.databind.JsonNode;

import java.util.List;
import java.util.Objects;

/**
 * High-level control-socket client, mirroring the EventDBX JS client surface.
 */
public class EventDbxClient implements AutoCloseable {
    private final EventDbxConfig config;
    private final ControlClient controlClient;
    private boolean closed;

    public EventDbxClient() {
        this(EventDbxConfig.builder().build());
    }

    public EventDbxClient(EventDbxConfig config) {
        this(config, new NoiseControlClient(config));
    }

    public EventDbxClient(EventDbxConfig config, ControlClient controlClient) {
        this.config = Objects.requireNonNull(config, "config");
        this.controlClient = Objects.requireNonNull(controlClient, "controlClient");
    }

    public void connect() {
        ensureOpen();
        controlClient.connect();
    }

    public void disconnect() {
        controlClient.disconnect();
        closed = true;
    }

    public boolean isConnected() {
        return controlClient.isConnected();
    }

    public Endpoint endpoint() {
        return controlClient.endpoint();
    }

    public Page<AggregateSnapshot> list(String aggregateType, PageOptions options) {
        ensureOpen();
        return controlClient.list(aggregateType, options == null ? PageOptions.builder().build() : options);
    }

    public AggregateSnapshot get(String aggregateType, String aggregateId) {
        ensureOpen();
        return controlClient.get(aggregateType, aggregateId);
    }

    public JsonNode select(String aggregateType, String aggregateId, List<String> fields) {
        ensureOpen();
        return controlClient.select(aggregateType, aggregateId, fields);
    }

    public Page<EventRecord> events(String aggregateType, String aggregateId, PageOptions options) {
        ensureOpen();
        return controlClient.events(aggregateType, aggregateId, options == null ? PageOptions.builder().build() : options);
    }

    public EventRecord apply(String aggregateType, String aggregateId, String eventType, AppendOptions options) {
        ensureOpen();
        return controlClient.apply(aggregateType, aggregateId, eventType, options == null ? AppendOptions.builder().build() : options);
    }

    public AggregateSnapshot create(String aggregateType, String aggregateId, String eventType, CreateAggregateOptions options) {
        ensureOpen();
        return controlClient.create(aggregateType, aggregateId, eventType, options == null ? CreateAggregateOptions.builder().build() : options);
    }

    public AggregateSnapshot archive(String aggregateType, String aggregateId, ArchiveOptions options) {
        ensureOpen();
        return controlClient.archive(aggregateType, aggregateId, options == null ? ArchiveOptions.builder().build() : options);
    }

    public AggregateSnapshot restore(String aggregateType, String aggregateId, ArchiveOptions options) {
        ensureOpen();
        return controlClient.restore(aggregateType, aggregateId, options == null ? ArchiveOptions.builder().build() : options);
    }

    public AggregateSnapshot patch(String aggregateType, String aggregateId, String eventType, List<JsonPatchOperation> operations, PatchOptions options) {
        ensureOpen();
        return controlClient.patch(aggregateType, aggregateId, eventType, operations, options == null ? PatchOptions.builder().build() : options);
    }

    @Override
    public void close() {
        disconnect();
    }

    private void ensureOpen() {
        if (closed) {
            throw new IllegalStateException("Client already closed");
        }
    }
}
