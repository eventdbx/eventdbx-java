package com.eventdbx.client;

import com.fasterxml.jackson.databind.JsonNode;

public interface ControlClient extends AutoCloseable {
    void connect();

    void disconnect();

    boolean isConnected();

    Endpoint endpoint();

    Page<AggregateSnapshot> list(String aggregateType, PageOptions options);

    AggregateSnapshot create(String aggregateType, String aggregateId, String eventType, CreateAggregateOptions options);

    AggregateSnapshot archive(String aggregateType, String aggregateId, ArchiveOptions options);

    AggregateSnapshot restore(String aggregateType, String aggregateId, ArchiveOptions options);

    AggregateSnapshot patch(String aggregateType, String aggregateId, String eventType, Iterable<JsonPatchOperation> operations, PatchOptions options);

    AggregateSnapshot get(String aggregateType, String aggregateId);

    JsonNode select(String aggregateType, String aggregateId, Iterable<String> fields);

    Page<EventRecord> events(String aggregateType, String aggregateId, PageOptions options);

    EventRecord apply(String aggregateType, String aggregateId, String eventType, AppendOptions options);

    @Override
    default void close() {
        disconnect();
    }
}
