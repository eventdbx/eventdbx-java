package com.eventdbx.client;

import com.fasterxml.jackson.databind.JsonNode;

public final class EventRecord {
    private final String aggregateType;
    private final String aggregateId;
    private final String eventType;
    private final long version;
    private final Long sequence;
    private final JsonNode payload;
    private final EventMetadata metadata;
    private final String hash;
    private final String merkleRoot;

    public EventRecord(
            String aggregateType,
            String aggregateId,
            String eventType,
            long version,
            Long sequence,
            JsonNode payload,
            EventMetadata metadata,
            String hash,
            String merkleRoot) {
        this.aggregateType = aggregateType;
        this.aggregateId = aggregateId;
        this.eventType = eventType;
        this.version = version;
        this.sequence = sequence;
        this.payload = payload;
        this.metadata = metadata;
        this.hash = hash;
        this.merkleRoot = merkleRoot;
    }

    public String aggregateType() {
        return aggregateType;
    }

    public String aggregateId() {
        return aggregateId;
    }

    public String eventType() {
        return eventType;
    }

    public long version() {
        return version;
    }

    public Long sequence() {
        return sequence;
    }

    public JsonNode payload() {
        return payload;
    }

    public EventMetadata metadata() {
        return metadata;
    }

    public String hash() {
        return hash;
    }

    public String merkleRoot() {
        return merkleRoot;
    }
}
