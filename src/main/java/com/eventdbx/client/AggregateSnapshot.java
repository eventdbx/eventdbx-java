package com.eventdbx.client;

import com.fasterxml.jackson.databind.JsonNode;

public final class AggregateSnapshot {
    private final String aggregateType;
    private final String aggregateId;
    private final long version;
    private final JsonNode state;
    private final String merkleRoot;
    private final boolean archived;

    public AggregateSnapshot(String aggregateType, String aggregateId, long version, JsonNode state, String merkleRoot, boolean archived) {
        this.aggregateType = aggregateType;
        this.aggregateId = aggregateId;
        this.version = version;
        this.state = state;
        this.merkleRoot = merkleRoot;
        this.archived = archived;
    }

    public String aggregateType() {
        return aggregateType;
    }

    public String aggregateId() {
        return aggregateId;
    }

    public long version() {
        return version;
    }

    public JsonNode state() {
        return state;
    }

    public String merkleRoot() {
        return merkleRoot;
    }

    public boolean archived() {
        return archived;
    }
}
