package com.eventdbx.client;

import com.fasterxml.jackson.databind.JsonNode;

public final class JsonPatchOperation {
    private final String op;
    private final String path;
    private final String from;
    private final JsonNode value;

    public JsonPatchOperation(String op, String path, String from, JsonNode value) {
        this.op = op;
        this.path = path;
        this.from = from;
        this.value = value;
    }

    public String op() {
        return op;
    }

    public String path() {
        return path;
    }

    public String from() {
        return from;
    }

    public JsonNode value() {
        return value;
    }
}
