package com.eventdbx.client;

import java.time.Instant;

public final class EventMetadata {
    private final String eventId;
    private final Instant createdAt;
    private final ActorClaims issuedBy;
    private final String note;

    public EventMetadata(String eventId, Instant createdAt, ActorClaims issuedBy, String note) {
        this.eventId = eventId;
        this.createdAt = createdAt;
        this.issuedBy = issuedBy;
        this.note = note;
    }

    public String eventId() {
        return eventId;
    }

    public Instant createdAt() {
        return createdAt;
    }

    public ActorClaims issuedBy() {
        return issuedBy;
    }

    public String note() {
        return note;
    }
}
