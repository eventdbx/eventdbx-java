package com.eventdbx.client;

public final class ActorClaims {
    private final String group;
    private final String user;

    public ActorClaims(String group, String user) {
        this.group = group;
        this.user = user;
    }

    public String group() {
        return group;
    }

    public String user() {
        return user;
    }
}
