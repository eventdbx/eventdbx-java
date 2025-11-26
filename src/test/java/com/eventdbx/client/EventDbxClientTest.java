package com.eventdbx.client;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class EventDbxClientTest {

    @Mock
    ControlClient controlClient;

    private final ObjectMapper mapper = new ObjectMapper();
    private final EventDbxConfig config = EventDbxConfig.builder().token("token").build();

    @Test
    void connectDelegatesToControlClient() {
        EventDbxClient client = new EventDbxClient(config, controlClient);
        client.connect();
        verify(controlClient).connect();
    }

    @Test
    void listDelegates() {
        PageOptions options = PageOptions.builder().take(10).build();
        Page<AggregateSnapshot> page = new Page<>(List.of(), "next");
        when(controlClient.list(any(), any())).thenReturn(page);

        EventDbxClient client = new EventDbxClient(config, controlClient);
        Page<AggregateSnapshot> result = client.list("person", options);

        assertSame(page, result);
        verify(controlClient).list("person", options);
    }

    @Test
    void getDelegates() {
        AggregateSnapshot snapshot = new AggregateSnapshot("person", "p1", 1, mapper.createObjectNode(), "root", false);
        when(controlClient.get("person", "p1")).thenReturn(snapshot);

        EventDbxClient client = new EventDbxClient(config, controlClient);
        AggregateSnapshot result = client.get("person", "p1");

        assertSame(snapshot, result);
        verify(controlClient).get("person", "p1");
    }

    @Test
    void selectDelegates() {
        JsonNode selection = mapper.createObjectNode().put("name", "Jane");
        List<String> fields = List.of("name");
        when(controlClient.select("person", "p1", fields)).thenReturn(selection);

        EventDbxClient client = new EventDbxClient(config, controlClient);
        JsonNode result = client.select("person", "p1", fields);

        assertSame(selection, result);
        verify(controlClient).select("person", "p1", fields);
    }

    @Test
    void eventsDelegates() {
        Page<EventRecord> page = new Page<>(List.of(), "cursor");
        PageOptions options = PageOptions.builder().take(5).build();
        when(controlClient.events("person", "p1", options)).thenReturn(page);

        EventDbxClient client = new EventDbxClient(config, controlClient);
        Page<EventRecord> result = client.events("person", "p1", options);

        assertSame(page, result);
        verify(controlClient).events("person", "p1", options);
    }

    @Test
    void applyDelegates() {
        EventRecord record = new EventRecord("person", "p1", "evt", 1, null, mapper.createObjectNode(), null, "", "");
        AppendOptions options = AppendOptions.builder().note("n").build();
        when(controlClient.apply("person", "p1", "evt", options)).thenReturn(record);

        EventDbxClient client = new EventDbxClient(config, controlClient);
        EventRecord result = client.apply("person", "p1", "evt", options);

        assertSame(record, result);
        verify(controlClient).apply("person", "p1", "evt", options);
    }

    @Test
    void createDelegates() {
        AggregateSnapshot snapshot = new AggregateSnapshot("person", "p1", 1, mapper.createObjectNode(), "root", false);
        CreateAggregateOptions options = CreateAggregateOptions.builder().note("n").build();
        when(controlClient.create("person", "p1", "evt", options)).thenReturn(snapshot);

        EventDbxClient client = new EventDbxClient(config, controlClient);
        AggregateSnapshot result = client.create("person", "p1", "evt", options);

        assertSame(snapshot, result);
        verify(controlClient).create("person", "p1", "evt", options);
    }

    @Test
    void patchDelegates() {
        AggregateSnapshot snapshot = new AggregateSnapshot("person", "p1", 2, mapper.createObjectNode(), "root", false);
        List<JsonPatchOperation> ops = List.of(new JsonPatchOperation("replace", "/name", null, mapper.createObjectNode()));
        PatchOptions options = PatchOptions.builder().note("n").build();
        when(controlClient.patch("person", "p1", "evt", ops, options)).thenReturn(snapshot);

        EventDbxClient client = new EventDbxClient(config, controlClient);
        AggregateSnapshot result = client.patch("person", "p1", "evt", ops, options);

        assertSame(snapshot, result);
        verify(controlClient).patch("person", "p1", "evt", ops, options);
    }

    @Test
    void archiveDelegate() {
        AggregateSnapshot snapshot = new AggregateSnapshot("person", "p1", 3, mapper.createObjectNode(), "root", true);
        ArchiveOptions options = ArchiveOptions.builder().note("n").build();
        when(controlClient.archive("person", "p1", options)).thenReturn(snapshot);

        EventDbxClient client = new EventDbxClient(config, controlClient);
        AggregateSnapshot result = client.archive("person", "p1", options);

        assertSame(snapshot, result);
        verify(controlClient).archive("person", "p1", options);
    }

    @Test
    void restoreDelegates() {
        AggregateSnapshot snapshot = new AggregateSnapshot("person", "p1", 4, mapper.createObjectNode(), "root", false);
        ArchiveOptions options = ArchiveOptions.builder().note("n").build();
        when(controlClient.restore("person", "p1", options)).thenReturn(snapshot);

        EventDbxClient client = new EventDbxClient(config, controlClient);
        AggregateSnapshot result = client.restore("person", "p1", options);

        assertSame(snapshot, result);
        verify(controlClient).restore("person", "p1", options);
    }

    @Test
    void closeDisconnects() {
        EventDbxClient client = new EventDbxClient(config, controlClient);
        client.close();
        verify(controlClient).disconnect();
    }
}
