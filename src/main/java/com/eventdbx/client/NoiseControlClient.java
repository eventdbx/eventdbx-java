package com.eventdbx.client;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import com.eventdbx.client.proto.ControlSchemas;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.jna.Pointer;

/**
 * Skeleton Noise transport that mirrors the EventDBX JS client behavior.
 * <p>
 * The control socket speaks Cap'n Proto messages encrypted with Noise
 * (pattern <code>Noise_NNpsk0_25519_ChaChaPoly_SHA256</code>). This stub wires
 * config and future retry handling but defers Cap'n Proto encoding/decoding
 * until the Java schema compiler (`capnpc-java`) is available locally.
 */
public final class NoiseControlClient implements ControlClient {
    private static final String DEFAULT_NOISE_PATTERN = "Noise_NNpsk0_25519_ChaChaPoly_SHA256";
    private static final int MAX_FRAME_LEN = 16 * 1024 * 1024;

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final EventDbxConfig config;
    private Socket socket;
    private DataInputStream in;
    private DataOutputStream out;
    private final SnowNative snow;
    private Pointer noiseState;
    private volatile boolean connected;
    private long nextRequestId = 1;
    private final String noisePattern;
    private boolean useNoise;

    public NoiseControlClient(EventDbxConfig config) {
        this.config = Objects.requireNonNull(config, "config");
        String pattern = System.getenv().getOrDefault("EVENTDBX_NOISE_PATTERN", DEFAULT_NOISE_PATTERN);
        this.noisePattern = pattern.isBlank() ? DEFAULT_NOISE_PATTERN : pattern.trim();
        this.snow = SnowNative.load();
    }

    @Override
    public void connect() {
        if (connected) {
            return;
        }
        if (config.token().isBlank()) {
            throw new EventDbxException("Control token is required to connect to EventDBX");
        }
        if (snow == null && !config.noNoise()) {
            throw new EventDbxException("Native snownoise library not available. Build native/snownoise via `cargo build --release` and ensure the resulting library is on java.library.path.");
        }
        try {
            this.socket = new Socket();
            socket.connect(new InetSocketAddress(config.host(), config.port()), (int) config.connectTimeout().toMillis());
            socket.setSoTimeout((int) config.requestTimeout().toMillis());
            this.in = new DataInputStream(socket.getInputStream());
            this.out = new DataOutputStream(socket.getOutputStream());

            sendControlHello();
            boolean serverNoNoise = readControlHelloResponse();
            this.useNoise = !(config.noNoise() || serverNoNoise);
            if (useNoise) {
                performNoiseHandshake();
            }
            connected = true;
        } catch (IOException e) {
            disconnect();
            throw new EventDbxException("Failed to open control socket", e);
        } catch (RuntimeException e) {
            disconnect();
            throw e;
        }
    }

    @Override
    public void disconnect() {
        connected = false;
        try {
            if (socket != null) {
                socket.close();
            }
        } catch (IOException ignored) {
        } finally {
            socket = null;
            in = null;
            out = null;
            if (snow != null && noiseState != null) {
                snow.snow_free(noiseState);
            }
            noiseState = null;
        }
    }

    @Override
    public boolean isConnected() {
        return connected;
    }

    @Override
    public Endpoint endpoint() {
        return new Endpoint(config.host(), config.port());
    }

    @Override
    public Page<AggregateSnapshot> list(String aggregateType, PageOptions options) {
        ensureConnected();
        PageOptions opts = options == null ? PageOptions.builder().build() : options;
        ControlSchemas.ControlResponse.Reader resp = sendRequest(builder -> {
            ControlSchemas.ControlRequest.Builder request = builder.initRoot(ControlSchemas.ControlRequest.factory);
            request.setId(nextRequestId++);
            ControlSchemas.ControlRequest.Payload.Builder payload = request.getPayload();
            ControlSchemas.ListAggregatesRequest.Builder body = payload.initListAggregates();
            body.setHasCursor(opts.cursor().isPresent());
            body.setCursor(opts.cursor().orElse(""));
            body.setHasTake(opts.take().isPresent());
            body.setTake(opts.take().orElse(0));
            body.setIncludeArchived(opts.includeArchived());
            body.setArchivedOnly(opts.archivedOnly());
            body.setToken(opts.token().orElse(config.token()));
            body.setHasFilter(opts.filter().isPresent());
            body.setFilter(opts.filter().orElse(""));
            body.setHasSort(opts.sort().isPresent());
            body.setSort(opts.sort().orElse(""));
        });
        return parseListAggregates(resp);
    }

    @Override
    public AggregateSnapshot create(String aggregateType, String aggregateId, String eventType, CreateAggregateOptions options) {
        ensureConnected();
        CreateAggregateOptions opts = options == null ? CreateAggregateOptions.builder().build() : options;
        ControlSchemas.ControlResponse.Reader resp = sendRequest(builder -> {
            ControlSchemas.ControlRequest.Builder request = builder.initRoot(ControlSchemas.ControlRequest.factory);
            request.setId(nextRequestId++);
            ControlSchemas.ControlRequest.Payload.Builder payload = request.getPayload();
            ControlSchemas.CreateAggregateRequest.Builder body = payload.initCreateAggregate();
            body.setToken(opts.token().orElse(config.token()));
            body.setAggregateType(aggregateType);
            body.setAggregateId(aggregateId);
            body.setEventType(eventType);
            JsonNode payloadNode = opts.payload().orElse(null);
            if (payloadNode != null) {
                body.setPayloadJson(json(payloadNode));
            }
            JsonNode metadataNode = opts.metadata().orElse(null);
            if (metadataNode != null) {
                body.setHasMetadata(true);
                body.setMetadataJson(json(metadataNode));
            } else {
                body.setHasMetadata(false);
                body.setMetadataJson("");
            }
            String note = opts.note().orElse("");
            body.setHasNote(!note.isBlank());
            body.setNote(note);
            List<PublishTarget> targets = opts.publishTargets();
            body.setHasPublishTargets(!targets.isEmpty());
            applyPublishTargets(body.initPublishTargets(targets.size()), targets);
        });
        return parseAggregate(resp);
    }

    @Override
    public AggregateSnapshot archive(String aggregateType, String aggregateId, ArchiveOptions options) {
        return setArchiveState(aggregateType, aggregateId, true, options);
    }

    @Override
    public AggregateSnapshot restore(String aggregateType, String aggregateId, ArchiveOptions options) {
        return setArchiveState(aggregateType, aggregateId, false, options);
    }

    @Override
    public AggregateSnapshot patch(String aggregateType, String aggregateId, String eventType, Iterable<JsonPatchOperation> operations, PatchOptions options) {
        ensureConnected();
        PatchOptions opts = options == null ? PatchOptions.builder().build() : options;
        ControlSchemas.ControlResponse.Reader resp = sendRequest(builder -> {
            ControlSchemas.ControlRequest.Builder request = builder.initRoot(ControlSchemas.ControlRequest.factory);
            request.setId(nextRequestId++);
            ControlSchemas.ControlRequest.Payload.Builder payload = request.getPayload();
            ControlSchemas.PatchEventRequest.Builder body = payload.initPatchEvent();
            body.setToken(opts.token().orElse(config.token()));
            body.setAggregateType(aggregateType);
            body.setAggregateId(aggregateId);
            body.setEventType(eventType);
            JsonNode patchArray = MAPPER.valueToTree(operations);
            body.setPatchJson(json(patchArray));
            JsonNode metadata = opts.metadata().orElse(null);
            if (metadata != null) {
                body.setHasMetadata(true);
                body.setMetadataJson(json(metadata));
            } else {
                body.setHasMetadata(false);
                body.setMetadataJson("");
            }
            String note = opts.note().orElse("");
            body.setHasNote(!note.isBlank());
            body.setNote(note);
            List<PublishTarget> targets = opts.publishTargets();
            body.setHasPublishTargets(!targets.isEmpty());
            applyPublishTargets(body.initPublishTargets(targets.size()), targets);
        });
        return parseAggregate(resp);
    }

    @Override
    public AggregateSnapshot get(String aggregateType, String aggregateId) {
        ensureConnected();
        ControlSchemas.ControlResponse.Reader resp = sendRequest(builder -> {
            ControlSchemas.ControlRequest.Builder request = builder.initRoot(ControlSchemas.ControlRequest.factory);
            request.setId(nextRequestId++);
            ControlSchemas.ControlRequest.Payload.Builder payload = request.getPayload();
            ControlSchemas.GetAggregateRequest.Builder body = payload.initGetAggregate();
            body.setAggregateType(aggregateType);
            body.setAggregateId(aggregateId);
            body.setToken(config.token());
        });
        return parseGetAggregate(resp);
    }

    @Override
    public JsonNode select(String aggregateType, String aggregateId, Iterable<String> fields) {
        ensureConnected();
        List<String> fieldList = new ArrayList<>();
        fields.forEach(fieldList::add);
        ControlSchemas.ControlResponse.Reader resp = sendRequest(builder -> {
            ControlSchemas.ControlRequest.Builder request = builder.initRoot(ControlSchemas.ControlRequest.factory);
            request.setId(nextRequestId++);
            ControlSchemas.ControlRequest.Payload.Builder payload = request.getPayload();
            ControlSchemas.SelectAggregateRequest.Builder body = payload.initSelectAggregate();
            body.setAggregateType(aggregateType);
            body.setAggregateId(aggregateId);
            org.capnproto.TextList.Builder list = body.initFields(fieldList.size());
            for (int i = 0; i < fieldList.size(); i++) {
                list.set(i, new org.capnproto.Text.Reader(fieldList.get(i)));
            }
            body.setToken(config.token());
        });
        return parseSelection(resp);
    }

    @Override
    public Page<EventRecord> events(String aggregateType, String aggregateId, PageOptions options) {
        ensureConnected();
        PageOptions opts = options == null ? PageOptions.builder().build() : options;
        ControlSchemas.ControlResponse.Reader resp = sendRequest(builder -> {
            ControlSchemas.ControlRequest.Builder request = builder.initRoot(ControlSchemas.ControlRequest.factory);
            request.setId(nextRequestId++);
            ControlSchemas.ControlRequest.Payload.Builder payload = request.getPayload();
            ControlSchemas.ListEventsRequest.Builder body = payload.initListEvents();
            body.setAggregateType(aggregateType);
            body.setAggregateId(aggregateId);
            body.setHasCursor(opts.cursor().isPresent());
            body.setCursor(opts.cursor().orElse(""));
            body.setHasTake(opts.take().isPresent());
            body.setTake(opts.take().orElse(0));
            body.setHasFilter(opts.filter().isPresent());
            body.setFilter(opts.filter().orElse(""));
            body.setToken(opts.token().orElse(config.token()));
        });
        return parseEvents(resp);
    }

    @Override
    public EventRecord apply(String aggregateType, String aggregateId, String eventType, AppendOptions options) {
        ensureConnected();
        AppendOptions opts = options == null ? AppendOptions.builder().build() : options;
        ControlSchemas.ControlResponse.Reader resp = sendRequest(builder -> {
            ControlSchemas.ControlRequest.Builder request = builder.initRoot(ControlSchemas.ControlRequest.factory);
            request.setId(nextRequestId++);
            ControlSchemas.ControlRequest.Payload.Builder payload = request.getPayload();
            ControlSchemas.AppendEventRequest.Builder body = payload.initAppendEvent();
            body.setToken(opts.token().orElse(config.token()));
            body.setAggregateType(aggregateType);
            body.setAggregateId(aggregateId);
            body.setEventType(eventType);
            JsonNode payloadNode = opts.payload().orElse(null);
            if (payloadNode != null) {
                body.setPayloadJson(json(payloadNode));
            } else {
                body.setPayloadJson("");
            }
            String note = opts.note().orElse("");
            body.setHasNote(!note.isBlank());
            body.setNote(note);
            JsonNode metadata = opts.metadata().orElse(null);
            if (metadata != null) {
                body.setHasMetadata(true);
                body.setMetadataJson(json(metadata));
            } else {
                body.setHasMetadata(false);
                body.setMetadataJson("");
            }
            List<PublishTarget> targets = opts.publishTargets();
            body.setHasPublishTargets(!targets.isEmpty());
            applyPublishTargets(body.initPublishTargets(targets.size()), targets);
        });
        return parseAppend(resp);
    }

    private void performNoiseHandshake() {
        try {
            byte[] psk = noisePattern.toLowerCase().contains("psk") ? derivePsk(config.token()) : new byte[0];
            noiseState = snow.snow_init(psk, psk.length);
            if (noiseState == null) {
                throw new EventDbxException("Failed to initialise snownoise");
            }

            byte[] outbound = new byte[256];
            long outboundLen = snow.snow_write_handshake(noiseState, outbound, outbound.length);
            if (outboundLen <= 0) {
                throw new EventDbxException("Noise handshake failed: unable to write handshake");
            }
            writeFrame(outbound, (int) outboundLen);

            byte[] inbound = readFrame();
            if (inbound == null) {
                throw new EventDbxException("Control socket closed during Noise handshake");
            }
            long read = snow.snow_read_handshake(noiseState, inbound, inbound.length);
            if (read < 0) {
                throw new EventDbxException("Noise handshake failed: read phase error");
            }
        } catch (IOException e) {
            throw new EventDbxException("IO failure during Noise handshake", e);
        } catch (Exception e) {
            throw new EventDbxException("Noise handshake failed: " + e.getMessage(), e);
        }
    }

    private void writeEncrypted(byte[] plaintext, int length) {
        if (noiseState == null) {
            throw new IllegalStateException("Noise state not initialised");
        }
        try {
            byte[] ciphertext = new byte[length + 64]; // sufficient headroom for MAC
            long written = snow.snow_write(noiseState, plaintext, length, ciphertext, ciphertext.length);
            if (written <= 0) {
                throw new EventDbxException("Noise encryption failed");
            }
            writeFrame(ciphertext, (int) written);
        } catch (IOException e) {
            throw new EventDbxException("Failed to write encrypted frame", e);
        }
    }

    private byte[] readEncrypted() {
        if (noiseState == null) {
            throw new IllegalStateException("Noise state not initialised");
        }
        try {
            byte[] ciphertext = readFrame();
            if (ciphertext == null) {
                return null;
            }
            byte[] buffer = new byte[ciphertext.length];
            long len = snow.snow_read(noiseState, ciphertext, ciphertext.length, buffer, buffer.length);
            if (len < 0) {
                throw new EventDbxException("Noise decryption failed");
            }
            byte[] plaintext = new byte[(int) len];
            System.arraycopy(buffer, 0, plaintext, 0, (int) len);
            return plaintext;
        } catch (IOException e) {
            throw new EventDbxException("Failed to read encrypted frame", e);
        }
    }

    private void writeFrame(byte[] payload, int length) throws IOException {
        if (length > MAX_FRAME_LEN) {
            throw new EventDbxException("Frame too large: " + length);
        }
        out.writeInt(length);
        if (length > 0) {
            out.write(payload, 0, length);
        }
        out.flush();
    }

    private byte[] readFrame() throws IOException {
        int len;
        try {
            len = in.readInt();
        } catch (IOException e) {
            return null;
        }
        if (len < 0 || len > MAX_FRAME_LEN) {
            throw new EventDbxException("Invalid frame length: " + len);
        }
        byte[] payload = new byte[len];
        in.readFully(payload);
        return payload;
    }

    private EventDbxException unsupported() {
        return new EventDbxException("Control operation not implemented.");
    }

    private void sendControlHello() {
        try {
            org.capnproto.MessageBuilder message = new org.capnproto.MessageBuilder();
            ControlSchemas.ControlHello.Builder hello = message.initRoot(ControlSchemas.ControlHello.factory);
            hello.setProtocolVersion((short) 1);
            hello.setToken(config.token());
            hello.setTenantId(config.tenantId() == null ? "" : config.tenantId());
            hello.setNoNoise(config.noNoise());

            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            WritableByteChannel channel = Channels.newChannel(baos);
            org.capnproto.Serialize.write(channel, message);
            channel.close();
            byte[] bytes = baos.toByteArray();
            out.write(bytes);
            out.flush();
        } catch (IOException e) {
            throw new EventDbxException("Failed to send control hello", e);
        }
    }

    private boolean readControlHelloResponse() {
        try {
            ReadableByteChannel channel = Channels.newChannel(socket.getInputStream());
            org.capnproto.MessageReader reader = org.capnproto.Serialize.read(channel);
            ControlSchemas.ControlHelloResponse.Reader helloResp =
                    reader.getRoot(ControlSchemas.ControlHelloResponse.factory);
            if (!helloResp.getAccepted()) {
                String reason = helloResp.getMessage().toString();
                throw new EventDbxException("Control handshake rejected: " + reason);
            }
            return helloResp.getNoNoise();
        } catch (IOException e) {
            throw new EventDbxException("Failed to read control hello response", e);
        }
    }

    private void ensureConnected() {
        if (!connected) {
            connect();
        }
    }

    private AggregateSnapshot setArchiveState(String aggregateType, String aggregateId, boolean archived, ArchiveOptions options) {
        ensureConnected();
        ArchiveOptions opts = options == null ? ArchiveOptions.builder().build() : options;
        ControlSchemas.ControlResponse.Reader resp = sendRequest(builder -> {
            ControlSchemas.ControlRequest.Builder request = builder.initRoot(ControlSchemas.ControlRequest.factory);
            request.setId(nextRequestId++);
            ControlSchemas.ControlRequest.Payload.Builder payload = request.getPayload();
            ControlSchemas.SetAggregateArchiveRequest.Builder body = payload.initSetAggregateArchive();
            body.setToken(opts.token().orElse(config.token()));
            body.setAggregateType(aggregateType);
            body.setAggregateId(aggregateId);
            body.setArchived(archived);
            String note = opts.note().orElse("");
            body.setHasNote(!note.isBlank());
            body.setNote(note);
        });
        return parseAggregate(resp);
    }

    private void applyPublishTargets(org.capnproto.StructList.Builder<ControlSchemas.PublishTarget.Builder> listBuilder, List<PublishTarget> targets) {
        if (listBuilder == null || targets == null) {
            return;
        }
        for (int i = 0; i < targets.size(); i++) {
            PublishTarget target = targets.get(i);
            ControlSchemas.PublishTarget.Builder dest = listBuilder.get(i);
            dest.setPlugin(target.plugin());
            dest.setHasMode(target.mode() != null);
            dest.setMode(target.mode() == null ? "" : target.mode());
            dest.setHasPriority(target.priority() != null);
            dest.setPriority(target.priority() == null ? "" : target.priority());
        }
    }

    private interface RequestBuilder {
        void build(org.capnproto.MessageBuilder builder);
    }

    private ControlSchemas.ControlResponse.Reader sendRequest(RequestBuilder requestBuilder) {
        org.capnproto.MessageBuilder message = new org.capnproto.MessageBuilder();
        requestBuilder.build(message);
        byte[] bytes = serialize(message);
        if (useNoise) {
            writeEncrypted(bytes, bytes.length);
        } else {
            try {
                writeFrame(bytes, bytes.length);
            } catch (IOException e) {
                throw new EventDbxException("Failed to write control frame", e);
            }
        }

        byte[] responseBytes;
        if (useNoise) {
            responseBytes = readEncrypted();
        } else {
            try {
                responseBytes = readFrame();
            } catch (IOException e) {
                throw new EventDbxException("Failed to read control frame", e);
            }
        }
        if (responseBytes == null) {
            throw new EventDbxException("Control socket closed while awaiting response");
        }
        return parseResponseReader(responseBytes);
    }

    private byte[] serialize(org.capnproto.MessageBuilder message) {
        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            WritableByteChannel channel = Channels.newChannel(baos);
            org.capnproto.Serialize.write(channel, message);
            channel.close();
            return baos.toByteArray();
        } catch (IOException e) {
            throw new EventDbxException("Failed to serialize control message", e);
        }
    }

    private ControlSchemas.ControlResponse.Reader parseResponseReader(byte[] bytes) {
        try {
            ByteBuffer buffer = ByteBuffer.wrap(bytes);
            org.capnproto.MessageReader reader = org.capnproto.Serialize.read(buffer);
            return reader.getRoot(ControlSchemas.ControlResponse.factory);
        } catch (IOException e) {
            throw new EventDbxException("Failed to parse control response", e);
        }
    }

    private Page<AggregateSnapshot> parseListAggregates(ControlSchemas.ControlResponse.Reader response) {
        switch (response.getPayload().which()) {
            case LIST_AGGREGATES -> {
                ControlSchemas.ListAggregatesResponse.Reader body = response.getPayload().getListAggregates();
                List<AggregateSnapshot> items = parseAggregateList(readString(body.getAggregatesJson()));
                String cursor = body.getHasNextCursor() ? readString(body.getNextCursor()) : null;
                return new Page<>(items, cursor);
            }
            case ERROR -> throw error(response.getPayload().getError());
            default -> throw new EventDbxException("Unexpected payload for listAggregates");
        }
    }

    private AggregateSnapshot parseGetAggregate(ControlSchemas.ControlResponse.Reader response) {
        switch (response.getPayload().which()) {
            case GET_AGGREGATE -> {
                ControlSchemas.GetAggregateResponse.Reader body = response.getPayload().getGetAggregate();
                if (!body.getFound()) {
                    return null;
                }
                return parseAggregateJson(readString(body.getAggregateJson()));
            }
            case ERROR -> throw error(response.getPayload().getError());
            default -> throw new EventDbxException("Unexpected payload for getAggregate");
        }
    }

    private AggregateSnapshot parseAggregate(ControlSchemas.ControlResponse.Reader response) {
        switch (response.getPayload().which()) {
            case CREATE_AGGREGATE -> {
                ControlSchemas.CreateAggregateResponse.Reader body = response.getPayload().getCreateAggregate();
                return parseAggregateJson(readString(body.getAggregateJson()));
            }
            case SET_AGGREGATE_ARCHIVE -> {
                ControlSchemas.SetAggregateArchiveResponse.Reader body = response.getPayload().getSetAggregateArchive();
                return parseAggregateJson(readString(body.getAggregateJson()));
            }
            case ERROR -> throw error(response.getPayload().getError());
            default -> throw new EventDbxException("Unexpected payload for aggregate operation");
        }
    }

    private EventRecord parseAppend(ControlSchemas.ControlResponse.Reader response) {
        switch (response.getPayload().which()) {
            case APPEND_EVENT -> {
                ControlSchemas.AppendEventResponse.Reader body = response.getPayload().getAppendEvent();
                return parseEventJson(readString(body.getEventJson()));
            }
            case ERROR -> throw error(response.getPayload().getError());
            default -> throw new EventDbxException("Unexpected payload for appendEvent");
        }
    }

    private Page<EventRecord> parseEvents(ControlSchemas.ControlResponse.Reader response) {
        switch (response.getPayload().which()) {
            case LIST_EVENTS -> {
                ControlSchemas.ListEventsResponse.Reader body = response.getPayload().getListEvents();
                List<EventRecord> items = parseEventList(readString(body.getEventsJson()));
                String cursor = body.getHasNextCursor() ? readString(body.getNextCursor()) : null;
                return new Page<>(items, cursor);
            }
            case ERROR -> throw error(response.getPayload().getError());
            default -> throw new EventDbxException("Unexpected payload for listEvents");
        }
    }

    private JsonNode parseSelection(ControlSchemas.ControlResponse.Reader response) {
        switch (response.getPayload().which()) {
            case SELECT_AGGREGATE -> {
                ControlSchemas.SelectAggregateResponse.Reader body = response.getPayload().getSelectAggregate();
                if (!body.getFound()) {
                    return null;
                }
                return parseJson(readString(body.getSelectionJson()));
            }
            case ERROR -> throw error(response.getPayload().getError());
            default -> throw new EventDbxException("Unexpected payload for selectAggregate");
        }
    }

    private EventDbxException error(ControlSchemas.ControlError.Reader error) {
        String code = readString(error.getCode());
        String message = readString(error.getMessage());
        return new EventDbxException("Server error (" + code + "): " + message);
    }

    private List<AggregateSnapshot> parseAggregateList(String json) {
        try {
            JsonNode node = MAPPER.readTree(json);
            List<AggregateSnapshot> list = new ArrayList<>();
            if (node.isArray()) {
                for (JsonNode item : node) {
                    list.add(toAggregate(item));
                }
            }
            return list;
        } catch (IOException e) {
            throw new EventDbxException("Failed to parse aggregates", e);
        }
    }

    private AggregateSnapshot parseAggregateJson(String json) {
        try {
            return toAggregate(MAPPER.readTree(json));
        } catch (IOException e) {
            throw new EventDbxException("Failed to parse aggregate", e);
        }
    }

    private List<EventRecord> parseEventList(String json) {
        try {
            JsonNode node = MAPPER.readTree(json);
            List<EventRecord> list = new ArrayList<>();
            if (node.isArray()) {
                for (JsonNode item : node) {
                    list.add(toEvent(item));
                }
            }
            return list;
        } catch (IOException e) {
            throw new EventDbxException("Failed to parse events", e);
        }
    }

    private EventRecord parseEventJson(String json) {
        try {
            return toEvent(MAPPER.readTree(json));
        } catch (IOException e) {
            throw new EventDbxException("Failed to parse event", e);
        }
    }

    private AggregateSnapshot toAggregate(JsonNode node) {
        String aggType = node.path("aggregateType").asText();
        String aggId = node.path("aggregateId").asText();
        long version = node.path("version").asLong();
        JsonNode state = node.path("state").isMissingNode() ? MAPPER.createObjectNode() : node.path("state");
        String merkleRoot = node.path("merkleRoot").asText("");
        boolean archived = node.path("archived").asBoolean(false);
        return new AggregateSnapshot(aggType, aggId, version, state, merkleRoot, archived);
    }

    private EventRecord toEvent(JsonNode node) {
        String aggType = node.path("aggregateType").asText();
        String aggId = node.path("aggregateId").asText();
        String eventType = node.path("eventType").asText();
        long version = node.path("version").asLong();
        Long sequence = node.has("sequence") ? node.path("sequence").asLong() : null;
        JsonNode payload = node.path("payload").isMissingNode() ? MAPPER.createObjectNode() : node.path("payload");

        JsonNode metaNode = node.path("metadata");
        EventMetadata metadata = null;
        if (!metaNode.isMissingNode()) {
            String eventId = metaNode.path("eventId").asText(metaNode.path("event_id").asText());
            String createdAtStr = metaNode.path("createdAt").asText(metaNode.path("created_at").asText());
            Instant createdAt = createdAtStr.isBlank() ? Instant.EPOCH : Instant.parse(createdAtStr);
            JsonNode issued = metaNode.path("issuedBy");
            ActorClaims issuedBy = null;
            if (!issued.isMissingNode()) {
                issuedBy = new ActorClaims(issued.path("group").asText(null), issued.path("user").asText(null));
            }
            String note = metaNode.path("note").asText(null);
            metadata = new EventMetadata(eventId, createdAt, issuedBy, note);
        }

        String hash = node.path("hash").asText("");
        String merkleRoot = node.path("merkleRoot").asText("");
        return new EventRecord(aggType, aggId, eventType, version, sequence, payload, metadata, hash, merkleRoot);
    }

    private String json(JsonNode node) {
        try {
            return MAPPER.writeValueAsString(node);
        } catch (IOException e) {
            throw new EventDbxException("Failed to serialize JSON payload", e);
        }
    }

    private JsonNode parseJson(String raw) {
        try {
            return MAPPER.readTree(raw);
        } catch (IOException e) {
            throw new EventDbxException("Failed to parse selection JSON", e);
        }
    }

    private String readString(org.capnproto.Text.Reader reader) {
        try {
            return reader.toString();
        } catch (Exception e) {
            return "";
        }
    }

    /**
     * Derive the 32-byte PSK for Noise from the control token, matching the JS client.
     */
    byte[] derivePsk(String token) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            return digest.digest(token.getBytes(StandardCharsets.UTF_8));
        } catch (NoSuchAlgorithmException e) {
            throw new EventDbxException("SHA-256 not available for Noise PSK derivation", e);
        }
    }
}
