package org.bsc.langgraph4j.checkpoint;

import static java.lang.String.format;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeDefinition;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.BillingMode;
import software.amazon.awssdk.services.dynamodb.model.GetItemResponse;
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement;
import software.amazon.awssdk.services.dynamodb.model.KeyType;
import software.amazon.awssdk.services.dynamodb.model.QueryRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryResponse;
import software.amazon.awssdk.services.dynamodb.model.ResourceInUseException;
import software.amazon.awssdk.services.dynamodb.model.ResourceNotFoundException;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;

/**
 * Low-level DynamoDB operations for the {@link DynamoDBSaver}.
 *
 * <h2>Key-naming conventions (single-table design)</h2>
 * <pre>
 *   PK                                     SK                Description
 *   ─────────────────────────────────────  ────────────────  ─────────────────────────────
 *   CHECKPOINT_{threadId}                  {checkpointId}    Checkpoint metadata item
 *   CHUNK_{threadId}#{checkpointId}        CHUNK             Serialized state payload
 *   RELEASED_{threadId}                    MARKER            Thread-released sentinel
 * </pre>
 *
 * <p>
 * All item attributes:
 * <ul>
 * <li>{@code PK} – partition key</li>
 * <li>{@code SK} – sort key</li>
 * <li>{@code checkpointId} – UUID string</li>
 * <li>{@code nodeId} – current node name</li>
 * <li>{@code nextNodeId} – next node name</li>
 * <li>{@code contentType} – serializer content-type string</li>
 * <li>{@code payload} – binary; only present in CHUNK items</li>
 * <li>{@code ttl} – optional epoch-second number for DynamoDB TTL</li>
 * </ul>
 */
class DynamoDBRepository {

    private static final Logger log = LoggerFactory.getLogger(DynamoDBRepository.class);

    // ─── PK/SK generators ───────────────────────────────────────────────────────
    static String checkpointPK(String threadId) {
        return "CHECKPOINT_" + threadId;
    }

    static String checkpointSK(String checkpointId) {
        return checkpointId;
    }

    static String chunkPK(String threadId, String checkpointId) {
        return format("CHUNK_%s#%s", threadId, checkpointId);
    }

    static String chunkSK() {
        return "CHUNK";
    }

    static String releasedPK(String threadId) {
        return "RELEASED_" + threadId;
    }

    static String releasedSK() {
        return "MARKER";
    }

    // ─── Record type returned to DynamoDBSaver ──────────────────────────────────
    /**
     * Immutable view of a checkpoint as loaded from DynamoDB.
     */
    record CheckpointRecord(
            String threadId,
            String checkpointId,
            String nodeId,
            String nextNodeId,
            byte[] payload,
            String contentType,
            Long savedAt
            ) {

    }

    // ─── Fields ─────────────────────────────────────────────────────────────────
    private final DynamoDbClient client;
    private final String tableName;
    private final Long ttlSeconds;

    // ─── Constructor ─────────────────────────────────────────────────────────────
    DynamoDBRepository(DynamoDbClient client, String tableName, Long ttlSeconds) {
        this.client = client;
        this.tableName = tableName;
        this.ttlSeconds = ttlSeconds;
    }

    // ─── Table lifecycle ─────────────────────────────────────────────────────────
    /**
     * Creates the DynamoDB table with {@code PK} (HASH) + {@code SK} (RANGE)
     * composite key and {@code PAY_PER_REQUEST} billing. No-ops if the table
     * already exists.
     */
    void createTableIfNotExists() {
        try {
            client.createTable(r -> r
                    .tableName(tableName)
                    .keySchema(
                            KeySchemaElement.builder().attributeName("PK").keyType(KeyType.HASH).build(),
                            KeySchemaElement.builder().attributeName("SK").keyType(KeyType.RANGE).build()
                    )
                    .attributeDefinitions(
                            AttributeDefinition.builder().attributeName("PK")
                                    .attributeType(ScalarAttributeType.S).build(),
                            AttributeDefinition.builder().attributeName("SK")
                                    .attributeType(ScalarAttributeType.S).build()
                    )
                    .billingMode(BillingMode.PAY_PER_REQUEST)
            );

            log.info("Created DynamoDB table '{}'", tableName);

            // Wait until the table is active
            client.waiter().waitUntilTableExists(r -> r.tableName(tableName));
            log.debug("DynamoDB table '{}' is now ACTIVE", tableName);

        } catch (ResourceInUseException e) {
            // Table already exists – no action needed
            log.debug("DynamoDB table '{}' already exists, skipping creation", tableName);
        }
    }

    /**
     * Deletes the table. Waits until the deletion is confirmed. No-ops if the
     * table does not exist.
     */
    void dropTable() {
        try {
            client.deleteTable(r -> r.tableName(tableName));
            log.info("Deleted DynamoDB table '{}'", tableName);
            client.waiter().waitUntilTableNotExists(r -> r.tableName(tableName));
        } catch (ResourceNotFoundException e) {
            log.debug("DynamoDB table '{}' not found – nothing to drop", tableName);
        }
    }

    // ─── Checkpoint operations ───────────────────────────────────────────────────
    /**
     * Persists a checkpoint metadata item and its associated payload chunk
     * item.
     *
     * @param threadId thread identifier
     * @param checkpointId UUID of the checkpoint
     * @param nodeId current graph node
     * @param nextNodeId next graph node
     * @param payload serialized state bytes
     * @param contentType serializer content-type string
     */
    void putCheckpoint(String threadId,
            String checkpointId,
            String nodeId,
            String nextNodeId,
            byte[] payload,
            String contentType) {

        // ── 1. Metadata item ──────────────────────────────────────────────────
        Map<String, AttributeValue> metaItem = new HashMap<>();
        metaItem.put("PK", s(checkpointPK(threadId)));
        metaItem.put("SK", s(checkpointSK(checkpointId)));
        metaItem.put("checkpointId", s(checkpointId));
        metaItem.put("nodeId", s(nodeId));
        metaItem.put("nextNodeId", s(nextNodeId));
        metaItem.put("contentType", s(contentType));
        metaItem.put("savedAt", n(Instant.now().toEpochMilli()));

        if (ttlSeconds != null) {
            metaItem.put("ttl", n(Instant.now().getEpochSecond() + ttlSeconds));
        }

        client.putItem(r -> r.tableName(tableName).item(metaItem));
        log.debug("Stored checkpoint metadata: threadId={}, checkpointId={}", threadId, checkpointId);

        // ── 2. Payload chunk item ─────────────────────────────────────────────
        Map<String, AttributeValue> chunkItem = new HashMap<>();
        chunkItem.put("PK", s(chunkPK(threadId, checkpointId)));
        chunkItem.put("SK", s(chunkSK()));
        chunkItem.put("payload", b(payload));

        if (ttlSeconds != null) {
            chunkItem.put("ttl", n(Instant.now().getEpochSecond() + ttlSeconds));
        }

        client.putItem(r -> r.tableName(tableName).item(chunkItem));
        log.debug("Stored payload chunk: threadId={}, checkpointId={}, size={}B",
                threadId, checkpointId, payload.length);
    }

    /**
     * Loads all checkpoint records for the given thread, sorted newest-first.
     * Each record's payload is fetched from the associated chunk item.
     *
     * @param threadId thread identifier
     * @return list of records, ordered by checkpointId descending (UUIDs are
     * time-based when v7, but for lexicographic ordering we rely on DynamoDB's
     * SK scan-forward=false)
     */
    List<CheckpointRecord> loadCheckpointRecords(String threadId) {
        String pk = checkpointPK(threadId);

        QueryRequest query = QueryRequest.builder()
                .tableName(tableName)
                .keyConditionExpression("PK = :pk")
                .expressionAttributeValues(Map.of(":pk", s(pk)))
                .scanIndexForward(false) // newest SK first
                .build();

        List<CheckpointRecord> records = new ArrayList<>();

        // Paginate through all results
        String lastEvaluatedKey = null;
        Map<String, AttributeValue> exclusiveStartKey = null;

        do {
            QueryRequest.Builder reqBuilder = query.toBuilder();
            if (exclusiveStartKey != null) {
                reqBuilder.exclusiveStartKey(exclusiveStartKey);
            }

            QueryResponse response = client.query(reqBuilder.build());

            for (Map<String, AttributeValue> item : response.items()) {
                String checkpointId = item.get("checkpointId").s();
                String nodeId = item.get("nodeId").s();
                String nextNodeId = item.get("nextNodeId").s();
                String contentType = item.get("contentType").s();
                Long savedAt = item.containsKey("savedAt") ? Long.parseLong(item.get("savedAt").n()) : 0L;

                // Fetch payload from chunk item
                byte[] payload = loadChunkPayload(threadId, checkpointId);
                if (payload == null) {
                    log.warn("Payload chunk not found for checkpointId='{}', thread='{}' – skipping",
                            checkpointId, threadId);
                    continue;
                }

                records.add(new CheckpointRecord(threadId, checkpointId, nodeId, nextNodeId, payload, contentType, savedAt));
            }

            exclusiveStartKey = response.hasLastEvaluatedKey() ? response.lastEvaluatedKey() : null;

        } while (exclusiveStartKey != null);

        log.debug("Loaded {} checkpoint record(s) for thread '{}'", records.size(), threadId);
        records.sort(Comparator.comparing(CheckpointRecord::savedAt).reversed());
        return records;
    }

    /**
     * Fetches the serialized payload for a single checkpoint from its chunk
     * item.
     *
     * @return the raw bytes, or {@code null} if the item is not found
     */
    byte[] loadChunkPayload(String threadId, String checkpointId) {
        GetItemResponse response = client.getItem(r -> r
                .tableName(tableName)
                .key(Map.of(
                        "PK", s(chunkPK(threadId, checkpointId)),
                        "SK", s(chunkSK())
                ))
                .projectionExpression("payload")
        );

        if (!response.hasItem() || !response.item().containsKey("payload")) {
            return null;
        }

        return response.item().get("payload").b().asByteArray();
    }

    /**
     * Deletes the checkpoint metadata item and its associated payload chunk
     * item.
     *
     * @param threadId thread identifier
     * @param checkpointId UUID of the checkpoint to delete
     */
    void deleteCheckpoint(String threadId, String checkpointId) {
        // Delete metadata item
        client.deleteItem(r -> r
                .tableName(tableName)
                .key(Map.of(
                        "PK", s(checkpointPK(threadId)),
                        "SK", s(checkpointSK(checkpointId))
                ))
        );

        // Delete chunk item
        client.deleteItem(r -> r
                .tableName(tableName)
                .key(Map.of(
                        "PK", s(chunkPK(threadId, checkpointId)),
                        "SK", s(chunkSK())
                ))
        );

        log.debug("Deleted checkpoint '{}' for thread '{}'", checkpointId, threadId);
    }

    // ─── Thread release operations ───────────────────────────────────────────────
    /**
     * Writes a sentinel item that marks this thread as "released" (archived).
     * Subsequent calls to {@link #isThreadReleased(String)} will return
     * {@code true}.
     */
    void markThreadReleased(String threadId) {
        Map<String, AttributeValue> item = new HashMap<>();
        item.put("PK", s(releasedPK(threadId)));
        item.put("SK", s(releasedSK()));

        if (ttlSeconds != null) {
            item.put("ttl", n(Instant.now().getEpochSecond() + ttlSeconds));
        }

        client.putItem(r -> r.tableName(tableName).item(item));
        log.info("Marked thread '{}' as released", threadId);
    }

    /**
     * Returns {@code true} if a released sentinel item exists for the given
     * thread.
     */
    boolean isThreadReleased(String threadId) {
        GetItemResponse response = client.getItem(r -> r
                .tableName(tableName)
                .key(Map.of(
                        "PK", s(releasedPK(threadId)),
                        "SK", s(releasedSK())
                ))
                .projectionExpression("PK")
        );
        return response.hasItem();
    }

    // ─── AttributeValue helpers ──────────────────────────────────────────────────
    private static AttributeValue s(String value) {
        return AttributeValue.builder().s(value).build();
    }

    private static AttributeValue n(long value) {
        return AttributeValue.builder().n(String.valueOf(value)).build();
    }

    private static AttributeValue b(byte[] value) {
        return AttributeValue.builder().b(SdkBytes.fromByteArray(value)).build();
    }
}
