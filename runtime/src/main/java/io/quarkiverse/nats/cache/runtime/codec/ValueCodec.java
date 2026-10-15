package io.quarkiverse.nats.cache.runtime.codec;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.NullNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * Codec between Java cache values and the stored payload envelope (design D5).
 *
 * <p>Every stored payload is one universal JSON envelope:
 * <pre>{@code { "data": <typed value encoding> }}</pre>
 *
 * <ul>
 *   <li>{@code data} carries the Jackson default-typing encoding of the value, so {@code get()} can
 *       reconstruct the exact runtime class despite generic erasure of {@code Uni<V>};</li>
 *   <li>a cached {@code null} result is stored as the same envelope with {@code "data": null} (KV cannot
 *       store null), which suppresses the loader until the entry is invalidated or expires.</li>
 * </ul>
 *
 * <p>Expiration is not part of the payload: per-item expiration is a server-side per-message TTL on the KV write
 * (design D6), so an expired entry is simply gone from the bucket.
 *
 * <p>The envelope structure is built with the plain mapper; the {@code data} member is (de)serialized
 * with the typed mapper, since tree operations are not possible on the typed mapper (see
 * {@link NatsCacheJson}).
 */
public final class ValueCodec {

    private static final String DATA = "data";

    private ValueCodec() {
    }

    /**
     * Encodes a cache value (or {@code null}) into the envelope payload.
     *
     * @param value the value to store; {@code null} is stored as the cached-null sentinel ({@code data: null})
     * @return the envelope payload bytes
     * @throws CacheValueException if the value cannot be serialized
     */
    public static byte[] encode(Object value) {
        try {
            ObjectNode root = NatsCacheJson.PLAIN.createObjectNode();
            root.set(DATA, value == null ? NullNode.getInstance()
                    : NatsCacheJson.PLAIN.readTree(NatsCacheJson.TYPED.writeValueAsBytes(value)));
            return NatsCacheJson.PLAIN.writeValueAsBytes(root);
        } catch (Exception e) {
            throw new CacheValueException("Cannot serialize cache value of class '"
                    + (value == null ? "null" : value.getClass().getName()) + "': " + e.getMessage(), e);
        }
    }

    /**
     * Decodes an envelope payload back into its value.
     *
     * @param payload the stored bytes; must not be null
     * @return the decoded envelope (never null)
     * @throws CacheValueException if the payload is not a valid NATS KV cache envelope (unknown/legacy data)
     */
    public static Envelope decode(byte[] payload) {
        if (payload == null) {
            throw new CacheValueException("Stored cache value payload is null");
        }
        JsonNode root;
        try {
            root = NatsCacheJson.PLAIN.readTree(payload);
        } catch (Exception e) {
            throw new CacheValueException("Stored cache value is not valid JSON: " + e.getMessage(), e);
        }
        if (!root.isObject() || !root.has(DATA)) {
            throw new CacheValueException(
                    "Stored cache value is not a NATS KV cache envelope (missing 'data' member): " + root);
        }
        Object data;
        try {
            JsonNode dataNode = root.get(DATA);
            data = dataNode.isNull() ? null
                    : NatsCacheJson.TYPED.readValue(NatsCacheJson.PLAIN.writeValueAsBytes(dataNode), Object.class);
        } catch (Exception e) {
            throw new CacheValueException("Cannot deserialize stored cache value: " + e.getMessage(), e);
        }
        return new Envelope(data);
    }

    /**
     * A decoded envelope.
     *
     * @param data the cached value; {@code null} means a cached-null result (loader must be suppressed)
     */
    public record Envelope(Object data) {
    }
}
