package io.quarkiverse.nats.cache.runtime.codec;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.NullNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * Codec between Java cache values and the stored payload envelope (design D5).
 *
 * <p>Every stored payload is one universal JSON envelope:
 * <pre>{@code { "exp": <epoch-millis, optional>, "data": <typed value encoding> }}</pre>
 *
 * <ul>
 *   <li>{@code data} carries the Jackson default-typing encoding of the value, so {@code get()} can
 *       reconstruct the exact runtime class despite generic erasure of {@code Uni<V>};</li>
 *   <li>a cached {@code null} result is stored as the same envelope with {@code "data": null} (KV cannot
 *       store null), which suppresses the loader until the entry is invalidated or expires;</li>
 *   <li>{@code exp} is the optional logical per-item TTL (design D6); a read path that sees
 *       {@code exp < now} must treat the entry as a miss and re-run the loader.</li>
 * </ul>
 *
 * <p>The envelope structure is built with the plain mapper; the {@code data} member is (de)serialized
 * with the typed mapper, since tree operations are not possible on the typed mapper (see
 * {@link NatsCacheJson}).
 */
public final class ValueCodec {

    private static final String EXP = "exp";
    private static final String DATA = "data";

    private ValueCodec() {
    }

    /**
     * Encodes a cache value (or {@code null}) into the envelope payload.
     *
     * @param value the value to store; {@code null} is stored as the cached-null sentinel ({@code data: null})
     * @param expiresAtEpochMillis logical expiration timestamp (epoch millis), or {@code null} for no per-item expiration
     * @return the envelope payload bytes
     * @throws CacheValueException if the value cannot be serialized
     */
    public static byte[] encode(Object value, Long expiresAtEpochMillis) {
        try {
            ObjectNode root = NatsCacheJson.PLAIN.createObjectNode();
            if (expiresAtEpochMillis != null) {
                root.put(EXP, expiresAtEpochMillis);
            }
            root.set(DATA, value == null ? NullNode.getInstance()
                    : NatsCacheJson.PLAIN.readTree(NatsCacheJson.TYPED.writeValueAsBytes(value)));
            return NatsCacheJson.PLAIN.writeValueAsBytes(root);
        } catch (Exception e) {
            throw new CacheValueException("Cannot serialize cache value of class '"
                    + (value == null ? "null" : value.getClass().getName()) + "': " + e.getMessage(), e);
        }
    }

    /**
     * Decodes an envelope payload back into its value and logical expiration.
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
        Long exp = null;
        if (root.has(EXP) && !root.get(EXP).isNull()) {
            if (!root.get(EXP).isNumber()) {
                throw new CacheValueException("Stored cache value envelope has a non-numeric 'exp' member: " + root);
            }
            exp = root.get(EXP).asLong();
        }
        Object data;
        try {
            JsonNode dataNode = root.get(DATA);
            data = dataNode.isNull() ? null
                    : NatsCacheJson.TYPED.readValue(NatsCacheJson.PLAIN.writeValueAsBytes(dataNode), Object.class);
        } catch (Exception e) {
            throw new CacheValueException("Cannot deserialize stored cache value: " + e.getMessage(), e);
        }
        return new Envelope(data, exp);
    }

    /**
     * A decoded envelope: the cached value plus its optional logical expiration.
     *
     * @param data the cached value; {@code null} means a cached-null result (loader must be suppressed)
     * @param expiresAtEpochMillis logical expiration (epoch millis), or {@code null} if none was set
     */
    public record Envelope(Object data, Long expiresAtEpochMillis) {

        /**
         * @return {@code true} if a logical expiration was set and is in the past relative to {@code nowEpochMillis}
         */
        public boolean isExpired(long nowEpochMillis) {
            return expiresAtEpochMillis != null && expiresAtEpochMillis < nowEpochMillis;
        }
    }
}
