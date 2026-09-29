package io.quarkiverse.nats.cache.runtime.codec;

import java.lang.reflect.Field;
import java.util.Base64;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import io.quarkus.cache.CompositeCacheKey;
import io.quarkus.cache.DefaultCacheKey;

/**
 * Uniform codec between arbitrary Java cache keys and NATS KV subject-safe strings (design D4).
 *
 * <p>Every key — plain values, POJOs, {@link CompositeCacheKey}, {@link DefaultCacheKey} — is serialized
 * with Jackson (embedded type info) and base64url-encoded (no padding), producing an opaque but fully
 * reversible subject token. Decoding is the exact inverse, so stored keys always round-trip back to the
 * original object; this is what makes {@code invalidateIf(predicate)} work uniformly: every stored key can
 * be decoded back into a Java object for predicate evaluation.
 *
 * <p>Constraints enforced here (spec: *Key type support and clear failure*, *Key length and determinism*):
 * <ul>
 *   <li>keys whose class is not reconstructable by Jackson are rejected at store time with an error naming
 *       the offending class (validated once per class, then memoized);</li>
 *   <li>encoded keys longer than {@link #MAX_ENCODED_KEY_LENGTH} characters are rejected at store time.</li>
 * </ul>
 */
public final class KeyCodec {

    /** NATS subject tokens are limited to 255 characters; KV keys must fit in one token. */
    public static final int MAX_ENCODED_KEY_LENGTH = 255;

    private static final Map<Class<?>, Boolean> ROUND_TRIP_VALIDATED = new ConcurrentHashMap<>();

    private KeyCodec() {
    }

    /**
     * Encodes a cache key into its subject-safe base64url form.
     *
     * @param key the cache key; must not be null and must be of a Jackson-reconstructable class
     * @return the encoded key, at most {@link #MAX_ENCODED_KEY_LENGTH} characters long
     * @throws CacheKeyEncodingException if the key is null, not serializable/reconstructable, or too long
     */
    public static String encode(Object key) {
        Objects.requireNonNull(key, "cache keys must not be null");
        byte[] json = toJson(key);
        String encoded = Base64.getUrlEncoder().withoutPadding().encodeToString(json);
        if (encoded.length() > MAX_ENCODED_KEY_LENGTH) {
            throw new CacheKeyEncodingException(String.format(
                    "Encoded cache key of class '%s' is %d characters long, exceeding the NATS KV limit of %d characters. Use a shorter key.",
                    key.getClass().getName(), encoded.length(), MAX_ENCODED_KEY_LENGTH));
        }
        validateRoundTrip(key, json);
        return encoded;
    }

    /**
     * Decodes a stored (subject-safe) cache key back into its original Java object.
     *
     * @param encoded the encoded key as stored in the bucket
     * @return the reconstructed key object
     * @throws CacheKeyEncodingException if the token is not valid base64url or the payload is not decodable
     */
    public static Object decode(String encoded) {
        Objects.requireNonNull(encoded, "encoded cache key must not be null");
        byte[] json;
        try {
            json = Base64.getUrlDecoder().decode(encoded);
        } catch (IllegalArgumentException e) {
            throw new CacheKeyEncodingException("Stored cache key is not valid base64url: " + encoded, e);
        }
        return fromJson(json);
    }

    private static byte[] toJson(Object key) {
        try {
            if (key instanceof CompositeCacheKey composite) {
                // CompositeCacheKey has no no-arg constructor and its elements are heterogeneous, so it is
                // encoded explicitly as a typed object whose "keyElements" array carries per-element type info.
                ObjectNode root = NatsCacheJson.PLAIN.createObjectNode();
                root.put(NatsCacheJson.TYPE_PROPERTY, CompositeCacheKey.class.getName());
                ArrayNode elements = root.putArray("keyElements");
                for (Object element : composite.getKeyElements()) {
                    elements.add(typedNode(element));
                }
                return NatsCacheJson.PLAIN.writeValueAsBytes(root);
            }
            if (key instanceof DefaultCacheKey defaultKey) {
                // DefaultCacheKey exposes no getter and has no no-arg constructor; encode it explicitly.
                ObjectNode root = NatsCacheJson.PLAIN.createObjectNode();
                root.put(NatsCacheJson.TYPE_PROPERTY, DefaultCacheKey.class.getName());
                root.put("cacheName", cacheNameOf(defaultKey));
                return NatsCacheJson.PLAIN.writeValueAsBytes(root);
            }
            // Everything else: the typed mapper embeds "@class" for POJOs and wraps Long values.
            return NatsCacheJson.TYPED.writeValueAsBytes(key);
        } catch (CacheKeyEncodingException e) {
            throw e;
        } catch (Exception e) {
            throw new CacheKeyEncodingException(
                    "Cannot serialize cache key of class '" + key.getClass().getName() + "': " + e.getMessage(), e);
        }
    }

    private static Object fromJson(byte[] json) {
        try {
            JsonNode root = NatsCacheJson.PLAIN.readTree(json);
            if (root.isObject()) {
                String type = root.path(NatsCacheJson.TYPE_PROPERTY).asText(null);
                if (CompositeCacheKey.class.getName().equals(type)) {
                    JsonNode elements = root.path("keyElements");
                    Object[] array = new Object[elements.size()];
                    int i = 0;
                    for (JsonNode element : elements) {
                        array[i++] = typedValue(element);
                    }
                    return new CompositeCacheKey(array);
                }
                if (DefaultCacheKey.class.getName().equals(type)) {
                    return new DefaultCacheKey(root.path("cacheName").asText());
                }
            }
            // Plain values and typed POJOs: default typing drives reconstruction of the exact runtime class.
            return NatsCacheJson.TYPED.readValue(json, Object.class);
        } catch (CacheKeyEncodingException e) {
            throw e;
        } catch (Exception e) {
            throw new CacheKeyEncodingException("Cannot decode stored cache key: " + e.getMessage(), e);
        }
    }

    /** Serializes an object with the typed mapper and parses the result into a plain tree node. */
    private static JsonNode typedNode(Object value) throws Exception {
        return NatsCacheJson.PLAIN.readTree(NatsCacheJson.TYPED.writeValueAsBytes(value));
    }

    /** Re-serializes a tree node and deserializes it with the typed mapper to recover the exact runtime class. */
    private static Object typedValue(JsonNode node) throws Exception {
        return NatsCacheJson.TYPED.readValue(NatsCacheJson.PLAIN.writeValueAsBytes(node), Object.class);
    }

    /**
     * Fail-fast validation (design D4): on first use of a key class, attempt a trial reconstruction and
     * reject classes Jackson cannot build back, naming the offending class. Results are memoized per class.
     */
    private static void validateRoundTrip(Object key, byte[] json) {
        Class<?> keyClass = key.getClass();
        if (ROUND_TRIP_VALIDATED.containsKey(keyClass)) {
            return;
        }
        try {
            if (key instanceof CompositeCacheKey composite) {
                for (Object element : composite.getKeyElements()) {
                    validateElement(element);
                }
            } else {
                fromJson(json); // trial reconstruction; throws CacheKeyEncodingException if not reconstructable
            }
        } catch (CacheKeyEncodingException e) {
            throw new CacheKeyEncodingException(
                    "Cache key class '" + keyClass.getName() + "' is not supported by the NATS KV cache codec: "
                            + e.getMessage(),
                    e);
        }
        ROUND_TRIP_VALIDATED.put(keyClass, Boolean.TRUE);
    }

    private static void validateElement(Object element) {
        Class<?> elementClass = element.getClass();
        if (ROUND_TRIP_VALIDATED.containsKey(elementClass)) {
            return;
        }
        try {
            fromJson(toJson(element)); // trial round-trip of the element alone
        } catch (CacheKeyEncodingException e) {
            throw new CacheKeyEncodingException(
                    "Composite cache key element class '" + elementClass.getName() + "' is not supported by the NATS KV cache codec: "
                            + e.getMessage(),
                    e);
        }
        ROUND_TRIP_VALIDATED.put(elementClass, Boolean.TRUE);
    }

    private static String cacheNameOf(DefaultCacheKey key) {
        try {
            Field field = DefaultCacheKey.class.getDeclaredField("cacheName");
            field.setAccessible(true);
            return (String) field.get(key);
        } catch (ReflectiveOperationException e) {
            throw new CacheKeyEncodingException(
                    "Cannot read the cache name of a DefaultCacheKey key: " + e.getMessage(), e);
        }
    }
}
