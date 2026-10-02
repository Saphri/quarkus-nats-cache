package io.quarkiverse.nats.cache.runtime.codec;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.databind.jsontype.impl.LaissezFaireSubTypeValidator;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

/**
 * Shared Jackson configuration for the key and value codecs.
 *
 * <p>Two mappers are kept deliberately separate because of a Jackson constraint: with default typing
 * ({@code EVERYTHING}, {@code As.PROPERTY}) enabled, tree operations such as {@code readTree} /
 * {@code valueToTree} fail — they deserialize into {@code JsonNode}, which Jackson then tries to resolve
 * as a typed subtype and rejects. Therefore:
 * <ul>
 *   <li>{@link #TYPED} embeds type info (class ids, {@code As.PROPERTY}) so that
 *       {@code readValue(json, Object.class)} reconstructs the exact runtime class despite generic
 *       erasure of {@code Uni<V>}. It is used only for serializing/deserializing whole objects via
 *       {@code writeValueAsBytes} / {@code readValue(..., Object.class)} — never for tree operations;</li>
 *   <li>{@link #PLAIN} performs all structural work (envelope and composite-key trees) without any typing.</li>
 * </ul>
 *
 * <p>Typing behavior (verified empirically on Jackson 2.17 through 2.22): natural types (String, Integer,
 * Double, Boolean) serialize without a marker and decode back to their natural Java types; {@code Long} is
 * wrapped as {@code ["java.lang.Long",42]} so it stays distinct from {@code Integer}; POJOs get an
 * {@code @class} property carrying the exact runtime class name.
 *
 * <p><b>Typing mode:</b> {@link ObjectMapper.DefaultTyping#EVERYTHING} is deprecated since Jackson 2.17
 * (removed in Jackson 3.0) and the documented alternative {@code NON_FINAL_AND_ENUMS} was rejected after
 * empirical verification on Jackson 2.21.x: it does not type <em>final</em> classes, so final POJOs and
 * immutable collections ({@code List.of}, {@code Map.of}) serialize without a type id and fail to decode
 * back into their original shape. {@code EVERYTHING} is the only mode in which all of those round-trip
 * (verified: scalars, enums, final POJOs, mutable and immutable collections). Quarkus 3.x pins Jackson 2.x,
 * where this setting remains fully functional; if a future Quarkus ships Jackson 3, the codec must be
 * revisited as part of that major migration.
 */
@SuppressWarnings("deprecation") // DefaultTyping.EVERYTHING: see class javadoc for the verified rationale
final class NatsCacheJson {

    /** Type-marker property name Jackson uses for default typing with {@code As.PROPERTY}. */
    static final String TYPE_PROPERTY = "@class";

    /** Typed mapper: whole-object (de)serialization only — never call tree operations on it. */
    static final ObjectMapper TYPED = createTypedMapper();

    /** Plain mapper: structural JSON (envelopes, composite-key trees) without default typing. */
    static final ObjectMapper PLAIN = new ObjectMapper();

    private NatsCacheJson() {
    }

    private static ObjectMapper createTypedMapper() {
        ObjectMapper mapper = JsonMapper.builder()
                .activateDefaultTyping(
                        LaissezFaireSubTypeValidator.instance,
                        ObjectMapper.DefaultTyping.EVERYTHING,
                        JsonTypeInfo.As.PROPERTY)
                .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
                .build();
        // jackson-datatype-jsr310 is provided by quarkus-jackson; register it so JSR-310 date/time
        // types round-trip instead of failing with "Java 8 date/time type not supported by default".
        mapper.registerModule(new JavaTimeModule());
        return mapper;
    }
}
