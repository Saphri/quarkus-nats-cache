package io.quarkiverse.nats.cache.runtime.codec;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Set;

import org.junit.jupiter.api.Test;

class CacheTypeAllowListTest {

    private final CacheTypeAllowList allowList = new CacheTypeAllowList(
            Set.of(CacheTypeAllowListTest.class.getPackageName()), List.of("org.example.allowed."));
    private final KeyCodec keyCodec = new KeyCodec(allowList);
    private final ValueCodec valueCodec = new ValueCodec(allowList);

    public static final class Versioned {
        private String name;

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }
    }

    private static byte[] envelope(String data) {
        return ("{\"data\":" + data + "}").getBytes(StandardCharsets.UTF_8);
    }

    @Test
    void storedValueNamingAForeignClassIsRefused() {
        // A planted payload must not get Jackson to instantiate an arbitrary class from the classpath.
        assertThatThrownBy(() -> valueCodec.decode(envelope("{\"@class\":\"com.example.Gadget\",\"x\":1}")))
                .isInstanceOf(CacheValueException.class)
                .hasMessageContaining("denied");
    }

    @Test
    void jdkSubPackagesAreNotAllowedByTheirParent() {
        assertThatThrownBy(() -> valueCodec.decode(envelope("{\"@class\":\"java.util.logging.FileHandler\"}")))
                .isInstanceOf(CacheValueException.class)
                .hasMessageContaining("denied");
    }

    @Test
    void storedKeyNamingAForeignClassIsRefused() {
        String encoded = java.util.Base64.getUrlEncoder().withoutPadding()
                .encodeToString("{\"@class\":\"com.example.Gadget\"}".getBytes(StandardCharsets.UTF_8));
        assertThatThrownBy(() -> keyCodec.decode(encoded)).isInstanceOf(CacheKeyEncodingException.class);
    }

    @Test
    void foreignClassInsideAnArrayIsRefused() {
        assertThatThrownBy(() -> valueCodec.decode(envelope("[\"[Lcom.example.Gadget;\",[]]")))
                .isInstanceOf(CacheValueException.class);
    }

    @Test
    void decisionsFollowPackagesNotPrefixesOfNames() {
        assertThat(allowList.isAllowed("java.lang.String")).isTrue();
        assertThat(allowList.isAllowed("java.util.ImmutableCollections$ListN")).isTrue();
        assertThat(allowList.isAllowed("java.time.Instant")).isTrue();
        assertThat(allowList.isAllowed("java.util.concurrent.ConcurrentHashMap")).isFalse();
        assertThat(allowList.isAllowed("javax.naming.InitialContext")).isFalse();
        assertThat(allowList.isAllowed("org.example.allowed.Model")).isTrue();
        assertThat(allowList.isAllowed("org.example.allowed.sub.Model")).isTrue();
        assertThat(allowList.isAllowed("org.example.allowedtoo.Model")).isFalse();
    }

    @Test
    void entriesWrittenByAnOlderClassVersionStillDecode() {
        // A field removed from a cached class must not make every old entry undecodable after a deploy.
        String data = "{\"@class\":\"" + Versioned.class.getName() + "\",\"name\":\"n\",\"removedField\":42}";
        Object decoded = valueCodec.decode(envelope(data)).data();
        assertThat(decoded).isInstanceOf(Versioned.class);
        assertThat(((Versioned) decoded).getName()).isEqualTo("n");
    }

    @Test
    void differentlyConfiguredCodecsDoNotShareState() {
        // Two applications in one JVM (test suites, dev-mode restarts) must not widen each other's trust boundary.
        CacheTypeAllowList fixtures = new CacheTypeAllowList(Set.of(CacheTypeAllowListTest.class.getPackageName()),
                List.of());
        CacheTypeAllowList jdkOnly = CacheTypeAllowList.jdkOnly();
        byte[] payload = new ValueCodec(fixtures).encode(new Versioned());

        assertThat(new ValueCodec(fixtures).decode(payload).data()).isInstanceOf(Versioned.class);
        assertThatThrownBy(() -> new ValueCodec(jdkOnly).decode(payload))
                .isInstanceOf(CacheValueException.class)
                .hasMessageContaining("denied");
        // ...and the other way round: building the narrower codec did not narrow the wider one.
        assertThat(new ValueCodec(fixtures).decode(payload).data()).isInstanceOf(Versioned.class);

        assertThat(fixtures.isAllowed(Versioned.class.getName())).isTrue();
        assertThat(jdkOnly.isAllowed(Versioned.class.getName())).isFalse();
        assertThat(allowList.isAllowed("org.example.allowed.Model")).isTrue();
        assertThat(fixtures.isAllowed("org.example.allowed.Model")).isFalse();
    }

    @Test
    void keyCodecsWithDifferentAllowListsDoNotShareTheirValidationMemo() {
        KeyCodec wide = new KeyCodec(new CacheTypeAllowList(Set.of(CacheTypeAllowListTest.class.getPackageName()),
                List.of()));
        KeyCodec narrow = new KeyCodec(CacheTypeAllowList.jdkOnly());
        Versioned key = new Versioned();
        key.setName("k");

        assertThat(wide.encode(key)).isNotEmpty();
        // The wide codec having validated the class must not let the narrow one accept it.
        assertThatThrownBy(() -> narrow.encode(key)).isInstanceOf(CacheKeyEncodingException.class);
    }
}
