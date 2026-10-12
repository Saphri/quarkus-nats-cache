package io.quarkiverse.nats.cache.runtime.codec;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Set;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class CacheTypeAllowListTest {

    @BeforeAll
    static void allowFixtureTypes() {
        CacheTypeAllowList.allowExactPackages(Set.of(CacheTypeAllowListTest.class.getPackageName()));
        CacheTypeAllowList.allowPackagePrefixes(List.of("org.example.allowed."));
    }

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
        assertThatThrownBy(() -> ValueCodec.decode(envelope("{\"@class\":\"com.example.Gadget\",\"x\":1}")))
                .isInstanceOf(CacheValueException.class)
                .hasMessageContaining("denied");
    }

    @Test
    void jdkSubPackagesAreNotAllowedByTheirParent() {
        assertThatThrownBy(() -> ValueCodec.decode(envelope("{\"@class\":\"java.util.logging.FileHandler\"}")))
                .isInstanceOf(CacheValueException.class)
                .hasMessageContaining("denied");
    }

    @Test
    void storedKeyNamingAForeignClassIsRefused() {
        String encoded = java.util.Base64.getUrlEncoder().withoutPadding()
                .encodeToString("{\"@class\":\"com.example.Gadget\"}".getBytes(StandardCharsets.UTF_8));
        assertThatThrownBy(() -> KeyCodec.decode(encoded)).isInstanceOf(CacheKeyEncodingException.class);
    }

    @Test
    void foreignClassInsideAnArrayIsRefused() {
        assertThatThrownBy(() -> ValueCodec.decode(envelope("[\"[Lcom.example.Gadget;\",[]]")))
                .isInstanceOf(CacheValueException.class);
    }

    @Test
    void decisionsFollowPackagesNotPrefixesOfNames() {
        assertThat(CacheTypeAllowList.isAllowed("java.lang.String")).isTrue();
        assertThat(CacheTypeAllowList.isAllowed("java.util.ImmutableCollections$ListN")).isTrue();
        assertThat(CacheTypeAllowList.isAllowed("java.time.Instant")).isTrue();
        assertThat(CacheTypeAllowList.isAllowed("java.util.concurrent.ConcurrentHashMap")).isFalse();
        assertThat(CacheTypeAllowList.isAllowed("javax.naming.InitialContext")).isFalse();
        assertThat(CacheTypeAllowList.isAllowed("org.example.allowed.Model")).isTrue();
        assertThat(CacheTypeAllowList.isAllowed("org.example.allowed.sub.Model")).isTrue();
        assertThat(CacheTypeAllowList.isAllowed("org.example.allowedtoo.Model")).isFalse();
    }

    @Test
    void entriesWrittenByAnOlderClassVersionStillDecode() {
        // A field removed from a cached class must not make every old entry undecodable after a deploy.
        String data = "{\"@class\":\"" + Versioned.class.getName() + "\",\"name\":\"n\",\"removedField\":42}";
        Object decoded = ValueCodec.decode(envelope(data)).data();
        assertThat(decoded).isInstanceOf(Versioned.class);
        assertThat(((Versioned) decoded).getName()).isEqualTo("n");
    }
}
