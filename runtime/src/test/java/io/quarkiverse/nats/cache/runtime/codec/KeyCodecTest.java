package io.quarkiverse.nats.cache.runtime.codec;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

import io.quarkus.cache.CompositeCacheKey;
import io.quarkus.cache.DefaultCacheKey;

class KeyCodecTest {

    // --- key fixtures -------------------------------------------------------

    public static final class Point {
        private int x;
        private int y;

        public Point() {
        }

        public Point(int x, int y) {
            this.x = x;
            this.y = y;
        }

        public int getX() {
            return x;
        }

        public void setX(int x) {
            this.x = x;
        }

        public int getY() {
            return y;
        }

        public void setY(int y) {
            this.y = y;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o)
                return true;
            if (!(o instanceof Point))
                return false;
            Point point = (Point) o;
            return x == point.x && y == point.y;
        }

        @Override
        public int hashCode() {
            return 31 * x + y;
        }
    }

    /** Two distinct key classes that both report the same {@code toString()} ("K"). */
    public static final class KeyA {
        private String v;

        public KeyA() {
        }

        public KeyA(String v) {
            this.v = v;
        }

        public String getV() {
            return v;
        }

        public void setV(String v) {
            this.v = v;
        }

        @Override
        public boolean equals(Object o) {
            return o instanceof KeyA && java.util.Objects.equals(v, ((KeyA) o).v);
        }

        @Override
        public int hashCode() {
            return java.util.Objects.hashCode(v);
        }

        @Override
        public String toString() {
            return "K";
        }
    }

    public static final class KeyB {
        private int n;

        public KeyB() {
        }

        public KeyB(int n) {
            this.n = n;
        }

        public int getN() {
            return n;
        }

        public void setN(int n) {
            this.n = n;
        }

        @Override
        public boolean equals(Object o) {
            return o instanceof KeyB && n == ((KeyB) o).n;
        }

        @Override
        public int hashCode() {
            return n;
        }

        @Override
        public String toString() {
            return "K";
        }
    }

    /** Not reconstructable by Jackson: no no-arg constructor and no creator. */
    public static final class NoDefaultCtor {
        private final String v;

        public NoDefaultCtor(String v) {
            this.v = v;
        }

        public String getV() {
            return v;
        }
    }

    // --- round-trips ---------------------------------------------------------

    @Test
    void stringKeysRoundTrip() {
        for (String key : new String[] { "simple", "with spaces", "unicode-ümlauts-日本語", "" }) {
            assertThat(KeyCodec.decode(KeyCodec.encode(key))).isEqualTo(key);
        }
    }

    @Test
    void numericAndBooleanKeysRoundTrip() {
        assertThat(KeyCodec.decode(KeyCodec.encode(42))).isEqualTo(42).isInstanceOf(Integer.class);
        assertThat(KeyCodec.decode(KeyCodec.encode(42L))).isEqualTo(42L).isInstanceOf(Long.class);
        assertThat(KeyCodec.decode(KeyCodec.encode(1.5d))).isEqualTo(1.5d).isInstanceOf(Double.class);
        assertThat(KeyCodec.decode(KeyCodec.encode(Boolean.TRUE))).isEqualTo(Boolean.TRUE);
    }

    @Test
    void longAndIntegerKeysDoNotCollide() {
        // Same numeric value, different Java types: encodings must differ (Long is type-wrapped).
        String encodedInt = KeyCodec.encode(42);
        String encodedLong = KeyCodec.encode(42L);
        assertThat(encodedLong).isNotEqualTo(encodedInt);
        assertThat(KeyCodec.decode(encodedInt)).isInstanceOf(Integer.class);
        assertThat(KeyCodec.decode(encodedLong)).isInstanceOf(Long.class);
    }

    @Test
    void pojoKeysRoundTripWithoutRegistration() {
        Point key = new Point(1, 2);
        Object decoded = KeyCodec.decode(KeyCodec.encode(key));
        assertThat(decoded).isInstanceOf(Point.class).isEqualTo(key);
    }

    @Test
    void distinctKeysWithEqualToStringDoNotCollide() {
        KeyA a = new KeyA("alpha");
        KeyB b = new KeyB(7);
        assertThat(a.toString()).isEqualTo(b.toString()); // the trap: identical toString()

        String encodedA = KeyCodec.encode(a);
        String encodedB = KeyCodec.encode(b);
        assertThat(encodedA).isNotEqualTo(encodedB);

        assertThat(KeyCodec.decode(encodedA)).isEqualTo(a).isInstanceOf(KeyA.class);
        assertThat(KeyCodec.decode(encodedB)).isEqualTo(b).isInstanceOf(KeyB.class);
    }

    @Test
    void compositeKeysWithHeterogeneousElementsRoundTrip() {
        CompositeCacheKey key = new CompositeCacheKey("user", 42L, new Point(3, 4));
        Object decoded = KeyCodec.decode(KeyCodec.encode(key));
        assertThat(decoded).isInstanceOf(CompositeCacheKey.class);
        assertThat(decoded).isEqualTo(key);
    }

    @Test
    void compositeKeysWithPojoElementsRoundTrip() {
        // Compact top-level element types so the encoded key stays within the 255-char NATS limit while still
        // exercising POJO elements that need no Jackson registration and must keep their exact runtime class.
        CompositeCacheKey key = new CompositeCacheKey(new K1(1), new K2(9));
        Object decoded = KeyCodec.decode(KeyCodec.encode(key));
        assertThat(decoded).isEqualTo(key);
        assertThat(((CompositeCacheKey) decoded).getKeyElements()[0]).isInstanceOf(K1.class).isEqualTo(new K1(1));
        assertThat(((CompositeCacheKey) decoded).getKeyElements()[1]).isInstanceOf(K2.class).isEqualTo(new K2(9));
    }

    @Test
    void defaultCacheKeysRoundTrip() {
        DefaultCacheKey key = new DefaultCacheKey("my-cache");
        Object decoded = KeyCodec.decode(KeyCodec.encode(key));
        assertThat(decoded).isInstanceOf(DefaultCacheKey.class).isEqualTo(key);
    }

    @Test
    void recordKeysRoundTrip() {
        PointRecord key = new PointRecord(5, 6);
        Object decoded = KeyCodec.decode(KeyCodec.encode(key));
        assertThat(decoded).isEqualTo(key);
    }

    public record PointRecord(int x, int y) {
    }

    // --- failure modes -------------------------------------------------------

    @Test
    void nullKeyIsRejected() {
        assertThatThrownBy(() -> KeyCodec.encode(null))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("null");
    }

    @Test
    void unsupportedKeyTypeIsRejectedAtStoreTimeNamingTheClass() {
        NoDefaultCtor key = new NoDefaultCtor("value");
        assertThatThrownBy(() -> KeyCodec.encode(key))
                .isInstanceOf(CacheKeyEncodingException.class)
                .hasMessageContaining(NoDefaultCtor.class.getName());
    }

    @Test
    void unsupportedCompositeElementIsRejectedNamingTheClass() {
        CompositeCacheKey key = new CompositeCacheKey("ok", new NoDefaultCtor("bad"));
        assertThatThrownBy(() -> KeyCodec.encode(key))
                .isInstanceOf(CacheKeyEncodingException.class)
                .hasMessageContaining(NoDefaultCtor.class.getName());
    }

    @Test
    void oversizedKeyIsRejectedWithDescriptiveError() {
        String longKey = "k".repeat(300); // encodes well over 255 base64url characters
        assertThatThrownBy(() -> KeyCodec.encode(longKey))
                .isInstanceOf(CacheKeyEncodingException.class)
                .hasMessageContaining("255")
                .hasMessageContaining(String.class.getName());

        // A key that fits must still be accepted (boundary sanity).
        String fittingKey = "k".repeat(100);
        assertThat(KeyCodec.encode(fittingKey).length()).isLessThanOrEqualTo(255);
    }

    @Test
    void decodingGarbageFailsCleanly() {
        assertThatThrownBy(() -> KeyCodec.decode("!!!not-base64url!!!"))
                .isInstanceOf(CacheKeyEncodingException.class);
    }
}
