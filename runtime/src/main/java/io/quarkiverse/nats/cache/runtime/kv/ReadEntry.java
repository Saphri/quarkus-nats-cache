package io.quarkiverse.nats.cache.runtime.kv;

import java.util.Objects;

/**
 * Outcome of {@link KvBucket#get}: the read result for one entry. Exactly one of three states:
 * <ul>
 * <li>{@link #hit(byte[])} — the key holds an entry with a payload (still opaque bytes: decoding stays on the cache
 * side);</li>
 * <li>{@link #miss()} — the key holds no live entry;</li>
 * <li>{@link #unreadable()} — the key holds an entry this application cannot read (for example one without a
 * value); the cache purges it before its reload is stored.</li>
 * </ul>
 * The factory methods are the only construction path, so a hit always carries a payload and the other states never
 * do. Miss and unreadable are shared instances: a miss allocates nothing.
 */
public abstract sealed class ReadEntry {

    private static final ReadEntry MISS = new Miss();
    private static final ReadEntry UNREADABLE = new Unreadable();

    private ReadEntry() {
    }

    /** An entry with a payload. */
    public static ReadEntry hit(byte[] payload) {
        return new Hit(Objects.requireNonNull(payload, "payload must not be null"));
    }

    /** No live entry for the key. */
    public static ReadEntry miss() {
        return MISS;
    }

    /** An entry is present but cannot be read. */
    public static ReadEntry unreadable() {
        return UNREADABLE;
    }

    public boolean isHit() {
        return false;
    }

    public boolean isUnreadable() {
        return false;
    }

    /**
     * The stored payload of a hit.
     *
     * @throws IllegalStateException if this is not a hit
     */
    public byte[] payload() {
        throw new IllegalStateException("Not a hit: " + this);
    }

    private static final class Hit extends ReadEntry {
        private final byte[] payload;

        Hit(byte[] payload) {
            this.payload = payload;
        }

        @Override
        public boolean isHit() {
            return true;
        }

        @Override
        public byte[] payload() {
            return payload;
        }

        @Override
        public String toString() {
            return "hit(" + payload.length + " bytes)";
        }
    }

    private static final class Miss extends ReadEntry {
        @Override
        public String toString() {
            return "miss";
        }
    }

    private static final class Unreadable extends ReadEntry {
        @Override
        public boolean isUnreadable() {
            return true;
        }

        @Override
        public String toString() {
            return "unreadable";
        }
    }
}
