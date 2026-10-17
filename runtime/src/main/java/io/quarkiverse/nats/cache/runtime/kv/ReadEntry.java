package io.quarkiverse.nats.cache.runtime.kv;

import java.util.Objects;

/**
 * Outcome of {@link KvBucket#get}: the read entry for one key, in exactly one of three declared states.
 * <ul>
 * <li>{@link #hit(byte[])}: the key holds an entry with a payload (still opaque bytes: decoding stays on the cache
 * side);</li>
 * <li>{@link #miss()}: the key holds no live entry;</li>
 * <li>{@link #unreadable()}: the key holds an entry this application cannot read; the cache purges it before its
 * reload is stored.</li>
 * </ul>
 * The factory methods are the only construction path, so a hit always carries a payload and the other states never
 * do. Miss and unreadable are shared instances: a miss allocates nothing.
 */
public final class ReadEntry {

    private enum State {
        HIT,
        MISS,
        UNREADABLE
    }

    private static final ReadEntry MISS = new ReadEntry(State.MISS, null);
    private static final ReadEntry UNREADABLE = new ReadEntry(State.UNREADABLE, null);

    private final State state;
    private final byte[] payload;

    private ReadEntry(State state, byte[] payload) {
        this.state = state;
        this.payload = payload;
    }

    /** An entry with a payload. */
    public static ReadEntry hit(byte[] payload) {
        return new ReadEntry(State.HIT, Objects.requireNonNull(payload, "payload must not be null"));
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
        return state == State.HIT;
    }

    public boolean isUnreadable() {
        return state == State.UNREADABLE;
    }

    /**
     * The stored payload of a hit.
     *
     * @throws IllegalStateException if this is not a hit
     */
    public byte[] payload() {
        if (state != State.HIT) {
            throw new IllegalStateException("Not a hit: " + state);
        }
        return payload;
    }
}
