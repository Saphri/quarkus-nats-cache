package io.quarkiverse.nats.cache.deployment;

/**
 * Top-level POJO key fixture: a top-level class keeps the fully-qualified name short enough that a composite
 * {@code [KPoint, String]} key stays within the 255-byte encoded-key budget (nested classes would not).
 */
public record KPoint(int x, int y) {
}
