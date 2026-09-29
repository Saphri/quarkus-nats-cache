package io.quarkiverse.nats.cache.runtime.codec;

/** Compact POJO element for composite-key round-trip tests (top-level so its FQCN stays short enough to fit the 255-char key limit). */
public record K1(int v) {
}
