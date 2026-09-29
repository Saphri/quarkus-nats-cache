package io.quarkiverse.nats.cache.it;

/** POJO cached value: exercises exact-runtime-class reconstruction via Jackson default typing in native mode. */
public record Order(String sku, int quantity, Address address) {
}
