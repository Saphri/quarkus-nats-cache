package io.quarkiverse.nats.cache.it;

/** Simple nested POJO used to prove Jackson reflection registration works in the native image. */
public record Address(String street, String postalCode) {
}
