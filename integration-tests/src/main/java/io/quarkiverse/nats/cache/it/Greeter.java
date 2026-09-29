package io.quarkiverse.nats.cache.it;

import java.util.concurrent.atomic.AtomicInteger;

import jakarta.enterprise.context.ApplicationScoped;

import io.quarkus.cache.CacheResult;

/**
 * Exercises the {@code @CacheResult} AOP path (interceptor + codec) inside the native image.
 * Loader invocations are counted so tests can assert hits vs misses.
 */
@ApplicationScoped
public class Greeter {

    private final AtomicInteger invocations = new AtomicInteger();

    @CacheResult(cacheName = "smoke")
    String greet(String name) {
        invocations.incrementAndGet();
        return "hello " + name;
    }

    int loaderInvocations() {
        return invocations.get();
    }
}
