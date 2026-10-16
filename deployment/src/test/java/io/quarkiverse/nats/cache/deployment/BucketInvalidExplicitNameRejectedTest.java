package io.quarkiverse.nats.cache.deployment;

import static org.assertj.core.api.Assertions.assertThat;

import jakarta.enterprise.inject.spi.DeploymentException;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import io.quarkus.test.QuarkusExtensionTest;

/**
 * Bucket validation scenario (failure): an explicit bucket name outside the NATS KV charset fails the build,
 * through the same resolution path the runtime recorder uses, instead of failing when the cache manager is created.
 */
public class BucketInvalidExplicitNameRejectedTest extends BucketCollisionTestBase {

    @RegisterExtension
    static final QuarkusExtensionTest unitTest = new QuarkusExtensionTest()
            .withApplicationRoot(distinctApp())
            .overrideConfigKey("quarkus.cache.type", "nats")
            .overrideConfigKey("quarkus.nats-cache.caches.alpha-cache.bucket", "bad.name")
            // DeploymentException = build-time validation error. Without it the runtime recorder would still reject
            // the name, but only when the cache manager bean is created (a CreationException at startup).
            .assertException(throwable -> assertThat(throwable)
                    .isInstanceOf(DeploymentException.class)
                    .message()
                    .contains("quarkus.nats-cache.caches.alpha-cache.bucket")
                    .contains("'bad.name'")
                    .contains("is not a valid NATS KV bucket name"));

    @Test
    void buildFailsOnInvalidExplicitBucketName() {
    }
}
