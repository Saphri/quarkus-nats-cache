package io.quarkiverse.nats.cache.deployment;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import io.quarkus.test.QuarkusExtensionTest;

/**
 * Bucket-collision scenario (success): two nats caches whose names derive distinct default buckets
 * ({@code ALPHA-CACHE} and {@code BETA-CACHE}) — no collision, the build must succeed.
 */
public class BucketDistinctNamesNoCollisionTest extends BucketCollisionTestBase {

    @RegisterExtension
    static final QuarkusExtensionTest unitTest = new QuarkusExtensionTest()
            .withApplicationRoot(distinctApp())
            .overrideConfigKey("quarkus.cache.type", "nats");

    @Test
    void distinctDerivedBucketsDoNotCollide() {
    }
}
