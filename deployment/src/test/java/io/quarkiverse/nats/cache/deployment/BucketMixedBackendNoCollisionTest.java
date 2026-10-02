package io.quarkiverse.nats.cache.deployment;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import io.quarkus.test.QuarkusExtensionTest;

/**
 * Bucket-collision scenario (success): {@code mycache} and {@code MYCACHE} would derive the same bucket,
 * but only one is a nats cache (the other is overridden to the Caffeine backend) — no two nats caches
 * share a bucket, so the build must succeed.
 */
public class BucketMixedBackendNoCollisionTest extends BucketCollisionTestBase {

    @RegisterExtension
    static final QuarkusExtensionTest unitTest = new QuarkusExtensionTest()
            .withApplicationRoot(myCacheApp())
            .overrideConfigKey("quarkus.cache.mycache.type", "nats")
            .overrideConfigKey("quarkus.cache.MYCACHE.type", "caffeine");

    @Test
    void onlyOneNatsCacheInTheBucketGroup() {
    }
}
