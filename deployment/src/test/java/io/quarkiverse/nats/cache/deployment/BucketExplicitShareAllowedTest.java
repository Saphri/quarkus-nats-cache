package io.quarkiverse.nats.cache.deployment;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import io.quarkus.test.QuarkusExtensionTest;

/**
 * Bucket-collision scenario (success): the same default-bucket collision as the failure scenarios, but
 * <em>both</em> caches pin the shared bucket explicitly via {@code quarkus.nats-cache.caches.<name>.bucket} —
 * treated as deliberate sharing, so the build must succeed.
 */
public class BucketExplicitShareAllowedTest extends BucketCollisionTestBase {

    @RegisterExtension
    static final QuarkusExtensionTest unitTest = new QuarkusExtensionTest()
            .withApplicationRoot(myCacheApp())
            .overrideConfigKey("quarkus.cache.type", "nats")
            .overrideConfigKey("quarkus.nats-cache.caches.mycache.bucket", "SHARED_BKT")
            .overrideConfigKey("quarkus.nats-cache.caches.MYCACHE.bucket", "SHARED_BKT");

    @Test
    void explicitBucketOnEveryCacheAllowsSharing() {
    }
}
