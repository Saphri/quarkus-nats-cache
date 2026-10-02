package io.quarkiverse.nats.cache.deployment;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import io.quarkus.test.QuarkusExtensionTest;

/**
 * Bucket-collision scenario (failure): {@code mycache} pins bucket {@code MYCACHE} explicitly while
 * {@code MYCACHE} derives the same bucket — an explicit bucket next to a derived one is still an accidental
 * collision ({@code invalidateAll()} on one cache would wipe the other's entries), so the build must fail.
 * Sharing a bucket requires <em>every</em> cache in the group to pin it explicitly.
 */
public class BucketExplicitMixedWithDerivedRejectedTest extends BucketCollisionTestBase {

    @RegisterExtension
    static final QuarkusExtensionTest unitTest = new QuarkusExtensionTest()
            .withApplicationRoot(myCacheApp())
            .overrideConfigKey("quarkus.cache.type", "nats")
            .overrideConfigKey("quarkus.nats-cache.caches.mycache.bucket", "MYCACHE")
            .assertException(throwable -> assertThat(throwable.getMessage())
                    .contains("would be shared by caches")
                    .contains("'MYCACHE'")
                    .contains("'mycache'")
                    .contains("quarkus.nats-cache.caches.<name>.bucket"));

    @Test
    void buildFailsWhenOnlyOneCachePinsTheSharedBucket() {
    }
}
