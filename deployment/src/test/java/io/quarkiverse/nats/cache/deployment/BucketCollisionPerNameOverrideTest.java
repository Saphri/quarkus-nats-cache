package io.quarkiverse.nats.cache.deployment;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import io.quarkus.test.QuarkusExtensionTest;

/**
 * Bucket-collision scenario (failure): the same collision detected through per-name type overrides
 * ({@code quarkus.cache.<name>.type=nats}) instead of the default cache type — {@code mycache} and
 * {@code MYCACHE} both derive bucket {@code MYCACHE}.
 */
public class BucketCollisionPerNameOverrideTest extends BucketCollisionTestBase {

    @RegisterExtension
    static final QuarkusExtensionTest unitTest = new QuarkusExtensionTest()
            .withApplicationRoot(myCacheApp())
            .overrideConfigKey("quarkus.cache.mycache.type", "nats")
            .overrideConfigKey("quarkus.cache.MYCACHE.type", "nats")
            .assertException(throwable -> assertThat(throwable.getMessage())
                    .contains("would be shared by caches")
                    .contains("'MYCACHE'")
                    .contains("'mycache'")
                    .contains("quarkus.nats-cache.caches.<name>.bucket"));

    @Test
    void buildFailsWhenPerNameOverridesAreNats() {
    }
}
