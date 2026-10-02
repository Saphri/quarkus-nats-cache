package io.quarkiverse.nats.cache.deployment;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import io.quarkus.test.QuarkusExtensionTest;

/**
 * Bucket-collision scenario (failure): the default cache type ({@code quarkus.cache.type=nats}) puts both
 * {@code mycache} and {@code MYCACHE} on the derived bucket {@code MYCACHE} — the build must fail, naming
 * both caches and pointing at the explicit bucket property.
 */
public class BucketCollisionDefaultTypeTest extends BucketCollisionTestBase {

    @RegisterExtension
    static final QuarkusExtensionTest unitTest = new QuarkusExtensionTest()
            .withApplicationRoot(myCacheApp())
            .overrideConfigKey("quarkus.cache.type", "nats")
            .assertException(throwable -> assertThat(throwable.getMessage())
                    .contains("would be shared by caches")
                    .contains("'MYCACHE'")
                    .contains("'mycache'")
                    .contains("quarkus.nats-cache.caches.<name>.bucket"));

    @Test
    void buildFailsWhenDefaultTypeIsNats() {
    }
}
