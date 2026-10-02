package io.quarkiverse.nats.cache.deployment;

import java.util.function.Consumer;

import org.jboss.shrinkwrap.api.spec.JavaArchive;

/**
 * Base class for the bucket-collision scenario tests.
 * <p>
 * Two QuarkusUnitTest constraints shape this design:
 * <ul>
 * <li>the test class is re-loaded inside the application classloader when the app runs, where only
 * classes exported into the app archive are visible — the test class itself, its member classes and
 * its <em>superclasses</em> — so shared helpers must live in a base class rather than a sibling helper;</li>
 * <li>{@code .java} sources added to the application archive are only compiled in dev mode, so the
 * fixture beans are added as compiled classes.</li>
 * </ul>
 */
public abstract class BucketCollisionTestBase {

    /** App with two caches whose names derive the same default bucket ({@code MYCACHE}). */
    protected static Consumer<JavaArchive> myCacheApp() {
        return archive -> archive.addClass(CollisionService.class);
    }

    /** App with two caches whose names derive distinct default buckets. */
    protected static Consumer<JavaArchive> distinctApp() {
        return archive -> archive.addClass(DistinctService.class);
    }
}
