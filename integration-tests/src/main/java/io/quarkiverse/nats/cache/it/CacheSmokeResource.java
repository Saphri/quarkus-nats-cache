package io.quarkiverse.nats.cache.it;

import java.time.Duration;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.DELETE;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.PUT;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;

import io.quarkus.cache.Cache;
import io.quarkus.cache.CacheName;
import io.quarkus.cache.CompositeCacheKey;

/**
 * REST surface over the {@code smoke} cache so the native {@code @QuarkusIntegrationTest} can drive it over
 * HTTP (bean injection is not supported in integration tests — this is the standard Quarkus pattern).
 *
 * <p>Every write goes through a loader ({@code get(key, loader)}), since the Quarkus {@code Cache} API has no
 * {@code put}; a "sentinel" loader distinguishes a hit (cached value returned) from a miss (sentinel loaded and
 * stored). A cached {@code null} is reported as HTTP 204 to keep it distinguishable from a miss.
 */
@Path("/cache")
@ApplicationScoped
@Produces(MediaType.APPLICATION_JSON)
public class CacheSmokeResource {

    private static final Duration TIMEOUT = Duration.ofSeconds(30);

    @Inject
    @CacheName("smoke")
    Cache cache;

    @Inject
    Greeter greeter;

    /** Backend implementation class name — lets the IT assert the NATS backend is active, not a Caffeine fallback. */
    @GET
    @Path("/backend")
    public String backend() {
        return cache.getClass().getName();
    }

    // ------------------------------------------------------------------ POJO round-trip

    @PUT
    @Consumes(MediaType.APPLICATION_JSON)
    @Path("/order")
    public Response storeOrder(Order order) {
        cache.get("order", k -> order).await().atMost(TIMEOUT);
        return Response.ok().build();
    }

    /** Hit returns the cached {@link Order} (exact class); miss loads and stores the sentinel POJO. */
    @GET
    @Path("/order")
    public Object getOrder(@QueryParam("sentinel") String sentinel) {
        return cache.get("order", k -> new Order(sentinel, 0, null)).await().atMost(TIMEOUT);
    }

    // ------------------------------------------------------------------ composite keys

    @PUT
    @Path("/composite/{a}/{b}")
    public Response storeComposite(@PathParam("a") String a, @PathParam("b") String b, String value) {
        cache.get(new CompositeCacheKey(a, b), k -> value).await().atMost(TIMEOUT);
        return Response.ok().build();
    }

    @GET
    @Path("/composite/{a}/{b}")
    public Object getComposite(@PathParam("a") String a, @PathParam("b") String b,
            @QueryParam("sentinel") String sentinel) {
        return cache.get(new CompositeCacheKey(a, b), k -> sentinel).await().atMost(TIMEOUT);
    }

    // ------------------------------------------------------------------ cached null

    /** Stores a cached-null under {@code nullkey} (the loader returns {@code null}). */
    @POST
    @Path("/null")
    public Response storeNull() {
        Object v = cache.get("nullkey", k -> null).await().atMost(TIMEOUT);
        return v == null ? Response.ok().build() : Response.serverError().build();
    }

    /** 204 = a cached-null is present (loader suppressed); 200 + body = miss, sentinel loaded and stored. */
    @GET
    @Path("/null")
    public Response getNull(@QueryParam("sentinel") String sentinel) {
        Object v = cache.get("nullkey", k -> sentinel).await().atMost(TIMEOUT);
        return v == null ? Response.noContent().build() : Response.ok(v).build();
    }

    // ------------------------------------------------------------------ invalidation

    @DELETE
    @Path("/{key}")
    public Response invalidate(@PathParam("key") String key) {
        cache.invalidate(key).await().atMost(TIMEOUT);
        return Response.noContent().build();
    }

    @DELETE
    public Response invalidateAll() {
        cache.invalidateAll().await().atMost(TIMEOUT);
        return Response.noContent().build();
    }

    // ------------------------------------------------------------------ @CacheResult (AOP) path

    /** Exercises the {@code @CacheResult} interceptor; returns value + loader invocation count. */
    @GET
    @Path("/greet/{name}")
    public Greeting greet(@PathParam("name") String name) {
        return new Greeting(greeter.greet(name), greeter.loaderInvocations());
    }

    /** DTO for the AOP path: the greeting plus how many times the loader has run. */
    public record Greeting(String value, int invocations) {
    }
}
