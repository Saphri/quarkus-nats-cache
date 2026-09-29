# Quarkus NATS KV Cache

A [Quarkus](https://quarkus.io) extension that adds **NATS JetStream Key-Value** as a backend for the
standard Quarkus cache API — `@CacheResult`, `@CacheInvalidate`, `@CacheInvalidateAll` and the programmatic
`io.quarkus.cache.Cache` interface. Entries live in a shared, server-side KV bucket, so every node of a
cluster reads and writes the same cache without any extra coordination.

The extension builds on the Quarkiverse [`quarkus-reactive-messaging-nats-jetstream`](https://github.com/quarkiverse/quarkus-nats)
extension: connection lifecycle, auth/TLS, the NATS JetStream **dev service** (zero-config local dev and test),
and native-image support all come from there.

## Features

- Drop-in backend for the Quarkus cache API — no JCache/JSR-107, no new annotations
- Mixable with Caffeine in the same application (`nats` for shared caches, `caffeine` for local ones)
- Lossless round-trip of arbitrary keys and values (Jackson JSON with embedded type info), including
  `CompositeCacheKey` and cached `null` results — no codec registration needed
- Stampede protection: concurrent misses on the same key share one loader execution
- Full invalidation surface: single key, all keys, predicate-based
- Per-item expiration (`expiresAfter`) as a logical TTL under the bucket-TTL ceiling (see [Expiration](#expiration))
- Zero-config local development and testing via the inherited NATS JetStream dev service (Docker)
- GraalVM native image support

## Requirements

- Quarkus **3.40.x** or later (tested against 3.40.0)
- A running NATS server with JetStream enabled in production; in `dev`/`test` modes a
  [Docker](https://www.docker.com/) container is started automatically when no server is configured

## Getting started

### 1. Add the dependency

```xml
<dependency>
  <groupId>io.quarkiverse.nats-cache</groupId>
  <artifactId>quarkus-nats-cache</artifactId>
  <version>1.0.0-SNAPSHOT</version>
</dependency>
```

### 2. Select the backend

```properties
# application.properties
quarkus.cache.type=nats
```

That makes **every** cache in the application NATS-backed. To use NATS for only some caches, set the type per
cache instead (and leave `quarkus.cache.type` at its default):

```properties
quarkus.cache.session.type=nats
quarkus.cache.config.type=caffeine
```

> **Note:** the per-cache selector is `quarkus.cache.<name>.type` — a property of the core cache extension,
> *not* `quarkus.cache.caches.<name>.type`.

### 3. Use it

Annotation-based:

```java
@ApplicationScoped
public class WeatherService {

    @Inject
    @CacheName("weather")
    Cache weather;

    @CacheResult(cacheName = "weather")
    public Forecast forecast(String city) {
        return weatherApi.call(city); // executed once per city until invalidated or expired
    }

    @CacheInvalidate(cacheName = "weather")
    public void stationReported(String city) { /* ... */ }
}
```

Programmatic (Mutiny-native):

```java
@Inject
@CacheName("weather")
Cache weather;

Uni<Forecast> f = weather.get(city, this::callWeatherApi);          // miss → load + store
Uni<String> s = weather.getAsync(id, this::loadAsync);              // async loader
weather.invalidate(key).await().atMost(Duration.ofSeconds(30));     // single key
weather.invalidateAll();                                            // whole bucket (stream purge)
weather.invalidateIf(k -> k instanceof String s && s.startsWith("DE"));
```

## Configuration reference

### Backend selection (core cache extension)

| Property | Description |
|---|---|
| `quarkus.cache.type` | Default backend for all caches (`nats`, or `caffeine`) |
| `quarkus.cache.<name>.type` | Per-cache backend override |

### Per-cache NATS settings (this extension)

All properties live under `quarkus.nats-cache.caches.<name>.*`, where `<name>` is the cache name. Caches with
no entry here use all defaults.

| Property | Default | Description |
|---|---|---|
| `quarkus.nats-cache.caches.<name>.bucket` | sanitized uppercase cache name | Explicit NATS KV bucket name. The default transform upper-cases the name and replaces every character that is not `A-Z`, `0-9`, `-` or `_` with `_` (e.g. `weather-cache` → `WEATHER-CACHE`). |
| `quarkus.nats-cache.caches.<name>.ttl` | *(none)* | Bucket-level TTL — the hard ceiling for every entry, enforced server-side and reset on each write. Applied when the bucket is created. |
| `quarkus.nats-cache.caches.<name>.history` | `1` | Maximum number of revisions retained per key (1–64). |

Example:

```properties
quarkus.cache.type=nats
quarkus.nats-cache.caches.weather.bucket=WEATHER_CACHE
quarkus.nats-cache.caches.weather.ttl=1h
quarkus.nats-cache.caches.weather.history=5
```

Bucket provisioning is **add-if-absent**: if the bucket already exists (created by other tooling, or shared
with another cache), it is used unmodified. If two `nats`-typed caches would *derive* the same default bucket
name (e.g. `my.cache` and `my_cache` both map to `MY_CACHE`), the **build fails** with an error naming both
caches — set an explicit `bucket=` on one of them to share a bucket deliberately.

### Connection settings (underlying extension)

Connection, authentication, TLS and named data-sources are configured through the underlying
`quarkus-reactive-messaging-nats-jetstream` extension (`quarkus.messaging.nats.*`); see its documentation.
In `dev`/`test` modes with Docker available and no server configured, the dev service starts a NATS JetStream
container automatically — no manual setup needed.

## Keys

Keys are serialized to JSON (with embedded type info) and base64url-encoded into a subject-safe string, so:

- **Any Jackson-friendly key type works**: strings, numbers, booleans, POJOs with public getters plus a
  no-arg constructor (or a Jackson creator), and `CompositeCacheKey` with any number of element types —
  all out of the box, no registration.
- **Distinct keys never collide**, even when their `toString()` representations are identical — identity is
  based on the full serialized form, not on `toString()`.
- **Fail-fast validation**: a key type that cannot be reconstructed from its encoding is rejected at *store*
  time with an error naming the offending class (checked once per class and memoized), rather than failing
  later at read-back.
- **255-byte limit**: NATS KV subjects are limited to 255 bytes, so encoded keys must fit within it. A key
  whose encoding exceeds the limit is rejected at store time with a descriptive error. Budget accordingly —
  composite POJO keys embed each element's fully-qualified class name and consume the budget quickly.

### Unordered collection elements: use ordered collections

Entry identity is guaranteed only for keys whose encoding is **deterministic**. If a key element contains an
*unordered* collection (e.g. `HashSet`), two equal keys may iterate in different orders at encoding time and
map to **distinct entries**. Applications MUST use ordered collections (`List`, `LinkedHashSet`, …) for key
elements that contain collections, so that equal keys always encode identically.

## Values

Values are stored as JSON with embedded type info, so a retrieved value is an instance of the **original
runtime class** with all fields preserved — POJOs, nested objects, collections and maps round-trip losslessly,
and `null` results are cached (the loader is not re-invoked while the sentinel is present).

A practical bonus: because values are readable JSON, you can inspect them directly from the NATS CLI
(see [Debugging](#debugging-with-the-nats-cli)). Keys are opaque base64url by design; values are where the
debugging value lives.

## Expiration

Two layers of TTL apply:

1. **Bucket TTL (hard ceiling)** — `quarkus.nats-cache.caches.<name>.ttl`, enforced server-side per entry and
   reset on every write. Entries stored without a per-item duration are governed solely by this ceiling.
2. **Per-item logical TTL** — `get(key, loader, expiresAfter)` stores the entry with a logical expiration of
   `expiresAfter`. Reads before expiration return the stored value; reads after expiration invoke the loader
   and overwrite the entry. The async overload `getAsync(key, asyncLoader, expiresAfter)` has identical
   semantics. A logically expired entry is treated as a miss on read (it lingers in the bucket until
   overwritten or reaped by the bucket TTL — bounded staleness, invisible to readers).

The effective lifetime of any entry never exceeds the bucket TTL.

> **Version note:** the `expiresAfter` overloads are not yet part of the released
> `io.quarkus.cache.Cache` interface (they landed on Quarkus `main` in September 2026, after 3.40.0). This
> extension implements them *ahead* with the exact upstream signatures as public methods on
> `NatsKvCacheImpl`:

```java
@Inject
@CacheName("weather")
io.quarkiverse.nats.cache.runtime.NatsKvCacheImpl weather; // cast/inject the impl for now

Uni<Forecast> f = weather.get(city, this::callWeatherApi, Duration.ofMinutes(5));
```

> Once your Quarkus version ships the per-item expiration API on the `Cache` interface, these methods become
> overrides automatically — no rework. Until then they are reachable only through the implementation class.

## Invalidation

- `invalidate(key)` / `@CacheInvalidate` — removes one entry (KV `purge`: key and its history)
- `invalidateAll()` / `@CacheInvalidateAll` — purges the whole backing stream (all keys + history). This is a
  single server-side bulk operation, but it is an *administrative* operation, not a hot-path API.
- `invalidateIf(predicate)` — streams the bucket's keys, decodes each back to its original key object and
  purges the matches. Also administrative; on large buckets this enumerates the key space.

After any invalidation, the next read for an affected key is a miss and re-invokes the loader.

## Native image

The extension supports GraalVM native execution (verified with a native smoke test covering put, get-hit,
get-miss, single-key and all-keys invalidation, POJO round-trips, composite keys, cached nulls and the
`@CacheResult` interceptor). When at least one cache is `nats`-typed, the deployment module automatically
registers **all application classes** for Jackson reflection (constructors, methods and fields), so your POJO
keys and values work in native mode without extra configuration.

If you cache types from *third-party* libraries that are not part of your application's index, register them
explicitly, e.g. with `@RegisterForReflection`.

## Debugging with the NATS CLI

Because values are readable JSON, the standard [NATS CLI](https://github.com/nats-io/cli) is a first-class
debugging tool:

```bash
nats kv ls                                  # list buckets
nats kv ls WEATHER_CACHE                    # keys in a bucket (keys are opaque base64url)
nats kv get WEATHER_CACHE <key>             # read the JSON value envelope
nats kv history WEATHER_CACHE <key>         # revision history (when history > 1)
nats kv purge WEATHER_CACHE                 # drop everything (same effect as invalidateAll)
```

The stored payload is an envelope `{ "exp": <epoch-millis?>, "data": <typed value> }`: `exp` is present only
for entries written with a per-item duration, and `data` is `null` for cached null results.

## Limitations

- **Quarkiverse dependency.** The extension builds on the Quarkiverse
  `quarkus-reactive-messaging-nats-jetstream` extension, so it cannot be upstreamed into Quarkus core. Its
  release cadence is an external dependency (a known-good version is pinned in this project).
- **Remote round-trip per read.** v1 has no local L1 cache layer; every read is a network call to the KV
  bucket. A Caffeine L1 with KV-watch-driven eviction is on the roadmap for a future version.
- **Dotted cache names cannot carry per-cache settings.** Quarkus configuration maps treat `.` as nesting, so
  a cache named e.g. `my.cache` cannot be configured via `quarkus.cache.<name>.type` or
  `quarkus.nats-cache.caches.<name>.*`. Use single-token names (or an explicit bucket on the other cache) —
  note that `my.cache` and `my_cache` would collide on the default bucket anyway, which the build-time
  validator rejects.
- **No JSR-107/JCache provider** — this extension targets the Quarkus cache API only.

## Building

All builds use the [Maven Wrapper](https://maven.apache.org/wrapper/) (`./mvnw`), which pins the Maven
version (see `.mvn/wrapper/maven-wrapper.properties`, kept current by Dependabot) — no local Maven
installation required:

```bash
./mvnw install              # build all modules + run the unit suites (runtime 26, deployment 25)
```

Integration tests (`NatsKvCacheNativeIT`, driven over HTTP; dev service NATS via testcontainers):

```bash
./mvnw -pl integration-tests verify -DskipITs=false   # against a JVM jar
```

Native-image verification (requires GraalVM or Mandrel with `native-image` on the PATH):

```bash
./mvnw install -DskipTests
./mvnw -pl integration-tests verify -Dnative          # build the native executable + run the IT suite
```

## Project layout

| Module | Purpose |
|---|---|
| `runtime` | `NatsKvCacheImpl`, key/value codecs, config mapping, recorder |
| `deployment` | Build steps: cache-manager info, bucket-collision validation, native-image reflection config |
| `integration-tests` | Native-image smoke application + `@QuarkusIntegrationTest` suite |
