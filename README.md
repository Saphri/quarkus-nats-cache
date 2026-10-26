# Quarkus NATS KV Cache

A [Quarkus](https://quarkus.io) extension that adds **NATS JetStream Key-Value** as a backend for the
standard Quarkus cache API — `@CacheResult`, `@CacheInvalidate`, `@CacheInvalidateAll` and the programmatic
`io.quarkus.cache.Cache` interface. Entries live in a shared, server-side KV bucket, so every node of a
cluster reads and writes the same cache without any extra coordination.

The extension builds on the Quarkiverse [`quarkus-nats-jetstream-client`](https://github.com/quarkiverse/quarkus-nats)
client extension (the messaging-free split of `quarkus-reactive-messaging-nats-jetstream`): connection lifecycle, auth/TLS, the NATS JetStream **dev service** (zero-config local dev and test),
and native-image support all come from there.

## Features

- Drop-in backend for the Quarkus cache API — no JCache/JSR-107, no new annotations
- Mixable with Caffeine in the same application (`nats` for shared caches, `caffeine` for local ones)
- Lossless round-trip of arbitrary keys and values (Jackson JSON with embedded type info), including
  `CompositeCacheKey` and cached `null` results — no codec registration needed
- Stampede protection: concurrent misses on the same key within one application instance share one loader
  execution (instances do not coordinate with each other)
- Fails open: if NATS is unreachable or an entry cannot be decoded, the loader runs and its value is returned,
  so a cache problem costs latency rather than failing the cached method
- Full invalidation surface: single key, all keys, predicate-based
- Per-item expiration (`expiresAfter`) as a server-side per-message TTL under the bucket-TTL ceiling (see [Expiration](#expiration))
- Zero-config local development and testing via the inherited NATS JetStream dev service (Docker)
- GraalVM native image support

## Requirements

- Quarkus **3.40.x** (this extension is built against Quarkus `3.40.1.1`, pinning `quarkus-nats-jetstream-client` `3.40.10`)
- A running NATS server **2.11 or later** (per-message TTL support) with JetStream enabled in production; in `dev`/`test` modes a
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
caches — to share a bucket deliberately, set an explicit `bucket=` on *each* of them; an explicit bucket next
to a derived one is still treated as an accidental collision.

Sharing a bucket means sharing invalidation: `invalidateAll()` and `invalidateIf(predicate)` work on the whole
bucket, so they remove the other cache's entries too. Only `invalidate(key)` is scoped to a key you address.

### Allowed key and value types

| Property | Default | Description |
|---|---|---|
| `quarkus.nats-cache.allowed-packages` | *(none)* | Comma-separated package prefixes (sub-packages included) whose classes may be stored as cache keys or values, in addition to the always-allowed ones below. |

Stored entries carry the class name of the key or value (see [Values](#values)), and the bucket is shared
network state, so those class names are treated as **untrusted input**: the codec only instantiates classes from
the application's own packages, the JDK value-type packages (`java.lang`, `java.util`, `java.time`,
`java.math`) and `allowed-packages`. Anything else is refused and logged once with a hint to this property.
Value types from a library (for example a shared model jar) need their package listed here:

```properties
quarkus.nats-cache.allowed-packages=com.acme.model
```

Treat write access to the cache buckets as a trust boundary all the same: anyone who can write to them can
change what the application reads back.

### Connection settings (underlying extension)

Connection, authentication, TLS and named data-sources are configured through the underlying
`quarkus-nats-jetstream-client` extension (`quarkus.messaging.nats.*`); see its documentation.
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

Entries written by an older version of a class still decode after a deploy: unknown properties are ignored.
An entry that cannot be decoded at all (for example after a class was renamed or moved) is treated as a miss:
it is purged and replaced with a freshly loaded value.

Entries are only ever *created*, never updated in place: a cached value stays until it expires or is
invalidated. When two application instances miss the same key at the same moment, the first write wins and the
other instance returns its own loaded value without caching it.

A practical bonus: because values are readable JSON, you can inspect them directly from the NATS CLI
(see [Debugging](#debugging-with-the-nats-cli)). Keys are opaque base64url by design; values are where the
debugging value lives.

## Expiration

Two layers of TTL apply:

1. **Bucket TTL (hard ceiling)** — `quarkus.nats-cache.caches.<name>.ttl`, enforced server-side per entry and
   reset on every write. Entries stored without a per-item duration are governed solely by this ceiling (they
   carry the longest per-message TTL NATS clients accept, about 68 years, which the bucket TTL undercuts).
2. **Per-item TTL** — `get(key, loader, expiresAfter)` writes the entry with a NATS per-message TTL
   (`Nats-TTL` header) of `expiresAfter`. Reads before expiration return the stored value; once it elapses the
   server deletes the entry, so the next read is a miss that invokes the loader. The async overload
   `getAsync(key, asyncLoader, expiresAfter)` has identical semantics. Expiry is enforced by the server's
   clock, not the application's, and expired entries do not linger in the bucket.

NATS TTLs have whole-second granularity, so `expiresAfter` is rounded **up** to the next second (an entry never
expires early), and the server's expiry sweep can trail the deadline by up to about a second.

Per-message TTLs must be allowed on the bucket. Buckets the extension creates allow them (it sets a one-second
limit marker, which is how a KV bucket enables `allow_msg_ttl`). A pre-existing bucket created without them is
left unmodified; since every entry is written with a TTL, the server rejects all writes to it, which fail open:
the loaded value is returned but not cached, and a warning is logged. To cache there, recreate the bucket with a
limit marker TTL, or let the extension create it.

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

Single-key purges (`invalidate`, `invalidateIf`) leave a purge marker that carries a one-second TTL, so the
server removes the marker too and invalidated keys leave nothing behind. On a pre-existing bucket without
per-message TTL support the extension falls back to a plain purge, whose marker stays until the key is written
again or the bucket TTL removes it.

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

The stored payload is an envelope `{ "data": <typed value> }`, where `data` is `null` for cached null results.
Every entry carries a `Nats-TTL` header (the per-item duration, or about 68 years without one) (`nats kv get` doesn't show headers;
`nats stream get KV_<bucket> --last-for '$KV.<bucket>.<key>'` does).

## Limitations

- **Quarkiverse dependency.** The extension builds on the Quarkiverse
  `quarkus-nats-jetstream-client` extension, so it cannot be upstreamed into Quarkus core. Its
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
