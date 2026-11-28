# AGENTS.md

Quarkus extension providing NATS JetStream KV as a backend for the Quarkus cache API (`@CacheResult`, `io.quarkus.cache.Cache`). User docs live in `README.md`.

## Build & test

Always use `./mvnw` (wrapper version pinned in `.mvn/wrapper/maven-wrapper.properties`), even though the devcontainer also installs system Maven.

- **Full unit build**: `./mvnw install` (CI runs `./mvnw -B clean install -Dno-format`). Builds all modules and runs `runtime` and `deployment` tests; skips `integration-tests` (`<skipITs>true</skipITs>`).
- **Single unit test**: `./mvnw -pl runtime test -Dtest=KeyCodecTest` (or `Class#method`).
- **Integration tests (JVM)**: run `./mvnw install` first (`integration-tests` resolves the extension from the local Maven repo), then:
  `./mvnw -pl integration-tests verify -DskipITs=false`
- **Native build + ITs (CI flags)**:
  `./mvnw -B install -Dnative -Dquarkus.native.container-build -Dnative.surefire.skip`
  - `-Dnative.surefire.skip` is required: `integration-tests/pom.xml` sets `<skipTests>${native.surefire.skip}</skipTests>` in the `native-image` profile without defining a default value, leaving the property unresolved if omitted.
  - Omit `-Dquarkus.native.container-build` only when `native-image` is on `PATH` (provided by Mandrel in `.devcontainer/devcontainer.json`).

## Test prerequisites & quirks

- **`runtime` tests**: plain JUnit against `FakeKvBucket` — no Docker or Quarkus runtime needed.
- **`deployment` & `integration-tests`**: require a running **Docker** daemon for the NATS JetStream dev service. Dev-service containers persist across runs, so every test must clear its bucket in `@BeforeEach` (`invalidateAll()` or HTTP `DELETE` in ITs).
- **`deployment/pom.xml` Surefire split**: `@QuarkusTest` and `QuarkusExtensionTest` cannot share a JVM fork, so Surefire runs two separate executions:
  - `default-test`: `**/NatsCache*Test.java` (`@QuarkusTest`)
  - `extension-scenario-tests`: `**/Bucket*Test.java` (`QuarkusExtensionTest` + `BucketCollisionValidatorTest`)
  - Any new test class in `deployment/` that does not match one of those two globs will be silently skipped.
- **`QuarkusExtensionTest` classloader (`BucketCollisionTestBase`)**: test classes reload in the application classloader, so shared helpers must live in a superclass (`BucketCollisionTestBase`) and fixture beans must be added as compiled classes, not `.java` sources.
- **Silent Caffeine fallback**: missing `quarkus.cache.type=nats` (or `quarkus.cache.<name>.type=nats`) falls back to Caffeine and most tests still pass. Keep `NatsKvCacheNativeIT#backendIsTheNatsKvCache` passing when touching wiring.
- **IT harness (`NatsKvCacheNativeIT`)**: `@QuarkusIntegrationTest` has no CDI injection; it drives the built jar/native runner over HTTP via `CacheSmokeResource` on `quarkus.http.test-port` (default `8081`).

## Toolchain, CI & POM gotchas

- **Java 17 target vs JDK 25 devcontainer**: `pom.xml` pins `<maven.compiler.release>17</maven.compiler.release>` and `.github/workflows/build.yml` builds on Temurin 17, while `.devcontainer/devcontainer.json` ships Mandrel JDK 25. Keep target at 17 unless deliberately migrating.
- **CodeQL (`codeql.yml`)**: uses `build-mode: none` (source scan only, no Maven compilation).
- **Standalone parent POM**: root `pom.xml` deliberately does **not** inherit `io.quarkus:quarkus-extensions-parent` (its grandparent imports `quarkus-bom:${project.version}`, breaking resolution against `999-SNAPSHOT`). Keep the standalone parent and explicit `quarkus-bom` import.
- **Project version**: POMs use `999-SNAPSHOT` (Quarkiverse convention); `README.md` still shows `1.0.0-SNAPSHOT` and is stale.
- **Unmanaged pins & Dependabot**: `quarkus-nats-jetstream-client` and `assertj-core` are not in `quarkus-bom` and are pinned in root `pom.xml`. `.github/dependabot.yml` ignores major updates (`version-update:semver-major`) for `io.quarkus:*` and `io.quarkiverse.reactivemessaging.nats-jetstream:*`.
- **Jackson 2 dependency (`runtime/pom.xml`, `NatsCacheJson`)**: codecs require Jackson 2 `ObjectMapper.DefaultTyping.EVERYTHING` (`NON_FINAL_AND_ENUMS` fails for final POJOs and `List.of`/`Map.of`). Keep direct `com.fasterxml.jackson.*` dependencies rather than `io.quarkus:quarkus-jackson` (which wraps Jackson 3 in Quarkus 4.x) unless redesigning the codec.

## Architecture & design constraints

- **Configuration keys**:
  - Backend selector belongs to core Quarkus Cache: `quarkus.cache.type=nats` or `quarkus.cache.<name>.type=nats` (not `quarkus.cache.caches.<name>.type`).
  - Extension settings live under `quarkus.nats-cache.caches.<name>.{bucket,ttl,history}` and `quarkus.nats-cache.allowed-packages`.
  - Dotted cache names (e.g. `my.cache`) cannot be addressed by SmallRye per-name properties.
- **`runtime/`**:
  - `NatsCacheBuildRecorder`: captures `NatsCachesBuildTimeConfig` (`BUILD_AND_RUN_TIME_FIXED`) and application packages, creating one `CacheTypeAllowList` instance per app (do not convert to a static/singleton — keeps multiple apps/restarts isolated). Resolves Quarkiverse `Client` from Arc.
  - `NatsKvCacheImpl`: cache policy only (no Quarkiverse or JNats imports). Features lazy one-shot bucket provisioning (`bucketReady`, cleared on failure for retry), per-encoded-key stampede protection (`inFlight` detached from caller cancellation via `.copy()`), sync loaders offloaded to `NatsCacheWorkers.EXECUTOR`, and fail-open reads/writes (except invalidations, which propagate failures).
  - Ahead-of-upstream `expiresAfter` overloads (`get`/`getAsync` with `Duration`) live on `NatsKvCacheImpl` with upstream signatures; convert to `@Override` when upgrading to a Quarkus version that adds them to `Cache`.
  - `kv/KvBucket` & `kv/QuarkiverseKvBucket`: sole NATS/JNats boundary. Translates miss-as-null -> `ReadEntry.miss()`, valueless entries -> `ReadEntry.unreadable()`, error `10071` (`JS_WRONG_LAST_SEQUENCE`) -> `CreateResult.ALREADY_CACHED`, TTL-less creates -> `MAX_TTL` (`Integer.MAX_VALUE` seconds; writes fail open if a pre-existing bucket disallows per-message TTL), purge markers -> 1s TTL with plain-purge fallback, and blocking `purgeAll()` -> `NatsCacheWorkers.EXECUTOR`.
  - `codec/`: `NatsCacheJson` separates `PLAIN` from `typedMapper` (Jackson rejects tree ops under `EVERYTHING` typing). `KeyCodec` serializes to deterministic JSON + base64url (<=255 chars; special-cases `CompositeCacheKey` and reflects `DefaultCacheKey.cacheName`), memoizing round-trip checks per class; unordered collections (`HashSet`, etc.) in keys produce non-deterministic keys. `ValueCodec` wraps values in `{"data": ...}` so `null` results cache as `{"data": null}`. Writes are create-only (first write wins; unreadable entries are purged before reload).
- **`deployment/`**:
  - `NatsCacheProcessor`: registers `DefaultCacheKey` fields plus all non-interface, non-annotation application classes for Jackson reflection whenever any cache is `nats`-typed; third-party key/value types still require `@RegisterForReflection` (and `quarkus.nats-cache.allowed-packages`).
  - `BucketCollisionValidator`: runs in `ValidationPhaseBuildItem` via `NatsCacheInfo.resolve`. Fails on derived bucket collisions unless *every* sharing cache explicitly sets `quarkus.nats-cache.caches.<name>.bucket`.

## Domain & workflow docs

- `GLOSSARY.md` defines canonical terms (`nats-typed cache`, `encoded key`, `envelope`, `cached-null result`, `unreadable entry`, `marker`, `limit marker`, `fail open`, `bucket collision`, etc.) — follow `docs/agents/domain.md` and avoid synonyms listed under `_Avoid_`.
- GitHub issue and label conventions (`gh` CLI, `Saphri/quarkus-nats-cache`) live in `docs/agents/issue-tracker.md` and `docs/agents/triage-labels.md`.
- `.opencode/` and `openspec/` are gitignored in `.gitignore`; never commit files from those directories.
