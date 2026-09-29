# AGENTS.md

Quarkus extension: NATS JetStream KV as a backend for the Quarkus cache API (`@CacheResult`, `io.quarkus.cache.Cache`). User-facing docs live in `README.md`; this file is working context for agents.

## Build & test (Maven wrapper only)

- Always use `./mvnw` (pins Maven 3.9.14). No local Maven, no CI, no lint/format tooling — the Maven build + tests are the only gate.
- `./mvnw install` — builds all modules and runs both unit suites. Needs **Docker running** for the deployment suite (see below).
- Single test: `./mvnw -pl runtime test -Dtest=KeyCodecTest` (or `-Dtest=Class#method`). Same pattern for `-pl deployment`.
- Integration tests are **skipped by default** (`skipITs=true` in `integration-tests/pom.xml`). Run them explicitly, after `./mvnw install` (the IT module resolves the extension from the local repo):
  - JVM jar: `./mvnw -pl integration-tests verify -DskipITs=false`
  - Native image: `./mvnw install -DskipTests && ./mvnw -pl integration-tests verify -Dnative` (needs `native-image` on PATH; devcontainer ships Mandrel)

## Test prerequisites & quirks

- `runtime` tests are plain JUnit (no Docker). `deployment` tests are `@QuarkusTest` and rely on the NATS JetStream **dev service**, which auto-starts a Docker container when no broker is configured — zero broker setup, but Docker must be up.
- Dev-service containers **persist between runs**. Existing tests clean their bucket in `@BeforeEach` (`invalidateAll()` / HTTP DELETE); new tests must do the same — never assume an empty server.
- If `quarkus.cache.type=nats` is missing, caches silently fall back to Caffeine and tests can still pass. `NatsKvCacheNativeIT#backendIsTheNatsKvCache` guards against exactly this; keep that guard in mind when touching backend wiring.
- `@QuarkusIntegrationTest` has no bean injection — drive the app over HTTP via `CacheSmokeResource`; port comes from `quarkus.http.test-port` (default 8081).

## Module map

- `runtime/` — `NatsKvCacheImpl` (cache impl + stampede protection), `codec/` (`KeyCodec`: JSON+base64url keys; `ValueCodec`: `{exp, data}` JSON envelope), `NatsCachesBuildTimeConfig`, `NatsCacheBuildRecorder`.
- `deployment/` — build steps: cache-manager info, native-image reflection registration (all app classes when any cache is `nats`-typed), and `BucketCollisionValidator` (build fails if two caches derive the same default bucket).
- `integration-tests/` — smoke app (`src/main`) + `NatsKvCacheNativeIT`.

## Gotchas (verified — don't "fix" these)

- The parent pom **deliberately does not inherit** `io.quarkus:quarkus-extensions-parent` (see comment in root `pom.xml`): it would re-interpolate `${project.version}` against the SNAPSHOT version and break BOM resolution. Keep the standalone parent + explicit `quarkus-bom` import.
- Project version is `999-SNAPSHOT` (Quarkiverse convention). The README's `1.0.0-SNAPSHOT` dependency snippet is stale — trust `pom.xml`.
- Per-cache backend selector is `quarkus.cache.<name>.type` (core cache extension), **not** `quarkus.cache.caches.<name>.type`. This extension's own properties are `quarkus.nats-cache.caches.<name>.*` (`bucket`, `ttl`, `history`).
- `expiresAfter` overloads are not yet on `io.quarkus.cache.Cache` in Quarkus 3.40; they exist ahead of upstream as public methods on `NatsKvCacheImpl` with the exact upstream signatures. When upgrading Quarkus, check whether they landed upstream and convert them to overrides.
- Pinned versions: Quarkus `3.40.0`, quarkiverse `reactive-messaging-nats-jetstream` `3.39.7` (not BOM-managed). Java target is 17 (`maven.compiler.release`) even though the toolchain here is Java 25.
- Key encoding must be deterministic (ordered collections only) and fit 255 bytes; fail-fast validation at store time is intentional — don't move it to read time.

## Workflow notes

- OpenSpec is used for spec-driven changes but is **local-only**: `openspec/` and `.opencode/` are gitignored. Repo-local commands: `/opsx-propose`, `/opsx-explore`, `/opsx-apply`, `/opsx-archive`.
- Devcontainer (`.devcontainer/devcontainer.json`) defines the intended environment: Java 25 + Mandrel, Maven, docker-in-docker.
