# AGENTS.md

Quarkus extension: NATS JetStream KV as a backend for the Quarkus cache API (`@CacheResult`, `io.quarkus.cache.Cache`). User-facing docs live in `README.md`; this file is working context for agents.

## Build & test (Maven wrapper only)

- Always use `./mvnw` (pins the Maven version in `.mvn/wrapper/maven-wrapper.properties`; Dependabot keeps it current). No local Maven needed.
- `./mvnw install` — builds all modules and runs both unit suites. Needs **Docker running** for the deployment suite (see below).
- Single test: `./mvnw -pl runtime test -Dtest=KeyCodecTest` (or `-Dtest=Class#method`). Same pattern for `-pl deployment`.
- Integration tests are **skipped by default** (`skipITs=true` in `integration-tests/pom.xml`). Run them explicitly, after `./mvnw install` (the IT module resolves the extension from the local repo):
  - JVM jar: `./mvnw -pl integration-tests verify -DskipITs=false`
  - Native image: `./mvnw install -DskipTests && ./mvnw -pl integration-tests verify -Dnative` (needs `native-image` on PATH; devcontainer ships Mandrel)

## CI (GitHub Actions)

- `.github/workflows/build.yml` (push to main + PRs, ubuntu, JDK 25 Temurin): `./mvnw -B clean install -Dno-format`, then the native step `./mvnw -B install -Dnative -Dquarkus.native.container-build -Dnative.surefire.skip`. Quarkus 4.x needs JDK ≥ 21 (25 recommended); with 17 the `quarkus-extension-processor` annotation processor fails to load (class file version 65 vs 61) — keep the CI JDK at 21.
- The `native-image` profile in `integration-tests/pom.xml` reads `${native.surefire.skip}`, which **no pom defines** — CI passes it as a flag. Mirror that when running `-Dnative` locally, or surefire's skip setting silently stays unresolved.
- `.github/workflows/codeql.yml`: CodeQL for Java with `build-mode: none` (no compile step; enables incremental overlay scans on PRs).

## Test prerequisites & quirks

- `runtime` tests are plain JUnit (no Docker). `deployment` tests are `@QuarkusTest` and rely on the NATS JetStream **dev service**, which auto-starts a Docker container when no broker is configured — zero broker setup, but Docker must be up.
- Dev-service containers **persist between runs**. Existing tests clean their bucket in `@BeforeEach` (`invalidateAll()` / HTTP DELETE); new tests must do the same — never assume an empty server.
- If `quarkus.cache.type=nats` is missing, caches silently fall back to Caffeine and tests can still pass. `NatsKvCacheNativeIT#backendIsTheNatsKvCache` guards against exactly this; keep that guard in mind when touching backend wiring.
- `@QuarkusIntegrationTest` has no bean injection — drive the app over HTTP via `CacheSmokeResource`; port comes from `quarkus.http.test-port` (default 8081).

## Module map

- `runtime/` — `NatsKvCacheImpl` (cache policy: fail-open read/write, stampede protection, provisioning retry; no quarkiverse/JNats imports), `kv/` (KV bucket access: `KvBucket` seam with six operations over encoded keys, `ReadEntry` (the `get` outcome: hit / miss / unreadable), `CreateResult`, and the `QuarkiverseKvBucket` adapter that owns every NATS call and quirk — miss-as-null, 10071 → `ALREADY_CACHED`, max-TTL substitution, purge-marker fallback, bucket configuration, raw-JNats stream purge on `NatsCacheWorkers`; unit tests use `FakeKvBucket`, the adapter itself is covered by the deployment suite), `codec/` (`KeyCodec`: JSON+base64url keys; `ValueCodec`: `{data}` JSON envelope; both are instances built by the recorder around one per-app `CacheTypeAllowList`, so no static allow-list state; `expiresAfter` is a server-side per-message TTL, not part of the payload), `NatsCachesBuildTimeConfig`, `NatsCacheBuildRecorder`.
- `deployment/` — build steps: cache-manager info, native-image reflection registration (all app classes when any cache is `nats`-typed), and `BucketCollisionValidator` (build fails if two caches derive the same default bucket).
- `integration-tests/` — smoke app (`src/main`) + `NatsKvCacheNativeIT`.

## Gotchas (verified — don't "fix" these)

- The parent pom **deliberately does not inherit** `io.quarkus:quarkus-extensions-parent` (see comment in root `pom.xml`): it would re-interpolate `${project.version}` against the SNAPSHOT version and break BOM resolution. Keep the standalone parent + explicit `quarkus-bom` import.
- Project version is `999-SNAPSHOT` (Quarkiverse convention). The README's `1.0.0-SNAPSHOT` dependency snippet is stale — trust `pom.xml`.
- Per-cache backend selector is `quarkus.cache.<name>.type` (core cache extension), **not** `quarkus.cache.caches.<name>.type`. This extension's own properties are `quarkus.nats-cache.caches.<name>.*` (`bucket`, `ttl`, `history`).
- `expiresAfter` overloads are not yet on `io.quarkus.cache.Cache` in Quarkus 3.40; they exist ahead of upstream as public methods on `NatsKvCacheImpl` with the exact upstream signatures. When upgrading Quarkus, check whether they landed upstream and convert them to overrides.
- Pinned versions: Quarkus `4.0.0.Beta1`, quarkiverse `quarkus-nats-jetstream-client` `4.0.0.Beta1` (not BOM-managed; the client-only split of `quarkus-reactive-messaging-nats-jetstream`, same groupId; its dev service runs `nats:2.15`). Java target is 21 (`maven.compiler.release`) even though the toolchain here is Java 21.
- Jackson: Quarkus 4.x ships `io.quarkus:quarkus-jackson` over **Jackson 3** (`tools.jackson.*`, whose `DefaultTyping` has no `EVERYTHING`). The codec needs Jackson 2's `DefaultTyping.EVERYTHING`, so `runtime/pom.xml` depends on `com.fasterxml.jackson.{core,datatype}` 2.22.2 directly (still pinned by the 4.x BOM), not on `quarkus-jackson`. Both Jacksons coexist on the classpath; keep the codecs on Jackson 2 unless the typing design is reworked.
- Cache writes are **create-only** (first write wins; a create also succeeds over delete/purge/expiry markers). Writes use the quarkiverse `KeyValue.create(key, value, Duration)` (≥ 3.40.7); quarkiverse has no create without a TTL (its `put` is an overwriting put, not a create), so entries without `expiresAfter` are created with the max TTL (`Integer.MAX_VALUE` seconds) and the bucket max age still caps them. Consequence: a pre-existing bucket without `allow_msg_ttl` refuses every write (fail open, nothing cached). Purges carry a 1s marker TTL (`purge(key, Duration)`) and fall back to a plain purge on buckets without `allow_msg_ttl`. Unreadable entries are purged before their reload is stored. Buckets are provisioned with a `limitMarker`, the setting that enables `allow_msg_ttl` on a KV bucket (quarkiverse ≥ 3.40.6 maps it). Needs NATS server 2.11+ (dev service runs 2.15).
- Markers never surface as entries: the quarkiverse `get` reports a null item for a key whose only revision is a purge/delete/expiry marker, so a read right after `invalidate(key)` is a clean **miss** — not an **unreadable entry** (verified by `NatsCachePurgeMarkerTest`; the "purge then reload" path is for foreign/legacy payloads only). Don't "fix" this by mapping valueless entries to a miss.
- Key encoding must be deterministic (ordered collections only) and fit 255 bytes; fail-fast validation at store time is intentional — don't move it to read time.

## Workflow notes

- Dependabot is active (`.github/dependabot.yml`): `maven` daily, `devcontainers` and `github-actions` weekly. The maven updates also cover the **Maven wrapper** — Dependabot bumps `org.apache.maven:apache-maven` in `.mvn/wrapper/maven-wrapper.properties` (already done: 3.9.14 → 3.9.16) and plugin versions in the poms. Expect these PRs; don't revert or manually fight them.
- OpenSpec is used for spec-driven changes but is **local-only**: `openspec/` and `.opencode/` are gitignored. Repo-local commands: `/opsx-propose`, `/opsx-explore`, `/opsx-apply`, `/opsx-archive`.
- Devcontainer (`.devcontainer/devcontainer.json`) defines the intended environment: Java 21 + Mandrel, Maven, docker-in-docker.

## Agent skills

### Issue tracker

Issues live in this repo's GitHub Issues (`Saphri/quarkus-nats-cache`), managed via the `gh` CLI. See `docs/agents/issue-tracker.md`.

### Triage labels

Default five-role vocabulary: `needs-triage`, `needs-info`, `ready-for-agent`, `ready-for-human`, `wontfix`. See `docs/agents/triage-labels.md`.

### Domain docs

Single-context: `GLOSSARY.md` and `docs/adr/` at the repo root. See `docs/agents/domain.md`.
