# Glossary

**KV bucket access** — the module (`io.quarkiverse.nats.cache.runtime.kv`) that talks to one NATS KV bucket on the
cache's behalf. Its interface, `KvBucket`, takes encoded keys and opaque payloads and returns clean outcomes (a miss
is an empty optional, a refused create is `ALREADY_CACHED`, a failure is a real error). Everything about *what to do*
(fail open, retry provisioning, stampede sharing, first write wins) stays in the cache; everything about *how to talk
to NATS* lives behind this seam. The production adapter is `QuarkiverseKvBucket`.
