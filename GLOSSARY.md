# Glossary

Domain language for quarkus-nats-cache. Use these names in code, issues, and discussion; one entry per concept.

## KV bucket access

The seam between cache policy and NATS transport: a narrow interface over one NATS KV bucket — `get`, `create`, `purge`, `keys`, `provision`, `purgeAll`. The production adapter wraps the quarkiverse JetStream client and absorbs its quirks (miss-as-null, wrong-last-sequence, TTL substitution, purge-marker fallback); tests implement the interface directly.

## Read entry

The read result: the three-outcome answer to one entry read — **hit** (payload), **miss**, or **unreadable entry** (present, but this application's codecs cannot decode it; purged before its reload is stored).
