# Glossary

Domain language for quarkus-nats-cache. Use these names in code, issues, and discussion; one entry per concept.

## KV bucket access

The seam between cache policy and NATS transport: a narrow interface over one NATS KV bucket — `get`, `create`, `purge`, `keys`, `provision`, `purgeAll`. The production adapter wraps the quarkiverse JetStream client and absorbs its quirks (miss-as-null, wrong-last-sequence, TTL substitution, purge-marker fallback); tests implement the interface directly.

## Read result

The three-outcome answer to one entry read: **hit** (the stored payload), **miss** (no live entry), or **unreadable entry** (present, but this application's codecs cannot decode it — the adapter reports an entry without a value, the cache reports a payload its value codec rejects). An unreadable entry is purged before its reload is stored; a failed read (transport error) is treated like a miss and never purges.
