# quarkus-nats-cache

Cache entries that live in a shared NATS JetStream KV bucket instead of in process memory. Several applications
can share a bucket, and whether an entry is still alive is decided by the server, not by the application.

## Cache and bucket

**nats-typed cache**:
A Quarkus cache whose entries live in a NATS KV bucket rather than in memory.
_Avoid_: NATS cache, NATS-backed cache, NATS cache backend

**Bucket**:
One named NATS KV store, the home of one cache's entries.
_Avoid_: stream, namespace, store, partition

**Entry**:
One live stored value for one encoded key — the unit a hit returns.
_Avoid_: item, record, cache value, cached entry

**Encoded key**:
The bucket-facing token a cache key is stored under; every cache key has exactly one.
_Avoid_: key, serialized key, base64 key

**Envelope**:
The single fixed shape every stored payload has, which is what tells this extension's entries apart from
foreign bucket contents.
_Avoid_: payload, wrapper, format, schema

**Cached-null result**:
An entry deliberately storing "this key resolves to nothing", so the loader is not run again for it.
_Avoid_: empty entry, null entry, missing entry, no value

**History**:
The earlier revisions a key keeps in the bucket; invalidation removes them along with the key.
_Avoid_: version history, changelog, revisions

**Unreadable entry**:
Bucket contents that are sound but belong to another application — this one cannot make sense of them.
_Avoid_: corrupt entry, invalid entry, broken payload

## Bucket operations

**Read entry**:
The whole answer to one lookup: a hit, a miss, or an unreadable entry. Nothing else is possible, and a
transport problem is not one of the three — it is reported separately.
_Avoid_: read result, lookup outcome, get response

**Hit**:
The key holds an entry this application can read.
_Avoid_: found, success

**Miss**:
The key holds no live entry — never stored, expired, and purged are all one answer.
_Avoid_: not found, absent, expired (an expired entry is only ever a miss)

**Create**:
Claims an empty key with a value; never replaces a live entry, so of competing writers the first one wins.
_Avoid_: put, set, update, overwrite, write

**Purge**:
Removes a key and all of its history from the bucket, leaving a purge marker behind.
_Avoid_: delete (a bucket delete keeps the history), clear, remove

**Purge all**:
Removes every entry of a bucket, history included — bucket-scoped, so caches that deliberately share a bucket
invalidate each other's entries as well.
_Avoid_: clear bucket, invalidateAll (the cache-side name of it)

**Provision**:
Takes a bucket if nobody has made one yet; an existing one is left exactly as it is.
_Avoid_: create bucket, initialize, ensure exists

**Marker**:
The valueless trace a purge or an expiration leaves in a bucket; it is bucket bookkeeping, never an entry, and
a read whose only leftover is a marker is a plain **miss**.
_Avoid_: tombstone, stale entry, leftover, placeholder

**Limit marker**:
The bucket setting whose presence is what allows per-item expiration on that bucket.
_Avoid_: allow_msg_ttl (the server's own name), TTL switch

## Cache-side behavior

**Invalidation**:
The cache-side family of removals — one key, the matching keys, or a whole bucket. Every invalidation is a purge.
_Avoid_: expire, clear, flush

**Stampede protection**:
Concurrent loads of one key run the loader once, for all of them.
_Avoid_: deduplication, request merging, in-flight map, lock

**Fail open**:
A cache-transport failure absorbed so the call completes uncached — cache trouble costs latency, never
correctness. Not everything is absorbed: invalidation failures do propagate.
_Avoid_: fallback, graceful degradation, swallow errors

## Expiration

**Bucket TTL**:
The ceiling on how long any entry in a bucket may live, fixed when the bucket is provisioned.
_Avoid_: global TTL, cache TTL, max age

**Per-item expiration**:
How long one entry lives, asked for per lookup and enforced by the server's clock.
_Avoid_: expiresAfter (the API's name for it), timeout, deadline, expiry

## Naming and collisions

**Derived bucket**:
A bucket name inferred from a cache name. The inference is not one-to-one, which is what makes it
ambiguous.
_Avoid_: default bucket, generated bucket, sanitized name

**Explicit bucket**:
A bucket name pinned in configuration instead of inferred from a cache name.
_Avoid_: custom bucket, named bucket

**Bucket collision**:
Two caches landing on one bucket because at least one of them got there by a derived name. Deliberate
sharing is not a collision: it needs every cache in the group to pin the bucket.
_Avoid_: name clash, duplicate bucket, shared bucket (that word is reserved for the deliberate case)

**Type allow-list**:
The classes the codecs may rebuild from a stored type name. Entries come back from a shared bucket, so a
stored type name is untrusted input.
_Avoid_: whitelist, allowed packages (a configuration knob, not the concept)

## Seams

**KV bucket access**:
The seam between cache policy and bucket mechanics: one narrow interface over one bucket, whose
implementations translate transport quirks into the clean outcomes above. A failure signal here always
means a real error.
_Avoid_: transport layer, NATS adapter, client
