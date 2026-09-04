# Spotify design panel — review

Chair's record of the panel discussion. Three engineers designed Spotify independently in 45 minutes each; this file reconciles the three write-ups into one design, which is published at `content/designs/spotify.md`.

## Panel

- **Principal Engineer** (`principal.md`): product flows end to end. Playback contract between client and server, catalog legal model (recording versus work versus track), playlist operational transform, and the royalty ledger as an auditable append-only record.
- **Distinguished Engineer** (`distinguished.md`): storage and delivery at planet scale. Encoding pipeline, content-addressed immutable objects, four-tier cache hierarchy, own PoPs plus commercial CDNs, multi-region placement of catalog and user data, and the cost model.
- **Sr Staff Engineer** (`sr-staff.md`): the data side. Search (FST typeahead, BM25 plus GBDT rerank), recommendations (batch candidates, real-time rerank, feature store), the play-event pipeline as at-least-once in and idempotent out, and social (follow graph, collaborative playlists, presence, Jam).

## Where the three designs agree

All three converged on the same spine without talking to each other, which is the strongest signal the panel gets that these are the right calls.

- **The client is a component, not a UI.** All three give it a local audio cache, a durable event buffer, prefetched manifests for the next tracks, and a local copy of library and playlists. The startup-latency story is won or lost in the client.
- **Playback has one hard dependency: the catalog.** Search, recommendations, social and the event pipeline can all be down and a play must still start. Principal and Sr Staff both state this as an availability requirement; Distinguished's failure table assumes it.
- **Whole encrypted file per rendition, fetched by HTTP byte range.** Principal and Distinguished both reject HLS/DASH for music: tracks are short, bitrate is constant, one object per rendition means fewer cache entries and a seek is one range request. AES-128-CTR is chosen precisely because any byte range decrypts independently.
- **The hot set is tiny and fits in one PoP.** The top 1M tracks are roughly 20 to 25 TB across renditions and account for about 80% of plays. Every design pre-warms it to the edge and treats the long tail as the only thing that goes upstream.
- **Manifests, keys and the first seconds of the next tracks are prefetched.** Nobody puts a server round trip on the tap-to-audio path. The resolve call happens when the queue changes, not when the user taps.
- **Play events are at-least-once from the device, deduplicated server-side on a client-generated id, partitioned by user in Kafka.** All three arrive at the same event contract independently: client id per play, sequence within the play, cumulative audio milliseconds, server receipt timestamp.
- **Two independent computations for the money.** A streaming job for freshness and a batch recount from raw events, diffed, with a page above 0.01%. Nobody trusts a single path for royalties.
- **Correctness budget goes to royalties; staleness is accepted everywhere else.** Search index freshness in minutes, recommendations a day stale, presence 30 s stale: invisible to users. A 0.1% overcount is real money paid to the wrong rights holder.
- **Market availability is filtered at query time from the catalog, never baked into a snapshot.** Sr Staff and Principal both call out that a Sunday recommendation snapshot can include a track delisted on Tuesday; Distinguished stores availability as a bitmap on the track view and checks it at resolve.
- **Release-day Friday midnight is the one predictable thundering herd,** and it is solved by pre-warming encrypted bytes with a time-locked key service, not by scaling origin.

## Disagreements and resolutions

### 1. Baseline numbers

The three estimate tables disagree by 2 to 3x on the inputs. Principal: 250M DAU, 4B plays/day, 30M peak concurrent, 6B events/day. Distinguished: 250M DAU, 8B plays/day, 50M peak concurrent, 8B events/day. Sr Staff: 100M DAU, 3B plays/day, 15M peak concurrent, 30B events/day (a heartbeat every 30 s plus seeks, likes and adds). The spread on events is the one that matters: 70k/s versus 1M/s peak changes the Kafka cluster by an order of magnitude.

**Resolution:** 250M DAU, 5B plays/day, 30M peak concurrent, and about 10 events per play, so 50B events/day, 600k/s average, 1.5M/s peak. The panel takes Sr Staff's event model because it is the one that includes the 30-second heartbeat that the royalty rule needs (audio milliseconds, not wall time, must be reported incrementally or a crash loses the whole session) and the CDN metrics that steering needs. Principal's three events per play undercounts what the clients actually send. Sr Staff's DAU of 100M is low against public numbers; 250M is used. At 500 B per event that is 25 TB/day raw, about 5 TB/day after columnar compression, which is a modest Kafka cluster (200 to 400 partitions) and not a design driver. None of the other decisions move when these numbers move by 2x, which is the point of estimating.

### 2. Delivery container and rendition set

Principal and Distinguished both chose whole file per rendition with byte-range fetch and a per-second byte index in the manifest; Principal asked whether HLS uniformity with a podcast/video pipeline would be worth it. Sr Staff wrote "signed CDN URL for the audio segments" in passing and did not design delivery. On renditions, Principal encodes Vorbis 96/160/320 plus AAC 256 (22 MB per track); Distinguished adds AAC 128 and FLAC at 850 kbps (50 MB per track, 5 PB catalog) and raises Opus as a 20% egress saving that breaks partner devices.

**Resolution:** whole file, byte range, AES-128-CTR, content-hash file ids. Segmentation is the wrong tool for a 3.5-minute constant-bitrate asset: it multiplies objects by 100, adds a manifest per segment, and buys mid-track adaptive bitrate that music rarely needs. Operational uniformity with a video pipeline is not worth 60 billion objects. On renditions the panel takes Distinguished's set minus nothing: Vorbis 96/160/320, AAC 128/256, and FLAC, but FLAC is encoded lazily on first request for the lossless tier rather than for all 100M tracks up front, which keeps the catalog at about 3 PB until the tier has users. Opus is deferred until client telemetry shows under 1% of plays on devices without an Opus decoder; the egress saving (about $700k/month on Distinguished's numbers) is real but a second codec migration in the same year as launch is not.

### 3. Manifest TTL and where playback policy is enforced

Principal caches the manifest on device for 24 hours and enforces device limits and subscription lapse through a 30-second heartbeat with a 90-second Redis TTL, because a cached manifest means resolve cannot be the enforcer. Distinguished expires manifests in 1 hour so that rights changes do not lag behind a long-lived client cache, and ties events to a signed token from resolve. These are in tension: a 1-hour manifest puts resolve back on the critical path every hour for a listener with a stale queue, and a 24-hour manifest leaves a delisted track playable for a day.

**Resolution:** split the manifest into two parts with different lifetimes. The immutable part (file ids, byte index, duration, gain, CDN base URLs) is cacheable for 24 hours and beyond, because content-hash ids never change meaning. The entitlement part (signed CDN token and session-wrapped content key) has a 1-hour TTL and is renewed by the same 30-second heartbeat that Principal already sends, in a batch for the whole prefetched queue. Availability is enforced at three points: the playback service refuses to resolve an unavailable track, the key service refuses to renew a key for a track no longer licensed in the user's market, and the CDN token expires within the hour. A rights withdrawal therefore takes effect within 60 minutes without any client cache invalidation, and the tap path stays at zero round trips.

### 4. Catalog store

Principal chose sharded Postgres by release id with Redis in front, because pulling a release needs a transaction across release, tracks and availability. Distinguished chose a single-writer, globally replicated relational store (Spanner-style, or Postgres with logical replicas per region) partitioned by track id, with a per-region Redis "track view" invalidated by a change stream. Sr Staff chose Cassandra with the 200 GB held in memory per region, arguing multi-region availability matters more than joins.

**Resolution:** single-writer relational with a read replica per region and a per-region Redis track view. Sr Staff's argument for Cassandra is availability, but the read path never touches the database; it reads Redis at a 99%+ hit ratio and a replica on miss, both of which are local to every region already. What the database has to be good at is the write side: a label pulling a release, a rights change across 30 territories, a work-share renegotiation, all of which are multi-row transactions that Cassandra makes the application's problem. At 300 GB and 10 writes/s the relational store does not need sharding at all; Principal's release-id shard key is kept as the partitioning key so that sharding is a config change later rather than a migration. The panel adopts Principal's legal model (recording, work, track, release, availability, versioned work shares) because the royalty ledger depends on it, and Distinguished's territory bitmap on the track view for constant-time availability checks.

### 5. Playlist collaboration model

Principal: an op log `(playlist_id, revision, op_seq, op)` in Cassandra with three op types over item uids, server-side rebase of stale clients, a materialized snapshot every 50 ops, and a resync if a client is more than 200 revisions behind. Distinguished: no op log; entries carry a fractional position key so an insert between two items is one write, plus a version counter for delta sync. Sr Staff: a server-sequenced op log with `after_item_id`, adds and removes commute, concurrent moves are last-writer-wins, and clients subscribe to the op stream over WebSocket. All three rejected a CRDT because the server is always available to sequence operations.

**Resolution:** server-sequenced op log over item uids (Principal and Sr Staff) with Distinguished's fractional position key as the representation inside the materialized snapshot. The op log is what makes collaboration and audit work ("who removed my track" is a query); the fractional key is what makes the snapshot cheap to maintain (a move rewrites one row, not fifty). Concurrent moves of the same item are last-writer-wins by server sequence, which is Sr Staff's simplification and the panel accepts it; a three-way rebase for moves is complexity nobody can point to a user complaint for. Live updates are pushed over the existing WebSocket only for a playlist the client currently has open; otherwise the client syncs the op log on foreground. The CRDT question is closed unless offline editing of shared playlists becomes a product requirement, at which point the op log is still the right server-side representation and the CRDT is a client concern.

### 6. The royalty ledger: which computation is the truth, and where dedup lives

Principal: dedup in the streaming job on `event_id` over a 7-day window, fold into sessions so one session yields at most one stream, but the ledger is written by a daily batch recount on a separate code path; streaming feeds only dashboards. Distinguished: a 48-hour per-user dedupe window in the stream processor, streaming writes the ledger, nightly batch reconciles. Sr Staff: Flink keyed state over `(play_uuid, seq)` for 7 days, and the ledger sink is an idempotent upsert keyed by `play_uuid`, so exactly-once is a property of the ledger's primary key rather than of any window; monthly reconciliation against the lake.

**Resolution:** Sr Staff's idempotent ledger is the exactly-once guarantee, Principal's daily batch recount is the auditor, and the 7-day window wins over 48 hours. The reasoning: a dedup window is a probabilistic guard that is only as good as its width, while a primary key on `play_uuid` is a constraint that holds regardless of Flink failovers, replays, or a week-offline device. So the streaming job writes qualified plays to the ledger by upsert and those rows are the ledger. The daily Spark recount from raw events on a separate code path is not the ledger; it is the check, and monthly close is blocked while the diff exceeds 0.01% for any label. Distinguished's 48-hour window is too narrow for the offline-upload case and is widened to 7 days; Principal's 30-day acceptance with quarantine beyond is adopted for late events. Artist-facing counts come from the streaming path with a "provisional" label until close. Principal's append-only monthly snapshot rows with adjustment rows for corrections is adopted unchanged; nobody argued against it.

### 7. Friend Activity: push or poll

Sr Staff chose a 30-second poll for the Friend Activity sidebar: a Redis `MGET` of 20 keys per open sidebar, 170k reads/s across 5M open sidebars, stateless. Principal did not design presence but does use a push channel to tell the older device to pause when a newer device starts, and to nudge a client to sync a collaborative playlist. Sr Staff also uses WebSocket for playlist op streams and Jam. So a persistent push connection exists in all three designs whether or not presence uses it.

**Resolution:** poll for Friend Activity, push for control messages, one connection. The WebSocket carries device-pause, playlist-changed and Jam state because those are rare and latency-sensitive. Friend Activity stays on a 30-second poll because a 30-second staleness bound is invisible, the read is a single `MGET`, and fanning out 2B "now playing" updates a day to 5M subscribed sidebars over push would add per-connection subscription state for a feature where wrong-for-30-seconds is fine. If the product ships "listen along" the presence write already exists and the push channel already exists; only the subscription fan-out is new. Private-session mode is enforced at the presence write, per Sr Staff, so a reader bug cannot leak it.

### 8. Multi-region placement of user data and play events

Distinguished: user data (playlists, library, follows) in Cassandra with a home region per user, local quorum in the home region, async replication to one DR region, and an accepted loss of seconds of playlist edits on regional failover. Principal: Cassandra by playlist id and user id, region not specified, read-your-writes via the writing device's local copy. Sr Staff: one global Kafka and one global lake, and an open question about whether EU play events must stay in the EU, which would force per-region royalty counting and a federated rollup.

**Resolution:** user data is homed per region as Distinguished proposed, and the loss of seconds of edits on a regional failover is accepted because the client holds its own copy and replays unacknowledged ops on reconnect. Play events are ingested into a Kafka cluster in the region of the gateway that received them, with a region column, and the streaming royalty job runs per region writing to a single global ledger keyed by `play_uuid`; the idempotent key makes multi-writer safe. The lake is one logical lake with region-partitioned storage from day one. This costs almost nothing now and means that if a residency constraint arrives, the only change is to pin the EU partition's storage and compute to the EU and federate the rollup, rather than to split a global pipeline. Catalog is not personal data and replicates freely.

## Open questions, answered

### Principal

**1. Byte-range single file versus HLS/DASH segments.** Single file, settled in disagreement 2. Uniformity with a podcast or video pipeline does not justify 100x the objects for the asset class that carries 90% of plays. If a podcast pipeline exists it can serve music files too, because a whole file is the degenerate case of one segment.

**2. Hardware DRM.** Not for the lossy tiers; labels have accepted software encryption with device-bound keys for audio for a decade. If a lossless or early-release deal requires Widevine or FairPlay, it is built as a second delivery path for that tier only, with a segmented container, and the range-based path is left alone. The offline design does not change: a hardware-DRM licence is stored where the wrapped key is stored today and renewed by the same batch call.

**3. Batch as the source of truth for the ledger.** Reversed: the streaming job writes the ledger through an idempotent upsert, and batch is the auditor that gates monthly close (disagreement 6). A 24-hour lag on artist counts is acceptable and the product already labels them provisional; what is not acceptable is a ledger whose correctness depends on a batch job finishing.

**4. Postgres for catalog metadata.** Stay relational, single writer, replicas per region. At 100x ingest volume from user-generated audio the write rate is still about 1,000/s, which one primary handles; the catalog would be 30 TB, which is when the release-id partition key becomes an actual shard key. The panel would rather shard Postgres in three years than give up transactions on rights data today.

**5. Playlist OT scope.** Three ops with item uids is the right foundation and the panel does not want a CRDT (disagreement 5). Comments and reactions are separate entities keyed by item uid, not list operations, so they do not widen the op set. Live co-listening is Jam, which is its own small state machine and does not touch the playlist log.

### Distinguished

**1. Opus migration.** Carry both until telemetry shows under 1% of plays from devices lacking an Opus decoder, then encode Opus for new ingest and backfill the hot 10M tracks first, because that is where 95% of the egress saving is. The long tail can stay Vorbis-only indefinitely; storage for a duplicate low-bitrate rendition of the tail is a few hundred TB and not worth a project.

**2. Encryption model for the lossless tier.** Two paths, not one (Principal's open question 2 is the same question). Forcing every tier onto a segmented DRM container to keep one path would cost the whole-file simplicity for the 95% of plays that do not need it. Build the lossless path when there is a signed deal that requires it, and meter its egress separately from day one.

**3. Own PoP footprint.** The panel has no better egress pricing than the write-up assumed and treats 40 metros as the working number. The decisive argument for own PoPs is not price; it is consistent-hash placement across nodes in a PoP and pre-warm control before a release, which commercial CDNs do not expose at the granularity needed. Start with the 10 densest metros and revisit the break-even with real bills.

**4. Playlist store consistency.** Home-region Cassandra with async DR is enough (disagreement 8). A global transactional store would add cross-region latency to every playlist write to protect against a loss of seconds of edits during a regional failover that the client-side op replay already covers. Collaborative playlists do not change that: the op log is sequenced in the playlist's home region, and collaborators in other regions pay one cross-region hop per op, which is acceptable for a human-paced edit.

**5. Prefetch aggressiveness.** 15 seconds of the next 3 tracks stays as the default until the data team supplies skip-rate distributions by context. The panel's prior is that user playlists and albums skip rarely (depth 2 is enough) and radio/autoplay skips often (depth 3 with shorter heads). Make depth and head length a server-supplied per-context parameter in the manifest so it can be tuned without a client release, and cap speculative bytes at 5% of the user's daily traffic.

### Sr Staff

**1. Neural reranking for search.** Not in the initial architecture. Navigational queries dominate and BM25 plus popularity plus a GBDT with personalization features gets the first result right. If semantic search is on the roadmap it is a second retriever (dense, over the same track embeddings the recommendation system already builds) merged into the candidate set, not a replacement for the lexical index; the reranker can be upgraded independently when the candidate set is worth it.

**2. On-device recommendations.** Worth a prototype for Home reranking specifically, because the last-50-tracks context is already on the device and a small reranker over precomputed shelves removes an online feature-store read per Home open. Discover Weekly and radio candidate generation stay server-side because they need the ANN index. Do not design the feature store around it yet.

**3. Kafka transactions versus idempotent sinks.** Idempotent sinks (disagreement 6). Kafka transactional exactly-once has matured, but it protects the Kafka-to-Flink-to-Kafka hop and not the hop from a crashing mobile client that re-uploads a batch, which is where the duplicates come from. The ledger's primary key handles both. The 7-day keyed state is kept because it also feeds sessionization and is cheap.

**4. CRDT for collaborative playlists.** No (disagreement 5). Offline editing of a shared playlist is not a stated requirement, and if it becomes one, the server op log is still the right representation; the client would apply its offline ops on reconnect and take the server's sequence as authoritative, which is what it does today for a stale base revision.

**5. Regional data residency for play events.** Not a constraint today, but the pipeline is built region-partitioned from day one (disagreement 8) because doing so costs a region column and a per-region Kafka cluster now, and costs a rewrite of the batch layer later. The royalty ledger stays global because it is keyed by `play_uuid` and multi-writer safe; only raw event storage and training data would need to pin to a region.

## What got cut for 45 minutes

In a real interview none of the three candidates would have covered everything above, and the interviewer would not expect them to. What the panel would accept as out of scope, and what it would ask as follow-ups:

- **Cut: the encoding pipeline in detail.** Say "transcode to five renditions at ingest, encrypt with per-track keys, store under a content hash" and move on. Follow-up if time: how a re-delivered master avoids serving mixed bytes (content-hash ids plus a pointer flip).
- **Cut: own PoPs versus commercial CDNs and the cost model.** One sentence: delivery per stream is 250x cheaper than the royalty per stream, so optimise the CDN for hit ratio not cents. Follow-up: what happens at Friday midnight (pre-warm encrypted, time-lock the keys).
- **Cut: the full legal catalog model.** Recording versus work versus track is worth 30 seconds because it changes the ledger's key; the ER diagram with publishers and work shares is not. Follow-up: why the ledger is keyed by ISRC and not track id.
- **Cut: recommendations model internals.** State batch candidates plus real-time rerank plus one feature definition materialized twice. Follow-up: training-serving skew and how it is detected.
- **Cut: social features other than collaborative playlists.** Friend Activity and Jam are follow-ups: "how would you build the sidebar" is a good 5-minute question about push versus poll.
- **Cut: fraud.** Mention that suspect plays are marked, not deleted. Follow-up: what signals (uniform 31-second plays, many accounts per device).
- **Cut: multi-region and data residency.** Home-region user data, replicated catalog, region-partitioned events. Follow-up: what is lost in a regional failover and why that is acceptable.
- **Would not cut:** the tap-to-audio latency budget as a table, the event contract and the idempotent ledger, and the playlist op log. Those three are where the interviewer learns whether the candidate has built a player, a money pipeline, and a collaborative editor.
