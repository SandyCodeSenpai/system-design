# Feature store — design panel, chair's record

## Panel

| Area | Lead's verdict |
|---|---|
| A, definitions and computation (DE 01 to 05) | One definition compiled to one tile spec runs in Spark and Flink; the registry is the only thing anything executes from, and every offline row carries `available_at` from the online commit. |
| B, serving and consistency (DE 06 to 10) | One Redis cluster, one hash per entity with a field per view, a Go serving service that returns a status per view and never a default, and a 15-minute checker whose match rate gates promotion. |
| C, platform (DE 11 to 15) | One region, three AZs, $50k expected and $60k ceiling; the wire returns None plus status, defaults live in the model's versioned FeatureService, and serving never calls the control plane. |
| D, adoption and evolution (DE 16 to 20) | Feast core narrowly, five pieces replaced; a node-local Go agent on the fraud and ranking pools; churn first and fraud last but load-tested from month 5; models reading from the store is the only metric. |

Engineers, by number and lens:

| # | Lens | # | Lens |
|---|---|---|---|
| 01 | Registry | 11 | Storage and cost |
| 02 | Batch computation | 12 | Availability |
| 03 | Streaming computation | 13 | Governance |
| 04 | Point-in-time correctness | 14 | Operations |
| 05 | Data quality | 15 | API and SDK |
| 06 | Online store | 16 | Adoption and build versus buy |
| 07 | Offline store | 17 | Embeddings |
| 08 | Training-serving consistency | 18 | Real-time use cases |
| 09 | On-demand features | 19 | Feature lifecycle and CI |
| 10 | Entities and keys | 20 | Scale engineering |

## The shape of the answer

Twenty engineers who never spoke to each other converged on the same skeleton. A feature is declared once, in Python, in git; CI compiles it into a registry that is the only thing batch, streaming, serving and the training join execute from, and the registry is never on the read path. Aggregations are mergeable tiles, so batch and streaming share one arithmetic and a backfill never rescans raw. Every offline row carries the instant serving could first have returned it, `available_at`, and the training join uses that instant rather than the event time, because the leak is exactly the materialisation delay. The online store is a rebuildable cache with a TTL on every row; the wire returns a value with a status and never invents one; defaults belong to the model. Skew is a metric with a threshold, not a code review.

Two things surprised me. First, the panel's numbers disagreed by 8x on the online store and 6x on the monthly bill, and every disagreement traced to one unstated input: encoded bytes versus raw bytes, requests versus rows, one region versus two. The design is only as good as the sentence that fixes each input, so the numbers table below names them. Second, the strongest single argument in the room was not about data at all: DE 16's point that 170k USD a month of people costs more than the machines, which is why fraud goes last, the platform pays for the coexistence year, and Feast's definition format survives while its serving does not.

## Cross-area decisions

### 1. Online key layout and value encoding

Lead B: one Redis hash per entity, one field per view holding a protobuf blob with a 4 B schema version and 8 B `available_at` header, `HPEXPIRE` per field; DE 06 rejected one key per (view, entity) on 240 GB of key overhead. Lead D: one key per entity per view with the entity in the hash tag, value a fixed-layout binary row with a registry-owned layout version, decoded in under 1 µs with zero allocations against 10 µs and 40 allocations for protobuf (DE 20).

**Decision:** B's key layout, D's value encoding. One hash per entity keyed by the surrogate int64 id, one field per immutable view id, so a fraud request is one pipelined HMGET per entity and three shards for three entities; the field value is DE 20's fixed-layout binary row for that view version with the 4 B layout version and 8 B `available_at` header B specified. The two proposals were never really in conflict: both co-locate an entity's views and replace a view atomically. The hash wins on key overhead and on the 64-views-per-entity cap; the fixed layout wins on decode cost, which is on the 10 ms path 200k times a second. The registry owns the layout version. Streaming and batch views are separate views by rule, so a Flink writer and a Spark writer never share a field.

### 2. Hot path topology, and the SDK never reads Redis

Lead D: one Go binary in two modes, a node-local DaemonSet agent over a Unix socket on the fraud and ranking pools (0.3 ms p99 hop) and a zone-affine gRPC Deployment for the other 28 teams; DE 18 wanted the SDK to read Redis directly. Lead B: a central Go serving service, and an explicit no to SDK-direct reads because status semantics, hedging, the small-entity cache and the served log all live in the service. Lead C: gRPC with an 8 ms client deadline, hedge at 5 ms, retry only on immediate UNAVAILABLE.

**Decision:** D's two-mode binary, with B's contents inside it and C's wire contract. The agent implements everything B put in the service: per-view status and `available_at`, hedging, the small-entity cache, the in-process authz bitset from the snapshot, on-demand execution and the served-log write. No SDK reads Redis directly, in any language, ever; DE 18's latency argument is answered by the Unix socket, not by three reimplementations of decode and hedge. Numbers: fraud carries an 8 ms deadline, hedges to the other replica at 3 ms (D's budget lands at 6.5 ms p99 with the hedge), retries only on an immediate UNAVAILABLE; ranking carries 25 ms and up to 500 rows. Reopen trigger: if the agent's p99 measured inside the fraud service exceeds 6 ms at the month-5 load test.

### 3. Who folds streaming tiles, and the streaming freshness number

Lead A: serving folds 64 tiles per feature at read, Flink writes only tiles, and asked Serving to confirm the budget. Lead B: fetch measured at 4.1 ms p99 with 3 ms for on-demand, 5 s p99 stream freshness. Lead D: 2 s p99 for the card and merchant velocity views, 5 s for the rest. Lead A also set p95 30 s, p99 60 s because the watermark delay alone is 30 s.

**Decision:** Flink folds. Serving does no tile arithmetic; the online field for a streaming view is the running window value, updated per event and debounced to one write per entity per second (DE 06), and the tiles exist in RocksDB and in the offline tile log for the training fold and the nightly overwrite. The deciding number is not CPU but bytes: 300 streaming features times 64 tiles is 19,200 values per entity, which cannot fit a 6 KB user budget, and the 10 ms path should carry no work that a job can do once. The cost A named, 20k to 60k writes per second, is inside DE 06's 70k write budget with the debounce. Freshness follows: because the online value updates per event and the tile closes at the watermark, the online path can be faster than the offline log. 2 s p99 event to online for the card and merchant velocity views, 5 s p99 for every other view a fraud model reads, 60 s p99 for the remaining streaming views; the offline tile log runs 60 s behind and training accepts that. `declared_lag` for bootstrap backfills uses the view's own SLO.

### 4. Requests or rows, and the capacity

DE 20 and DE 06 read the brief's 200k as entity rows per second (30k fraud requests times 3 entities plus 500 ranking requests times 220 rows). DE 18 and lead D read it as requests and sized on 4.5M rows per second and 1.8 GB per second, which assumes 150k fraud requests per second.

**Decision:** rows. The brief says tens of thousands of fraud requests per second and hundreds of candidates per ranking request; DE 06's arithmetic is the only one consistent with both sentences. Capacity: 200k entity rows per second peak with 3x headroom, 24 shards, 615 GB of data, 1.6 TB provisioned with one replica per shard, about $18k a month. D's sizing was 20x over. The month-5 load test still runs fraud-shaped synthetic traffic at 150k requests per second, because 450k rows per second is exactly the headroom we claim and it is cheaper to prove it than to argue it.

### 5. One region or two, and the fraud availability number

DE 12: two regions at full capacity, 99.99 per region. DE 11 and DE 14: one region, 99.95, a second region doubles the Redis bill and the on-call load. Leads B, C and D all landed on one region with three AZs.

**Decision:** one region, three AZs, primary and replica in different AZs, AZ-local reads. Fraud SLO 99.95 percent monthly measured in the SDK with degraded responses counting as success. The cell replicates inputs, not state, so a second region is a copy: it costs about $25k a month, buys the step to 99.99, and is built the month the fraud team signs and funds that number. Until then the drill is a cold rebuild from S3 plus Kafka replay in under 4 h, quarterly, with tier-1 views first.

### 6. Redis versus DynamoDB, and whether there is ever a second online store

DE 07, 10 and 15 assumed DynamoDB for some or all views; leads B, C and D all rejected it for the 10 ms path (p99 5 to 10 ms is the whole budget, the 100-key batch cap, $94k a month for full nightly pushes). Lead C would permit DynamoDB for long-tail views; lead B proposed a ScyllaDB tier when Redis passes 5 TB or $50k and asked whether Platform would run it.

**Decision:** Redis or Valkey cluster mode is the only online store in version one; no DynamoDB for features, no ScyllaDB tier. The release valve is the byte budget, not a second store: the registry refuses `online=true` on a view that would push an entity type past its budget (user 6 KB, item 4 KB, device 1 KB), the 60-day no-reader auto-unflag removes bytes nobody reads, and half the 5,000 features stay offline-only. A second store means two writers, two TTL semantics and two consistency checks for one view, which is the skew we are here to remove. If the 5 TB trigger fires anyway, that is a chair decision at that time with the bill attached.

### 7. Where defaults live, including for on-demand transforms

DE 12 wanted a per-feature `on_missing` policy in the registry; DE 15 wanted None plus status and defaults in the model; lead C put `default` and `required` in the model-owned, versioned FeatureService, applied by the SDK. Lead D wanted defaults in the definition, not the caller. Lead C asked whether on-demand transforms need a registry-level rule for None inputs.

**Decision:** lead C's contract, which also satisfies D once you notice the FeatureService is a registered, versioned definition. The wire carries None plus a status (OK, STALE, MISSING, DISABLED, ERROR) and `available_at` per view; the offline join returns null in the same cases; the FeatureService declares `default` and `required` per feature; the SDK fills defaults only when asked and counts every fill; the server sets `degraded` when a required feature is not OK. Defaults apply to MISSING, DISABLED and ERROR, never to STALE. For on-demand transforms the rule is in the DSL, not in a second place: null propagates through every library function by default, and a transform that wants otherwise writes `coalesce` in its expression, so the same AST gives the same answer in Spark, Flink and the agent. Nothing in the caller's code decides a value.

### 8. Registry ownership of layout version, max_staleness and TTL

Lead D: the registry must own the row layout version and a mandatory `max_staleness`, or the fixed-layout row and the near cache are correctness bugs. Lead A: TTL and the as-of rule are one registry field read by serving and the join engine. Lead C: `freshness_sla` and `max_staleness`, STALE past the first, MISSING past 3x the second.

**Decision:** the registry owns three mandatory fields per view, and the names are settled here so four areas stop using five words. `ttl`: how long an online row lives and the lookback the join applies; batch views 14 days with a weekly full rewrite that refreshes it, streaming views the window length. `max_staleness`: the freshness bound; a value older than it is STALE, older than 3x is MISSING, and the agent may cache a view only when it is 60 s or more, with cache TTL equal to it. `layout_version`: the fixed-layout row schema for the view version, in the 4 B header. Plus `owner`, `tier` and `oncall_channel`, enforced in CI.

### 9. Tier-1 failure semantics and the skew auto-block

DE 05 and lead A: quarantine and serve the registered default with a `stale` flag, or return an error so the model declines; and whether 3 days over the skew threshold blocks training sets. Lead B: a breach below 99.9 percent blocks new versions of the view. Lead C: fail-open at the store, fail-closed only through `required` in the FeatureService, authz fail-closed always.

**Decision:** two axes, two answers, as lead C put it. Authorisation is fail-closed without exception: 403 for the whole request. Data is fail-open at the store: 200 with statuses, and a quarantined feature arrives as DISABLED with the verdict from the snapshot within 5 s. The only fail-closed data path is `required` in the model's FeatureService, because the feature owner does not know the loss function of twelve models. Fraud's written agreement is open with defaults, with `required` on KYC-tier attributes. Skew: a view below 99.9 percent match (0.1 percent mismatched entities, exact for ints and categoricals, 1e-6 for floats) blocks new versions of that view the same day; three consecutive days block new training sets on it, with a signed override recorded in the manifest. Views read by fraud models are held at 99.99 percent with exact equality and shared-library-only functions.

### 10. Year-one money, chargeback timing and the served-log budget

Lead D: platform pays coexistence (about 30k USD a month) and backfills through month 12, chargeback from month 13, cost tags from day one. Lead C: readers pay by share of key reads, producers pay offline, requesters pay training sets, all from meters, platform subsidises only the registry and monitoring. Lead B: the served log at about 650 GB a day with a 1 percent full-value sample, and asked whether the five highest-value models justify 12 TB a day.

**Decision:** C's mechanics on D's calendar. Meters and cost tags run from day one and every team sees its bill monthly from month 1; money moves from month 13. The year-one platform budget is 100 to 120k USD a month including the 30k of coexistence; the steady state after migration is 60 to 80k. The served log stays at keys, versions and per-view timestamps for everyone, full values for the 1 percent sample, for services with a `required` feature (roughly the fraud services, 1.6 TB a day, under $1.2k a month for 30 days) and for any model's first 30 days; 12 TB a day of full values is rejected because reconstruction from the logged `available_at` is exact by construction. Training-set cost is shown before a run and blocked above the team threshold.

### 11. Archiving Kafka topics for streaming backfill

Lead A: any topic a registered feature reads must be archived to Iceberg from the day the feature registers, or the feature cannot be backfilled and is rejected; DE 03's figure for every topic is 2 TB a day, 1.1 PB over 3 years, about 22k USD a month, more than the store's own storage.

**Decision:** archive on registration, not everything. The registry rejects a streaming view whose topic is not archived, and registering the view turns archiving on for that topic from that day; year one is 12 topics for 40 views. The archive is the lake's line, owned by Platform, and the 1.1 PB is the ceiling if every topic ends up read, not the day-one bill. Backfill over 3 years comes only from the archive, never from Kafka retention.

### 12. Spark plus Flink for a team of 8

Lead A: Flink is the right engine for per-partition watermarks, RocksDB state with TTL and 30 s checkpoints, and asked whether 8 people can run 40 team-defined Flink jobs. Lead D's trip-wire: fewer than 4 infrastructure engineers means flip to Tecton.

**Decision:** yes, with two conditions. Teams do not write Flink: the compiler emits one Flink job per streaming view from the same tile spec that emits the Spark job, so there is one job template to operate, not 40 programs. Platform is paged for job health; the view owner is paged for data verdicts, and tier-1 registration requires a PagerDuty schedule. If the platform drops below 4 infrastructure engineers the Tecton trip-wire fires as D wrote it.

### 13. Migration order

Lead D: churn first, then recs and search, fraud in months 7 to 10, because a public failure on the first migration ends the platform; but the 10 ms path is built in phase 2 and load-tested with fraud-shaped traffic from month 5, with fraud shadow reads from month 6. DE 18 and 20 designed from fraud outward.

**Decision:** D's order for cutover, engineering from fraud outward. Phase 0 (weeks 1 to 8) ships the registry, batch materialisation, the point-in-time join and wrap-a-table; churn is live by month 4; recs and search, the agent, the fixed-layout row and the store writer by month 7, with the load test at 150k requests per second under 6.5 ms p99 from month 5; fraud shadow-reads from month 6 and cuts over behind a flag between months 7 and 10 after 30 days of green parity; the deploy gate turns on at month 9 once 10 teams are live. Kill criterion: fewer than 5 teams with a model reading from the store at month 6 stops building and embeds every engineer with a team.

### 14. Right to erasure and models trained before the request

Lead C: out of scope for the store; provide the lineage query "which models trained on sets containing this entity" and record the position in the DPIA; a retraining trigger is an order of magnitude more cost and a legal decision.

**Decision:** out of scope for version one, exactly as C framed it, and I will not let the platform team decide it by default. The store guarantees a tombstone table anti-joined by both materialisers and the training-set builder, tested in CI with a seeded tombstone and a backfill; online delete within 24 h; erasure complete in 30 days with a page at 25; pinned training sets rewritten. Lineage gives legal the list of affected models on demand. Whether to retrain them is legal's decision with the bill attached, recorded in the DPIA.

## Non-negotiables, final

**Definitions and computation**

1. The registry is the only source any job, serving path or training set executes from, resolved by name and version, and it is never on the online read path.
2. A feature name is immutable in meaning, enforced by a semantic hash; a breaking change is a new name with `supersedes`.
3. Every batch definition is a pure function of source snapshots and `as_of_date`; the compiler rejects non-deterministic functions and `now()`.
4. One mergeable tile and partial layer is the only aggregation primitive; raw is read once per night per source and no window rescans raw.
5. One definition compiles to Spark, Flink and the request-time executor, with a daily parity diff at 0.1 percent.
6. Every offline row carries `event_ts`, `available_at` and `created_ts`; rows are append-only and corrections are new rows.
7. Every materialised partition and every training set has a manifest pinning source snapshots, definition versions and code hash.
8. Nothing writes to the offline or online store except through the validator; a blocked batch is never readable.

**Serving and consistency**

9. One writer per feature view; the online view field is replaced whole, never patched.
10. The read API returns a status and `available_at` per view; the store never substitutes a value.
11. The online store is rebuildable from the offline store plus Kafka in under 4 h, tested quarterly.
12. Every point-in-time join uses `available_at` and `created_ts <= R`; a value serving never had is invisible to training, and bootstrap backfills are flagged in the manifest.
13. Every on-demand transform is a registered, content-hashed definition executed by the platform in serving and training, under a hard per-request cap.
14. The served log captures keys, view versions and per-view timestamps for every model from day one.
15. Entities are registered objects with a surrogate key; composite views on two large entities are never materialised without an approved bound.

**Platform**

16. A production model reads through an immutable, versioned FeatureService that declares `default` or `required` per feature; training and serving take the same object.
17. Serving has no runtime dependency on the registry, offline store or Spark; a control plane down for 24 h changes nothing served.
18. Every online row has a TTL and every online view has a live reader; no reader, no bytes in Redis.
19. Every view registers with `owner`, `tier`, `ttl`, `max_staleness`, `layout_version` and `oncall_channel`, enforced in CI, and carries a metered monthly cost line.
20. SLOs are measured in the SDK; the fraud SDK has a hard 8 ms deadline with a fallback agreed in writing.
21. Authorisation is in-process from the snapshot over mTLS service identities, fail-closed; sensitivity inherits in CI with untagged columns treated as PII.
22. A tombstone table that every write path anti-joins, tested in CI with a seeded tombstone and a backfill.

**Adoption and evolution**

23. The wrap-a-table path ships in phase 0; without zero-rewrite onboarding the migration does not start.
24. The weekly metric is production models reading from the store; feature counts are never reported as progress.
25. No pipeline, feature version or embedding version is removed without serving lineage showing zero consumers and the stated grace.
26. No synchronous network hop other than the store on the fraud path; no SDK reads Redis directly.
27. One throttled store writer for batch and streaming with a p99 feedback loop; no job writes to Redis directly.
28. Every embedding column carries its encoder version; a request without a pinned version is refused, and item vectors are shipped as snapshots, never fetched per candidate.

## What got cut

- **A second region.** $25k a month and a second on-call load for a step from 99.95 to 99.99 that fraud has not signed. The cell is a copy when they do.
- **DynamoDB, ScyllaDB or any second online store.** Two writers and two TTL semantics for one view; the byte budget is the release valve.
- **Serving-side tile folding.** 19,200 values per entity does not fit any byte budget; Flink folds.
- **Restricted Python in the serving path.** 8 people cannot own a compiler and an embedded interpreter; the SQL-subset DSL runs in three engines and Python survives only as a content-hashed heavy sidecar, 3 per model, 2 ms.
- **Daily offline snapshots.** 20x the storage of a change log with monthly anchors and still needs `available_at`.
- **Archiving every Kafka topic on day one.** Archive on registration; the 1.1 PB is a ceiling.
- **Full-value served logging for everyone.** 12 TB a day for nothing reconstruction cannot give; 1 percent plus fraud plus new models.
- **Composite keys online.** Collapse onto the large-side entity as a bounded map; a true composite only when one side is under 1,000.
- **Per-feature scheduling and per-feature defaults in the registry.** Views of 5 to 30 features; defaults in the FeatureService.
- **Forced deprecation of a production model.** Training refuses after 30 days; serving never stops; the monthly bill is the pressure.
- **Retraining models on erasure.** A legal decision with the lineage query provided.
- **Streaming encoders, a fourth embedding family, sub-second streaming updates, and 1-minute tiles everywhere.** Each is a v2 item with a named trigger.

## The numbers the design stands on

| Figure | Value | Source |
|---|---|---|
| Feature views | about 400: 350 batch, 40 streaming over 12 topics, 10 on-demand; 5 to 30 features each | Lead A from DE 01, 02, 03 |
| Online store | 615 GB data, 1.6 TB provisioned, 24 shards with one replica, about $18k a month | DE 06 |
| Online byte budget per entity | user 6 KB, item 4 KB, device 1 KB; 64 views per entity max, 4 KB per view | DE 06, lead B |
| Read rate | 200k entity rows per second peak (30k fraud x 3 entities, 500 ranking x 220 rows), 3x headroom | DE 06, DE 20 |
| Write rate | 20k to 60k streaming writes per second after a 1 s debounce; nightly diff about 600M rows in 35 min | DE 03, DE 06 |
| Offline store | about 550 TB over 3 years (change log with monthly anchors, 5-min tiles 90 days, hourly 1 year, daily 3 years), $12k to $14k a month | DE 04, DE 03, DE 07 |
| Raw archive | up to 1.1 PB, about $22k a month, a lake cost | DE 02, DE 03 |
| Nightly batch compute | 1,650 core-hours, $2k to $4k a month | DE 02 |
| Streaming compute | 32 task managers, about 150 GB RocksDB state, about $12k a month | DE 03 |
| Training set, 50M rows x 300 features over 2 years | 16 min, about $8; $8k to $10k a month for 1,000 | DE 04, DE 07 |
| Backfill of one view over 3 years | 10k core-hours, about $400, 3 h; approval above 200 core-hours | DE 02 |
| Fraud latency budget | 10 ms p99 end to end; fetch 1.2 ms p50 and 6.5 ms p99 through the agent including on-demand; 8 ms client deadline, hedge at 3 ms | DE 18, DE 15 |
| Ranking latency budget | 25 ms p99 at 500 candidates | DE 06, DE 08 |
| Streaming freshness | 2 s p99 card and merchant velocity, 5 s p99 other fraud-read views, 60 s p99 the rest | DE 18, DE 06, DE 03 |
| Batch freshness | online by 06:00 D+1; `available_at` within 5 h of midnight | DE 02, DE 14 |
| Skew SLO | 0.1 percent mismatched entities per view per day; 0.01 percent for fraud views; exact ints and categoricals, 1e-6 floats | DE 05, DE 08 |
| Served log | about 150 B per record, 650 GB a day; 90 days full, then 10 percent plus fraud-labelled for 3 years | DE 08 |
| Availability | 99.95 percent fraud tier, 99.9 percent others, SDK-measured; second region $25k a month for 99.99 | DE 14, DE 12 |
| Monthly total | about 75k USD steady state for store, compute, serving and quality; 100 to 120k in year one including 30k coexistence; 60 to 80k after migration | DE 11, DE 16 |
