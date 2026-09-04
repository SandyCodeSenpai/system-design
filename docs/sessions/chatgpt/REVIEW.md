# ChatGPT design panel — review

Chair's record of the panel discussion. Three engineers designed ChatGPT independently in 45 minutes each, from the same brief: "we can run the model on one GPU box; we cannot serve it to a billion users." This file reconciles the three write-ups into one design, which is published at `content/designs/chatgpt.md`.

## Panel

- **Distinguished Engineer** (`distinguished.md`): the GPU fleet and the inference engine. Derives tokens per second per GPU from HBM bandwidth, sizes the fleet and the cost per token, and designs continuous batching, paged KV, prefill/decode disaggregation, a tiered KV store with cache-aware routing, request classes with a shed order, and fleet operations (cells, P2P weight rollout, stragglers, several hardware generations).
- **Senior Staff Engineer** (`sr-staff.md`): the request path and the data around it. Gateway and limits in weighted tokens, SSE streaming decoupled from the GPU through a stream store, admission control with weighted fair queuing, a telemetry-driven router with the case against least-connections, the conversation store and prompt assembly, the usage ledger as billing truth, safety classifiers, abuse, and multi-region.
- **Engineer** (`engineer.md`): the product flows and the evolution from one box to a fleet. The nine things that break in order and what fixes each, the client-server contract (events with a sequence number, regenerate and edit as siblings, stop, idempotent retry, resume), the message tree, the write path for a streamed reply, worker health and draining, rollouts as new pools, and what to monitor.

## Where the three designs agree

The three converged on a spine without talking to each other.

- **The GPU is a batch machine and the batch is measured in tokens.** All three say decode is memory-bandwidth bound: one step reads the weights once for every sequence in the batch, so one sequence uses a few percent of the GPU and continuous batching with a paged KV cache is the 10× to 50× that everything else stands on. All three admit by KV blocks, not by request count, because one 50k-token conversation costs a step what sixteen normal ones cost.
- **The follow-up turn must land where its prefix lives.** Each turn re-sends the whole history. Distinguished, Sr Staff and Engineer each independently make routing prefix-aware and each puts the saving at a quarter to a third of the prefill fleet. All three call out the same failure: a byte changing near the top of the system prompt (a date, a reordered tool schema) silently drops the hit rate to zero.
- **The socket is not the generation.** All three put a stream store (Redis Streams keyed by the response id, entries with a monotonic sequence number) between the worker and the client, and all three build resume as "reconnect to any gateway with the last sequence seen". This one decision gives resume, stop, second-device viewing and gateway deploys without dropping streams.
- **SSE over HTTP/2, WebSocket only for voice.** Unanimous, for the same reasons: one direction is all a response needs, every proxy passes it, and `Last-Event-ID` is the resume cursor.
- **Placeholder row on start, checkpoint every 5 seconds, final write once.** All three reject writing every token to the database (40M writes/s) and reject writing only at the end (a crash loses the answer and a reload mid-stream shows nothing).
- **A conversation is a tree.** Regenerate and edit create siblings under the same parent; nothing is mutated in place; the visible transcript is the path to a stored current leaf.
- **The fleet is fixed on any timescale a request cares about.** GPUs arrive in quarterly tranches and weights take minutes to load, so admission control is the availability story: explicit classes, a fixed shed order, and free-tier degradation to a small model before anyone sees an error.
- **Money is the one place correctness beats latency.** Sr Staff and Engineer both make the engine's usage record the billing truth, delivered at least once and upserted by request id, the same pattern as the Spotify royalty ledger.
- **Data lives in the user's home region; GPUs live where the power is.** All three accept one backbone round trip per turn rather than a GPU region per user region.

## Disagreements and resolutions

### 1. Baseline numbers and the size of the fleet

The estimate tables agree on demand and disagree by 4× on supply. Demand: 300M DAU, 3B consumer messages a day, ~400 output tokens per message, ~2,000 to 3,000 tokens of context, and 40M output tokens per second at peak; all three arrive at that last number. Supply: Distinguished sizes ~50k H100-class GPUs; Engineer ~160k; Sr Staff ~200k. The spread comes from two inputs. KV bytes per token: Distinguished uses FP8 KV on a model with 8 KV heads (160 KB per token); Engineer uses BF16 (320 KB); Sr Staff assumes 300 KB. And per-GPU decode throughput: Distinguished derives ~2,600 tokens/s per GPU from HBM bandwidth at a batch of 2,000 and plans on 1,500 sustained; Engineer and Sr Staff assume a node holds 80 to 100 streams at 20 to 40 tokens/s, which is ~300 tokens/s per GPU, a batch-of-80 number.

**Resolution:** Distinguished's capacity model is the panel's, because it is derived rather than assumed and because the batch size it assumes is exactly what the KV tiering and admission design exist to sustain. 300M DAU, 3B consumer messages plus 1B API requests a day, 46k requests/s average and ~80k/s at the global peak after cross-region spill flattens the curve, 40M output tokens/s peak, a 65% prefix-cache hit rate (Sr Staff's number, between Distinguished's 75% and Engineer's 60%, and the one the fleet is planned against with an alarm at 40%), ~27k decode plus ~10k prefill GPUs, ~50k H100-class with headroom, ~$2.4M a day, ~$0.0005 per consumer message. Engineer's 20k-node figure is kept in the published design as the "what a strong generalist would estimate" and the gap is explained: the difference between a batch of 80 and a batch of 2,000 is the whole point of the serving design.

### 2. Prefill and decode: one pool or two

Distinguished disaggregates: prefill units (TP-8 in a node) write KV blocks and hand them to 32-GPU decode units over RDMA, so a long document never stalls 2,000 streams. Engineer co-locates with chunked prefill interleaved into the decode loop. Sr Staff asked which way the fleet was going because it changes what affinity attaches to.

**Resolution:** disaggregated for the flagship model, chunked prefill retained inside the prefill pool for very long prompts, co-located for the small model where step time is short anyway. The deciding argument is Distinguished's: decode step time is the per-user tokens per second, and a co-located 30k-token prefill freezes every stream on the unit for hundreds of milliseconds. The ~10 ms KV handoff is inside the TTFT budget. Affinity attaches to the cell (see 3), not to a replica, so Sr Staff's concern dissolves.

### 3. Where the KV prefix lives between turns, and how routing finds it

Engineer: the prefix lives in the worker's HBM; route by hashing the conversation id to three candidate workers and pick the least loaded, falling back to the pool. Sr Staff: the prefix lives on the replica that served the last turn; route there if its estimated TTFT is within 1.5× of the pool's best, else power-of-two choices by estimated TTFT. Distinguished: HBM holds only in-flight work; a finished conversation's blocks demote to host RAM (~10 min) and NVMe (hours); the global router hashes the conversation id to a cell, a cell-wide radix index knows which unit holds which blocks, and a busy holder's blocks are pulled over RDMA (~10 ms) rather than routing to it.

**Resolution:** Distinguished's tiers and two-level routing, with Sr Staff's tolerance rule inside the cell. The global router hashes conversation id to a cell with consistent hashing. The cell scheduler places the request on the unit that holds the longest prefix if its estimated TTFT is within 1.5× of the cell's best, otherwise on the best unit, which pulls the blocks from the holder's host RAM over RDMA. Engineer's three-candidate hash is dropped because the cell-wide index is exact where a hash is a guess. The arithmetic that settles it is Distinguished's: recomputing a 3,000-token prefix costs 0.375 GPU-seconds and ~50 ms; reloading 480 MB costs ~10 ms from host RAM and a hundredth of the money. Keeping every conversation warm for 5 minutes is ~12 PB, which is the fleet's host RAM, so the RAM tier holds 5 to 10 minutes with eviction biased toward paid and active conversations. The NVMe tier is kept for the long-context cell and residency-pinned enterprise sessions only; a consumer conversation resumed after 10 minutes recomputes, pending the think-time distribution Distinguished asked for.

### 4. The queue in front of the fleet and who waits

Engineer: in-memory per-model per-tier queues in the router, bounded by wait time, no Kafka; 4:1 weighted fair between paid and free plus 30% of capacity reserved for paid; queued event with position after ~1 s; free offered the small model; hard fail at 20 s paid, 60 s free. Sr Staff: per-region per-pool schedulers, in-memory queues mirrored to Redis; weighted fair queuing 50/30/15/5 across P0 to P3 with per-user deficit round-robin; position shown after 5 s; free degraded to the small model at 60 s expected wait; reject at a 5-minute estimate; API gets a 429 within 10 s. Distinguished: per-class priority queues in the cell scheduler with a fixed shed order (pause batch, route easy free requests to the small model, queue free, cap free output, 429 API above quota, queue paid last), and preemption of free and batch sequences by swapping their KV to host RAM when a paid request needs the slot.

**Resolution:** in-memory queues bounded by wait time (Engineer and Sr Staff agree; the durable record is the stored user message and the client's idempotent retry). Weighted fair queuing between classes with per-user deficit round-robin inside a class (Sr Staff), so free never starves and one user cannot hog a class. Distinguished's shed order as the overload policy, each step a config value with an alert, and his preemption of free and batch by KV swap because it turns "a paid user waits" into "a free user pauses". What the user sees: a queue position after 5 s, free routed to the small model when the expected wait passes 60 s, API 429 within 10 s, a static at-capacity page past a 5-minute estimate. Engineer's 20-second hard fail for paid is rejected: a paid user who has waited 20 s would rather see a position than an error.

### 5. Small-model routing: always, or only under overload

Distinguished routes ~40% of free-tier messages to the small model at all times through a difficulty classifier, and calls it the largest post-foundation cost lever (~$0.5M a day). Sr Staff and Engineer degrade free traffic to the small model only under overload. Sr Staff's open question 2 and Engineer's open question 4 are the product side of this: a silent downgrade reads as "the model got worse", and a hard cap on free concurrency would give the free users who get in the good model every time.

**Resolution:** the difficulty classifier runs on free-tier traffic at all times, behind an eval gate owned by the same quality loop that gates a new checkpoint, and the threshold moves with load. It is the only lever that scales with demand and the model quality gap is measured, not assumed. The hard cap is rejected because it turns a request that a small model could answer well into a "come back later". The product shows which model answered, so the downgrade is visible, which resolves Sr Staff's concern.

### 6. A worker dies under a running answer

Engineer: transparent retry only if no tokens have been shown, otherwise an honest "interrupted" with a regenerate offer, because a restarted generation produces different text and splicing it under text the user has read is worse. Sr Staff: transparent retry under 50 emitted tokens. Distinguished: the retry carries the same request id and the tokens already streamed become part of the prefix, so the generation continues rather than restarts and the user sees a stall of one to two seconds.

**Resolution:** Distinguished's continuation, which is what Engineer's open question 5 asked for. The prompt plus the emitted tokens is a valid prefix; the continuation is not byte-identical to what the dead unit would have produced, but nothing the user has read is replaced, which was Engineer's actual objection. The checkpoint every 5 s and the stream store hold the emitted tokens, so the continuation needs no new machinery. Under 50 tokens a plain restart is acceptable too.

### 7. Rate limits and metering

Sr Staff: limits in weighted tokens (`uncached × 1 + cached × 0.1 + output × 4`), sliding window for consumer tiers because the product promise is windowed with a countdown, token buckets per org and model for the API, reserve at admission with the median output and settle from the engine's usage record, 10-minute reservation expiry. Engineer: token buckets per user and model plus a daily output-token budget, consumption counted from the usage event through Kafka a few seconds late. Distinguished did not design limits.

**Resolution:** Sr Staff's model. Engineer's daily output budget is the same idea with a coarser window; the reserve-then-settle bounds overshoot to one response instead of concurrency × responses, and the settle through the usage record is what Engineer's Kafka path does with a tighter loop. The usage ledger upserted by request id with a 0.01% reconciliation gate on invoices is unanimous where it was designed.

### 8. Conversation store: wide-column from day one, or Postgres first

Engineer would start on Postgres and migrate to wide-column at 10 to 20M DAU, designing single-partition access from the start so the move is mechanical. Sr Staff and Distinguished choose wide-column from the start for native TTL, multi-DC replication and 1.5 PB a year without resharding.

**Resolution:** wide-column in the published design, because the design is for the stated scale and the access patterns (one partition per conversation, one per user for the sidebar, no cross-partition transactions) are what a wide-column store is for. Engineer's point stands as advice for a team that is actually at one box: the schema is the same either way. The message tree is Engineer's and Sr Staff's, which are the same schema: messages partitioned by conversation id with a parent id, a sidebar partition by user id clustered by updated time, a stored current leaf.

### 9. Stream store capacity

Engineer asked whether Redis Streams survive 1M concurrent generations at 40M appends per second, or whether a purpose-built relay tier is needed. Sr Staff batches tokens into a chunk every 100 ms or 10 tokens, which brings the rate to a few million appends per second on a sharded cluster of ~200 nodes and ~200 GB of memory.

**Resolution:** Redis Streams with 100 ms chunking. Engineer's 40M/s was a per-token number; per-chunk it is ~8M/s at peak, which a sharded cluster handles, and the 100 ms batching is invisible at 40 tokens/s. A purpose-built relay is a later optimisation, not a design change, because the contract (sequence number per event, replay from a cursor, 10-minute retention) does not depend on what implements it.

## Open questions, answered

### Distinguished

**1. KV retention economics.** Host RAM holds 5 to 10 minutes, biased toward paid and active conversations; NVMe only for the long-context cell and residency-pinned enterprise sessions; everything else recomputes (disagreement 3). Revisit when the think-time distribution is measured; the metric that decides it is the share of follow-up turns that arrive between 10 minutes and 2 hours, since that is the only band NVMe would serve.

**2. Small-model routing ownership.** The classifier ships behind the same eval gate as a checkpoint, owned by the quality loop, with the threshold as a config value the capacity controller moves (disagreement 5). The regression that turns it off is a drop in the free-tier thumbs-up rate on messages the classifier routed, measured against a 1% holdout that always gets the flagship.

**3. Capacity follows power versus latency.** Accepted. The conversation store is read once per turn in the gateway's region before the prompt crosses the backbone; the GPU region persists nothing and tokens return to the home region's stream store. One backbone round trip per turn, ~50 to 150 ms, inside a 1-second TTFT budget. Residency-pinned tenants are the exception and get an at-capacity response instead of spill.

**4. Free-tier overload policy.** Visible queue with a position, then the small model with a visible note (disagreements 4 and 5). Uniform slowdown is rejected because it spends paid SLOs to hide a free-tier queue.

**5. Prefill:decode ratio and thinking mode.** The fleet manager rebalances units between pools (a decode unit becomes a prefill unit by restarting the engine in a different parallel configuration, not by downloading weights). The next tranche is biased toward memory bandwidth because every product signal (thinking mode, longer answers) moves demand toward decode, and prefill is the pool the prefix cache keeps shrinking.

### Sr Staff

**1. Disaggregated prefill and decode.** Yes (disagreement 2). Affinity attaches to the cell, the cell-wide radix index knows where blocks are, and the TTFT estimate includes the ~10 ms transfer. The hit rate survives because the prefix lives in host RAM regardless of which unit runs the next turn.

**2. Automatic degradation versus visible waiting.** Both: position first, then the small model with a visible note (disagreement 5).

**3. Zero-retention contracts and resume.** A per-org relay mode that holds chunks only in gateway memory and gives up resume and second-device attach. The contract language says so. This is a config on the org, not a second stream store.

**4. Org-scoped access to transcripts.** A nightly export to the org's own store, keyed by conversation id so deletions tombstone. The product store is partitioned for the product; an eDiscovery index is a different system with different access control and should not share a partition key with the chat path.

**5. How real is the prefix-cache assumption.** Planned at 65% with the tiered store carrying most of the history hits and the pinned system prompt carrying ~40 points regardless of routing (disagreement 1). Under overload the RDMA pull keeps the hit rate up even when placement is load-driven; the alarm is at 40% per cell, and the prefill pool is sized with 30% headroom against exactly that.

### Engineer

**1. Redis Streams at 1M concurrent generations.** Yes, with 100 ms chunking (disagreement 9).

**2. Affinity versus a distributed KV cache.** The tiered store with a cell-wide index (disagreement 3). Affinity alone stops being enough the moment a unit is full, which at peak is always; the RDMA pull is what lets the scheduler balance load without giving up the prefix.

**3. Wide-column from day one.** For the published design, yes (disagreement 8). For a team at one box, Postgres with single-partition access patterns, as the write-up says.

**4. Hard cap on free concurrency.** No (disagreement 5).

**5. Continue from the checkpointed partial.** Yes; that is the panel's resolution for unit failure (disagreement 6).

## What got cut for 45 minutes

In an interview none of the three would have covered everything above. What the panel would accept as out of scope, and what it would ask as follow-ups:

- **Cut: the per-GPU arithmetic.** Say "decode is memory-bandwidth bound, so batch until HBM is full; that is a batch of about 2,000 on a 32-GPU unit" and move on. Follow-up: why an MoE looks 25× cheaper at batch 1 than at batch 2,000.
- **Cut: the KV tier table.** Say "finished conversations demote to host RAM for ten minutes and the next turn is routed to the cell that has them". Follow-up: reload versus recompute, with the 100× cost argument.
- **Cut: fleet operations.** Cells, P2P weight distribution, stragglers and hardware generations are a 5-minute answer to "how do you roll out a new model without dropping streams".
- **Cut: limits internals.** Say "limits are in weighted tokens, reserved at admission and settled from the engine's count". Follow-up: why not count requests.
- **Cut: memory, search, titles, shared links, export.** One sentence each; they are ordinary async jobs off a message event stream.
- **Cut: multi-region.** Data in the home region, GPUs where the power is, one backbone hop per turn. Follow-up: what a GPU region outage looks like (waits and downgrades, not errors).
- **Would not cut:** continuous batching and the token-budget admission, the stream store with a sequence number and resume from a cursor, prefix-aware routing and why the system prompt must be byte-stable, and the shed order under overload. Those four are where the interviewer learns whether the candidate has served a model, held a million sockets, and decided who waits.
