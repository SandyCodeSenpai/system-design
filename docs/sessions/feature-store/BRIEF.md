# Brief: design a feature store

Shared brief for every engineer on the panel. State assumptions instead of asking questions.

## The situation

- A company with about 200 data scientists and ML engineers across 30 teams, 60 models in production, and a platform team of 8 that will own this. Existing infrastructure: a cloud data lake (Parquet on object storage with a table format), Spark for batch, Kafka for events, Kubernetes for services, a relational metadata database, and a managed key-value store. Assume one major cloud; name it if a decision depends on it.
- Workloads that must be served: fraud scoring at request time (p99 under 10 ms for the feature fetch, tens of thousands of requests per second), recommendation and search ranking (hundreds of candidate entities per request), churn and lifetime-value models scored in nightly batches over 100M users, and training set generation for all of the above.
- Scale: 100M users, 20M items, 5,000 registered features growing 20 percent a year, 2B raw events a day, 200k online feature lookups per second at peak, 1,000 training-set generations a month over up to 3 years of history.
- The reason it exists: today every team computes its own features in its own pipelines, training and serving disagree silently, backfills take weeks, and nobody can answer "which model uses this column". The feature store must make one definition produce the same value in training and serving, make point-in-time correct training data cheap, and make features discoverable and reusable across teams.
- Time box: you have 45 minutes, as in an interview. Requirements 5, estimates 5, high-level design 10, deep dive on your lens 20, trade-offs 5.

## What every design must answer

1. Requirements, functional and non-functional, with numbers.
2. Estimates: storage online and offline, write and read rates, computation cost, freshness, and monthly cost order of magnitude.
3. High-level design of the whole system, one diagram, and the main flows: define a feature, materialise batch, materialise streaming, serve online, generate a training set, monitor.
4. A deep dive on your lens: the hard part, the obvious approach, why it breaks, what you would do instead.
5. Trade-offs, pitfalls, and three to five open questions for the panel.
6. Non-negotiables: the two or three things you would block the design on if they were missing.

## Writing rules

- Plain, direct, numbers over adjectives. Tables for anything comparative. Roughly 180 to 240 lines.
- 2 or 3 mermaid diagrams. Mermaid rules: every label containing parentheses, commas, colons, slashes or quotes goes in double quotes, e.g. `S[("Online store (Redis)")]`; never use `end` as a node id; edge labels with punctuation use `-->|"label"|`; node ids alphanumeric; sequenceDiagram participants declared as `participant X as Name`; flowchart subgraphs as `subgraph id [Title]`; at most 14 nodes per diagram.
- Section headings, in order: `## Requirements`, `## Estimates`, `## High-level design`, `## Deep dive: <your lens>`, `## Trade-offs`, `## Pitfalls`, `## Open questions for the panel`, `## Non-negotiables`.
