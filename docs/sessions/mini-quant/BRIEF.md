# Brief: a mini quant trading system on a Mac mini with $2,000

Shared brief for every engineer on the panel. State assumptions instead of asking questions.

## The situation

- One person, technically strong, operating alone in their spare time. No team, no on-call rotation.
- Hardware: one Mac mini (Apple silicon, 16 GB RAM, 512 GB SSD) on home broadband, always on. No cloud budget beyond a few dollars a month.
- Capital: $2,000 total. That is the entire trading account. Losing it is survivable but the point is not to.
- Jurisdiction: assume a US retail account unless a lens says otherwise; call out where rules differ.
- Goal: a safe, honest, automated system that trades a small number of instruments on a daily or intraday timeframe, can be trusted to run unattended for a day, and lets the operator learn whether a strategy has an edge before it costs real money. Not high-frequency, not a hedge fund.
- Time box: you have 45 minutes, as in an interview. Requirements 5, estimates 5, high-level design 10, deep dive on your lens 20, trade-offs 5.

## What every design must answer

1. Requirements, functional and non-functional, with numbers.
2. Estimates: data volumes, request rates, costs per month, expected trade counts, and the honest expected edge versus fees.
3. High-level design of the whole system, one diagram, and the main flows: data in, signal, order out, reconcile, report.
4. A deep dive on your lens: the hard part, the obvious approach, why it breaks, what you would do instead.
5. Trade-offs, pitfalls, and three to five open questions for the panel.
6. Non-negotiables: the two or three things you would block the design on if they were missing.

## Writing rules

- Plain, direct, numbers over adjectives. Tables for anything comparative. Roughly 180 to 240 lines.
- 2 or 3 mermaid diagrams. Mermaid rules: every label containing parentheses, commas, colons, slashes or quotes goes in double quotes, e.g. `B[("Broker (Alpaca)")]`; never use `end` as a node id; edge labels with punctuation use `-->|"label"|`; node ids alphanumeric; sequenceDiagram participants declared as `participant X as Name`; flowchart subgraphs as `subgraph id [Title]`; at most 14 nodes per diagram.
- Section headings, in order: `## Requirements`, `## Estimates`, `## High-level design`, `## Deep dive: <your lens>`, `## Trade-offs`, `## Pitfalls`, `## Open questions for the panel`, `## Non-negotiables`.
