---
title: Design a Thing
difficulty: Medium            # Easy | Medium | Hard
category: Storage             # reuse an existing category where one fits
summary: One or two sentences on what the system does and the one property that makes it hard.
concepts: [caching, sharding] # slugs of files under content/concepts/
askedAt: [Example Co]         # optional
references:                   # optional
  - label: Original paper
    url: https://example.com
date: 2026-01-01
---

## Requirements

**Functional**

- What the system must do, as bullets.

**Non-functional**

- Latency, availability, consistency, scale. Numbers, not adjectives.

## Estimates

| Quantity | Value | How |
| --- | --- | --- |
| Writes / s | 1,000 | 86M / day ÷ 86,400 |

## High-level design

```mermaid
flowchart LR
  C[Client] --> LB[Load balancer] --> API[API servers]
  API --> DB[(Primary DB)]
```

Walk the diagram: what each box owns, and the request path for the main use case.

## Deep dive: the hard part

The one component the interviewer will push on. Alternatives, and why this one.

## Trade-offs

- What was given up, and what it bought.

## Pitfalls

- The mistake that is easy to make in the room.
