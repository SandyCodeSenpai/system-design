# Mini quant system — design panel, chair's record

Brief: one operator, one Mac mini, $2,000, safety first, not high-frequency. Twenty Distinguished Engineers designed the whole system through one lens each; four area leads reconciled five each; this record settles what the leads escalated and fixes the design that goes on the site.

## Panel

| Area | Lead's verdict |
|---|---|
| A, data and research (01, 03, 12, 13, 14) | Data costs $0, daily bars are the only trading timeframe, one pure `target_weights` function is shared byte-for-byte by backtest, paper and live, and no backtest runs without a register row. The product is knowledge of whether an edge exists, not the $20 to $120 a year it might earn. |
| B, execution and risk (02, 04, 05, 06, 07) | Cash account and cash semantics in software, the strategy emits weights and never touches the broker, every order carries a deterministic client order id persisted before the HTTP call, reconcile before anything, and no halt ever auto-flattens. Expected edge $0 to $100 a year against a $2,000 downside from one bug; every limit follows from that asymmetry. |
| C, platform (08, 09, 10, 15, 16) | launchd is the only supervisor, a stateless 60-second tick reads a `runs` table, SQLite in WAL with `synchronous=FULL` is the only truth, keys live in the Keychain of a non-admin user, three healthchecks.io checks watch the box, and a halt is a row cleared only by a human over Tailscale SSH. |
| D, operations and evaluation (11, 17, 18, 19, 20) | One code path with stage as a column; downward moves (halt, demote, retire) are automatic and upward moves need the evaluator to pass a pre-registered gate and the operator to confirm. Live proves plumbing and cost; the backtest carries the edge hypothesis, because one live year has a Sharpe standard error of 1.0. |

Engineers by number and lens:

| # | Lens | # | Lens |
|---|---|---|---|
| 01 | Market data ingestion | 11 | Promotion from backtest to live |
| 02 | Broker and execution | 12 | Strategy families |
| 03 | Backtesting | 13 | ML skeptic, research discipline |
| 04 | Risk | 14 | Data quality and corporate actions |
| 05 | Portfolio construction | 15 | Security |
| 06 | Regulatory | 16 | Scheduling and time |
| 07 | Costs | 17 | Software architecture |
| 08 | Mac mini infrastructure | 18 | Crypto |
| 09 | Reliability | 19 | Performance statistics |
| 10 | Observability | 20 | Operations and the human |

## The shape of the answer

Twenty engineers with twenty lenses converged on one small system: a Python process of a few thousand lines, one SQLite file, launchd, a cash account at Alpaca, eleven liquid ETFs, one decision a day, and a monthly momentum strategy with two parameters that will trade fifteen to twenty-five times a year. Every area independently reached the same asymmetry, an expected edge of $0 to $100 a year against a $2,000 loss from one bug, and every area independently concluded that the deliverable is an honest answer to "is there an edge", not the money. Nobody proposed a queue, a container, a cloud, a paid feed, machine learning on the signal, or intraday trading, and the ones who considered those ruled them out with arithmetic rather than taste.

Two things surprised the chair. First, the loudest cross-area disagreement was not the strategy or the risk limits but the clock: Area A designed a signal at 18:00 with a fill at the next open, Areas B and D a single run at 15:45, and Area C an order window at 09:45. Three plausible schedules for one daily decision, each with a different fill convention in the backtest. Second, how many carefully argued components were removed by another area's arithmetic: 04's bracket stops by 05's fractional orders, 01's Parquet layout by the panel's own cut of minute history to 45 MB, and 11's twenty-fill paper gate, which would have retired the reference strategy for trading too rarely to be measured. The design below is what survived.

## Cross-area decisions

### 1. Daily only, no intraday path, and bar size capped at one minute

Positions: A, B and D each designed intraday out. B's arithmetic: cash-account good-faith violations or the PDT rule in margin make round trips inside a day structurally impossible at $2,000, and 07's table needs 10.5 to 18 percent gross a year before either rule is counted. C's tick supports one-minute bars over REST and asks whether anything finer is wanted. 12's second candidate, RSI(2) on SPY, signals at the close.

**Decision:** Daily is the only trading timeframe in version one and the system has no intraday order path. Minute bars are stored for two years for eleven symbols and used only to measure realised slippage; nothing finer than a minute is ever stored, so the tick and REST polling stand and no websocket daemon exists. An intraday strategy is a different system (a loop, intraday reconcile, no one-push-a-day rule) and is a version-two decision, not a version-one option.

### 2. One decision window at 15:45 ET, and what the backtest fills at

Positions: A schedules the signal at 18:00 from the final close and the order at 09:31 the next session, and its backtest fills at the next open. B runs once at 15:45, submits by 15:50 and cancels at 15:58, because the overnight gap between a close signal and a 09:35 fill is about 0.7 percent on SPY, ten times the round-trip cost. D agrees on 15:45. C's gantt drew a 09:45 window but deferred the choice to B. A's non-negotiable is that the strategy never sees today's partial bar.

**Decision:** One run at 15:45 ET, submissions by 15:50, every non-terminal order cancelled at 15:58, half days shifted to fifteen minutes before the close from the broker calendar. The strategy sees completed sessions only, so at 15:45 its input is bars through yesterday's close, validated the night before; the backtest computes the signal from `close[t-1]` and fills at `close[t]` minus the cost model. That is the same one-bar lag as A's design with a fifteen-minute gap in place of an overnight one, proceeds from 15:45 sells are settled for tomorrow's buys, and the reference strategy's spec changes only its clock: signal from bars through the prior close, executed at 15:45 on the last session of the month. Ingest stays at 16:35 and 20:05 the evening before.

### 3. Cash account, and cash semantics in software regardless

Positions: 02 wanted the Alpaca default margin account with a local day-trade counter; 12 wanted margin so a sell does not block a buy for a day; 04, 06 and 07 wanted cash; 05 said the type barely matters at the FINRA 4210 floor and the software must run cash semantics either way; 06's decisive point is that in margin the exit can be the illegal leg and Alpaca's PDT check will reject the sell that protects capital. D's ladder assumes cash and its canary stage tests settled-cash rejects.

**Decision:** Cash account at the broker, long only, and cash semantics in the portfolio layer regardless: a settled-cash ledger (cash minus proceeds settling after today, cross-checked against the broker's `cash`, lower wins), no same-day round trip per symbol, one run per day, with a test that fails if any of the three is removed. 02's day-trade counter survives as ten lines that halt entries if `daytrade_count` is ever above 0. The engine models T+1 settlement in backtest so the fill rules are identical in all three modes. Margin is revisited only above $25,000 or if a strategy needs shorts.

### 4. No broker-side stops: engineer 04's non-negotiable is overruled

Positions: 04 makes a bracket stop at 2 x ATR on every entry a non-negotiable because the Mac dying must not be a risk event. 05 makes fractional orders a non-negotiable and Alpaca brackets need whole shares. 02 bans GTC because an instruction nobody is watching is the outage failure mode. 07 notes whole shares turn a $400 slot into a 25 to 100 percent sizing error. Lead B overruled 04 and asked the chair to confirm. Lead C's halt semantics assumed positions "stay under broker stops".

**Decision:** Confirmed, no broker-side stops, and the arithmetic is recorded so the decision is not reopened after the first bad overnight gap. A DAY bracket's stop leg expires at 16:00, so protecting an overnight position needs GTC, which the panel bans. Whole-share brackets make SPY at $560 unholdable at a $400 slot, which defeats the portfolio. The book is long-only liquid ETFs at a 0.40 weight cap with no leverage, so a 5 percent overnight gap on the largest position is 2 percent of NAV, inside the daily limit, and 04's own pitfall notes a gap fills through a stop anyway. What replaces the stop: the strategy's exit at the next 15:45 run, the equity check against the daily limit on every 60-second tick during market hours, a dead-man page within 6 minutes of the Mac going silent, and Halt and Flatten from the phone plus the broker's own app, which works with the Mac unplugged. C's halt semantics are unchanged in effect; the phrase "under broker stops" is struck from them.

### 5. The risk gate is a separate process under a separate macOS user

Positions: 04 wants two processes under two users, keys only in the executor's Keychain, the strategy writing intents to a directory the executor reads. 02 and 17 have one process. C designed one LaunchAgent under `trader` and asked whether it must pay the launchd and Keychain cost of two. B resolved for two.

**Decision:** Two users. The `strategy` user runs its own 60-second LaunchAgent whose whole job is: if today is a session, the clock is between 15:35 and 15:45 ET and `intents/<date>.json` does not exist, read bars, call `target_weights` for each strategy row and write the file. It has no Keychain items and no broker calls. The `trader` user runs the tick that owns SQLite, the gate, the executor, the reconciler, the evaluator and the report, and its login Keychain holds the five secrets. On a machine where one person edits the strategy weekly with no reviewer, "the strategy cannot reach the broker" must be a permission, not a convention; the cost is one extra plist and an hour once. The evaluator's nightly shadow replay imports the strategy function under `trader`, which is fine: the boundary is keys, not code.

### 6. Halt semantics: entries only, sticky, never auto-flatten, and what the daily loss cap does

Positions: 02, 05, 09, 10, 15, 16 and 20 all halt new orders and never auto-flatten; 04 and 08 flatten on a hard halt or any heartbeat failure; 06 wants exits never gated; C says only the daily loss cap flattens; B says the daily 3 percent cap halts entries with automatic re-arm and no flatten; D says the daily cap is 2 percent, sticky, cleared only by a typed human reason with no auto-resume path in the code; 14 asks whether a loss-cap flatten bypasses a global data RED. On the phone: C allows Halt as the dashboard's only action, B wants cancel-all plus close-all from the phone, D wants three buttons.

**Decision:** Two states, both rows in SQLite that survive restart. `HALT_ENTRIES`: no buys; sells that reduce or close pass; every open order is cancelled so reconcile sees a closed book; P1 page; sticky, cleared only by `quant ack <id> --reason` over Tailscale SSH. Triggers: daily loss of 3 percent of start-of-day equity ($60), weekly 5 percent rolling five sessions ($100), reconcile mismatch, global data RED or stale bars, clock more than 30 seconds off the broker, turnover cap, realised cost above 1.5x modeled over 20 trades, `daytrade_count` above 0, five unacknowledged daily pushes, or the operator. `FLATTEN`: cancel-all then close-all with limit-then-market at 15:59, triggered only by the operator or by the 10 percent drawdown budget at the next 15:45 run, after which the strategy sits in paper for 20 sessions. Three rulings inside that: B's 3 percent number wins over D's 2 percent because 2 percent fires on the ordinary daily range of a 95 percent long ETF book; D's sticky resume wins over B's automatic re-arm because one rule for every halt means no auto-resume code path and a 3-percent-a-day week is exactly when a human should look; a reconcile mismatch sends nothing at all, not even exits, because computed quantities are untrusted. The drawdown flatten prices off the broker's live quote, so it does bypass a data RED. The phone dashboard gets two actions, Halt and Flatten, both downward; resume exists only at a shell.

### 7. SQLite only; Parquet is dropped

Positions: A resolved on Parquet for bars written by whole-partition atomic replace, plus a SQLite `meta.db` for row-shaped tables, read through DuckDB; the argument was a 30M-row scan under a second and eight years of minute bars. C resolved on one SQLite file for everything, with DuckDB reading it directly, and Parquet only if sub-minute quotes are ever stored. D wants raw broker responses recorded for replay tests and asks Platform to redact account ids and encrypt the off-box copy.

**Decision:** One SQLite file, `quant.sqlite`, WAL, `synchronous=FULL`. The number that justified Parquet changed under A's own reconciliation: the panel cut minute history to two years for eleven symbols, 2.2M rows and 45 MB, and daily bars for thirty symbols over twenty-five years are 8 MB. One file means the nightly `.backup` is the whole system and the restore drill is one copy. 01's idempotency survives as SQL: the nightly ingest rewrites the window it fetched (last five sessions of daily bars, the current month of minute bars) by delete-then-insert in one transaction, so any rerun and any vendor correction lands the same way. 13's and 14's append-only triggers, the adjustment factors materialised nightly to 1.0 today, the two views `bars_1d_tr` and `bars_1d_split`, and the snapshot hash all move into that file unchanged. Raw broker responses are stored with headers stripped and account ids replaced by a stable hash before insert, so the backup needs no encryption beyond B2's own. Parquet returns the day someone stores sub-minute quotes, and nobody has.

### 8. A launchd tick with a runs table, not a daemon and not two fires a day

Positions: 08, 09, 10 and 15 want one long-running process with `KeepAlive`; 16 wants a stateless 60-second tick with every step a row in `runs(trading_day, step)` and a hard deadline on the order step; A schedules three calendar fires (16:35, 18:00, 20:05); D wants one script fired twice (15:45 trade, 16:15 evaluate) with nothing between.

**Decision:** The tick. Every failure the panel listed (sleep, reboot, crash, hung loop, a coalesced late fire, a March hour that does not exist) reduces to "the next tick runs and reads the table", and a step past its deadline is skipped and alerted rather than replayed, so a late wake never places an order. D's two-fire model has nothing running between 15:58 and 16:15 and nothing checking the daily loss limit during the day; B's 60-second equity check and C's per-tick reconcile both need something awake, and the tick is the cheapest thing that is. A's three evening jobs become `runs` rows (16:35 ingest and verify, 20:05 ingest and verify) with `StartCalendarInterval` gone from the design and `TZ=UTC` on the process. D's persisted order state machine survives as the sub-states of the order step. Nine steps a day, one table, one plist per user.

### 9. After five unacknowledged days: hold, do not flatten

Positions: 20 asks whether five unacknowledged pushes should pause-and-hold or flatten; D resolves hold with the loss rules still armed; B has no timer-based flatten anywhere; C does no time-based flatten.

**Decision:** Hold. Five unacknowledged daily pushes set `HALT_ENTRIES`, and the positions stay. The daily and weekly halts and the 10 percent drawdown budget remain armed and will flatten on their own if the book earns it. A timer flatten turns every holiday week into a forced exit at 2 to 4 bps for no information; a held book of long-only ETFs under a 10 percent budget is the designed safe state.

### 10. The reconcile mismatch threshold is $5 per instrument, and Area B owns it

Positions: 17 and 19 halt at $0.01 or one share on totals; 20 halts at $5 or one share per instrument and logs smaller diffs for Saturday; B's own text says $1 per position or $5 cash in one place and "alert above $50" in another; D proposed $5 per instrument as the default and asked B to own it.

**Decision:** Compared per instrument, never on totals. A position that differs by more than $5 at the broker's mark or cash that differs by more than $5 halts entries and sends nothing; below that, the broker's number overwrites the ledger, the delta is logged and listed in Saturday's report. Dividend accrual, cash interest and fractional rounding produce sub-dollar diffs weekly, and halting on them trains the operator to type resume without reading. B owns the number because it interacts with settled cash and fractional handling; B's disposition table stands with the two thresholds unified at $5.

### 11. Paper and live run in parallel, in one process and one file

Positions: 10 asks whether paper and live run concurrently; C prices it at a second SQLite file, ntfy topic and healthchecks set; 02 wants live keys only in the executor's Keychain and paper keys in separate config; D's ladder has stage as a column with up to four strategy rows, two at canary or live, routed by one multiplier function; 12 wants one strategy in paper at a time.

**Decision:** Yes, D's way. Stage is a column on the strategy row; the same tick routes each row's intents to the log (shadow), the paper endpoint (paper), or the live endpoint at 0.1 or 1.0 size (canary, live). One SQLite file, one process, both key pairs as separate Keychain items under `trader`, `LIVE_ARMED` gating the live endpoint, paper alerts at P3 only. At most four rows total, at most two at canary or live each owning its symbols, and one row in paper at a time. Each row keeps a virtual book and reconcile compares the sum of books to the broker per instrument. No second file and no second topic.

### 12. Research runs on the laptop, and the research record's offsite copy is git

Positions: 15 wants research on the laptop or at worst the admin account, never `trader`; C wants one user and one venv on the box; A needs the Mac mini's always-on data, a sealed holdout file, and an append-only research log that survives disk loss with an offsite copy owned by Platform under $1 a month.

**Decision:** Research runs on the laptop against a copy of `quant.sqlite` pulled over Tailscale, read by DuckDB. The engine runs 252k bars in 0.8 seconds and a 500-config sweep in under a minute, so nothing about backtesting needs the box. `research.sqlite` lives on the laptop with the register, the append-only `research_log` and the sealed holdout; the artefacts a promotion needs (neighbourhood grid, deflated Sharpe, holdout row, replay diff count, the written economic reason) are committed to the repository alongside the strategy row, so the promotion gate reads git and git on a remote is the offsite copy of the research record. B2 holds the trading file. The Mac mini has one working user for trading, one for the strategy, and no notebooks.

### 13. Idle cash: no platform sweep into a T-bill ETF

Positions: 07 asks whether idle cash should sit in SGOV or BIL, worth about $80 a year, roughly the expected edge; B flagged that it touches the settled-cash gate and the strategy universe; A's reference strategy already routes every unfilled slot to BIL by weight; 19 says use the account's actual sweep rate as the cash benchmark and BIL's return if it is zero.

**Decision:** No sweep. The reference strategy holds BIL as a target weight, so the $80 question dissolves into the strategy's own universe: unselected slots are in BIL by construction, not by a platform rule. The 5 percent reserve ($100) stays as settled cash because it is the fee and rounding buffer and an automatic sweep would put a sell in front of every buy against the settled-cash rule; at 4 percent that reserve forgoes $4 a year. A strategy that wants a different cash sleeve declares it in its universe and is reviewed as a strategy.

### 14. Promotion gates: A's backtest bar, D's ladder, with the paper stage re-timed for a monthly strategy

Positions: A's backtest gate is pooled out-of-sample Sharpe above 0.3 and positive in 4 of 5 folds, out-of-sample to in-sample ratio above 0.5, survives 10 bps, max drawdown under 20 percent, 50 out-of-sample fills, grid median positive, holdout Sharpe positive; D's is deflated out-of-sample Sharpe at least 0.8, 100 closed trades, MDD at most 15 percent, at most 5 parameters. D's paper gate is 30 days and 20 fills with a 90-day cap that retires anything slower; A wants at least 60 paper days spanning 3 signal days; 02 wants 20 sessions with zero UNKNOWN residues and zero reconcile deltas; 07 wants 60 days of fills against the model.

**Decision:** A's backtest gate, because A computed the noise ceiling on this data (Sharpe 0.87 at 20 variants over 8 years) and a 0.8 deflated bar would reject every honest strategy the panel can name; the deflated Sharpe and bootstrap interval are printed on every report and are not gates. Parameters are capped at 3, not 5. D's ladder stands with the paper stage re-timed: shadow is 10 days at 100 percent replay match; paper is 60 trading days spanning at least 3 signal days with 100 percent signal match, zero UNKNOWN residues and zero unexplained reconcile breaks; canary is 20 trading days at 10 percent size spanning at least 2 signal days with reject rate at most 2 percent and the daily cap never hit. The slippage rule (median at most 5 bps live against the model, p95 at most 20 bps) is evaluated as a rolling window over the first 30 live fills wherever they land, because 20 fills is a year for the reference strategy and paper fills are optimistic anyway. The 90-day cap becomes "90 paper days without 3 signal days retires". Upward moves need the evaluator to mark the gate passed and the operator to press confirm; the operator can delay but never accelerate or edit a gate.

## Non-negotiables, final

**Data and research**

1. Raw unadjusted bars are the only prices on disk; adjustment factors are rebuilt nightly to 1.0 today in a separate table and applied only in read-time views (01, 14).
2. Every ingest is idempotent: the fetched window is rewritten in one transaction, and any job can be rerun any number of times (01).
3. A validator runs after every ingest and writes a GREEN, AMBER or RED row per symbol per session; the gate refuses position-opening orders on anything but GREEN, and exits are never blocked by data status (01, 14).
4. All timestamps are UTC, session dates come from the exchange calendar and never from truncating a timestamp, and the first-bar-at-open assertion runs every session (01, 14, 16).
5. One engine loop serves backtest, paper and live; the strategy is a pure `target_weights(bars, held)` function that may import only pandas and numpy; the 20-day replay produces zero order diffs (03, 12, 17).
6. Fills are at the session close after the 15:45 decision with 5 bps per side on by default, and every result is rerun at 10 and 20 bps (03, 07).
7. Every run has a record with data snapshot hash, git commit, full parameters, seed and sweep size, and an identical rerun reproduces it bit for bit (03, 13, 14).
8. No backtest runs without a research-log row created first; the log is append-only with triggers rejecting UPDATE and DELETE; at most 20 logged runs per idea (13).
9. The 24-month holdout is sealed in a separate file and read once per idea by a function that refuses the second call (13).
10. A deflated Sharpe and a bootstrap interval are printed wherever a Sharpe is printed; a raw Sharpe alone is blocked from any report (13, 19).
11. At most 3 free parameters per strategy, each a published convention, and the neighbourhood grid is recorded before the strategy enters paper (12).

**Execution and risk**

12. `client_order_id` is deterministic and persisted with `synchronous=FULL` before the HTTP call; no submit is retried without a lookup by that id first (02, 09, 17).
13. Every tick reconciles positions, open orders and cash against the broker before any intent; a mismatch above $5 per instrument cancels open orders, halts entries, sends nothing and pages; no reconcile action ever creates an order (02, 09, 20).
14. DAY time in force on every order, no GTC, no market orders except the flatten's fallback; nothing outlives the session (02).
15. The strategy process has no broker credentials and emits weights, not quantities; the executor under a separate macOS user holds the keys and the gate (04, 15).
16. Halt state and every limit live in SQLite, survive restart, and every change carries a reason row; the only resume is a typed human reason and no auto-resume path exists (04, 05, 20).
17. Cash account, long only, fractional orders; settled cash only, no same-day round trip per symbol, one run per day, with a test that fails if any of the three is removed (05, 06).
18. Exits are never gated except by a reconcile mismatch, in which case nothing is sent (06).
19. Append-only fill ledger keyed on broker fill id, FIFO lots, reconciled nightly, exportable to Form 8949 (06, 19).
20. One cost function shared by backtest and live with per-symbol measured spreads; the gate enforces the 20 bp spread cap and the 1.25x NAV per 20 sessions turnover cap and halts entries when realised cost exceeds 1.5x modeled over 20 trades (07).
21. The pre-deploy test: a strategy that emits 50 intents at weight 1.0 for a symbol off the allow-list produces zero broker calls and 50 reject rows (04).

**Platform**

22. `pmset -a sleep 0 disksleep 0 autorestart 1 womp 1`, auto-login as `trader`, FileVault off, and a UPS with the modem and router on it (08, 16).
23. Three healthchecks.io checks page by 09:30 ET on a missed pre-open reconcile, within 6 minutes of a missed tick, and by 16:45 on a missing report; the ping is the last line of a successful tick (08, 10).
24. SQLite WAL with `synchronous=FULL` is the only truth; a nightly `.backup` goes to B2 and a restore drill is on the calendar quarterly (08, 09).
25. Every run, order, fill and bar row carries `trading_day` and `ts_utc`, both NOT NULL; no step calls `date.today()` and the process runs with `TZ=UTC` (16).
26. The order step has a hard deadline; catch-up after sleep or reboot never places orders (16).
27. Broker keys are trading-only with no transfer rights and exist only in the `trader` Keychain; never in a file, environment block, plist, log line or backup (15).
28. The dashboard binds to 127.0.0.1, is reachable only over Tailscale, and offers Halt and Flatten only; the router forwards nothing and resume needs a shell (15, 20).
29. The trading venv is installed with `uv sync --frozen` from a hash-pinned lockfile under a non-admin user (15).
30. Alerts key on freshness and invariants, name the action, and are deleted when they fire twice with no action (10).

**Operations and evaluation**

31. One code path for backtest, shadow, paper, canary and live, with stage as a column and the size multiplier in exactly one function (11, 17).
32. `core/` imports no network, no clock and no randomness, enforced by an import-linter rule in CI (17).
33. Every strategy passes through shadow with nightly replay against the stored snapshot; any mismatch demotes to shadow (11).
34. Gate, demotion and retirement criteria are written to the strategy row before the stage starts, immutable in stage, and applied by the evaluator; the operator can delay a promotion, never accelerate or edit one (11, 20).
35. The simulated-day fault suite, including crash-after-submit and position mismatch, passes before any config points at the live account (17).
36. Every report number is rebuilt from insert-only tables by one script and carries a standard error or a minimum-sample flag (19).
37. Manual override works with the Mac switched off: closing positions in the broker app produces a halt on the next reconcile, not a correction (20).
38. Deploys are Saturday only, refused by the script on weekdays between 09:25 and 16:05 and whenever orders are open; a `core/` change is a new strategy row at shadow (20, 11).

## What got cut

| Cut | Why |
|---|---|
| Intraday trading, any bar finer than daily for signals | Good-faith violations in cash, PDT in margin, and 10.5 to 18 percent gross a year to break even at daily turnover before either rule is counted (06, 07, B) |
| Crypto, including a sleeve | 0.8 to 1.2 percent per round trip at retail tiers, 3 to 5 percent of equity a month at the cadence PDT would have blocked, 24/7 removes the free daily halt; re-entry needs gross edge over 3x measured cost, BTC and ETH only on Alpaca crypto, and an account over $10,000 (18, D) |
| Options | One contract is 5 to 50 percent of the account and spread is 5 to 10 percent of premium (07) |
| Shorts, pairs, stat-arb | Cash account cannot short and a $2,000 margin account cannot borrow (07) |
| Margin account | $2,000 sits on the FINRA 4210 floor, a $60 drawdown flips it off, and the exit can be the illegal day trade (05, 06) |
| Paid data: Alpaca SIP at $99, Polygon at $29 to $79, Databento except a deferred audit | 17 to 59 percent of the account a year against an edge of $0 to $100; Alpaca's free REST tier returns consolidated SIP bars once 15 minutes old (01, 07) |
| Machine learning on the signal | Daily SNR is 0.03; 1,000 variants on noise yield a Sharpe of 1.2; a rung-2 ridge model burns the same 20-run budget and the deflated Sharpe does the shrinking (13) |
| Broker-side bracket stops | DAY legs die at 16:00, GTC is banned, whole shares break a $400 slot; see decision 4 (02, 05, 07) |
| Single stocks in the traded universe | $400 per slot cannot hold 20 names and no point-in-time constituent list is budgeted; single-stock runs are labelled `survivor_only` and cannot graduate (03, 13, 14) |
| Whole-market daily bars, a 100-symbol universe | Survivorship becomes a real task and no strategy at $400 per slot can use it (01, 03, 13) |
| Parquet and DuckDB as the store, a second `meta.db` | Minute history was cut to 45 MB; one SQLite file is the whole backup (decision 7) |
| Backtrader, vectorbt, Zipline; a vectorised engine mode | The backtest loop must be the live loop; the event-driven engine already sweeps 500 configs in under a minute (03, 12, 13) |
| A long-running daemon, a websocket feed, a separate heartbeat process, a $5 VPS watchdog, a second broker | The tick plus healthchecks.io covers every failure listed, and a second thing that can act on the account is a second thing that can be wrong (08, 09, 16, 02) |
| Docker, supervisord, pm2, APScheduler, Airflow, a queue, uvicorn, Time Machine | launchd, one SQLite file and a stdlib `http.server` do the job; Time Machine copies the WAL mid-write (C) |
| FileVault | Contradicts unattended reboot; Apple silicon SSDs are hardware-encrypted regardless and the mitigation is a trading-only key rotated on theft (08, 15, 16) |
| Bracket-based 1 percent risk-per-trade sizing, the minimum holding period, a December re-entry flag, a 2 percent daily cap | Sizing follows the stop decision; exits are never gated; a flag that changes behaviour one month a year is a bug generator; 2 percent fires on noise (04, 06, B) |
| A netted book across strategies, more than 4 strategy rows, more than 2 live | Doubles reconciliation for a diversification gain $2,000 cannot use (17, D) |
| "Edge confirmed" as a stage or a report line | Sharpe SE after one live year is 1.0; the live stages confirm mechanics only (19, D) |

## Honest expectations

The panel's numbers, not its hopes. A SPY-class round trip at $200 costs about 4 bps in spread and slippage and the panel plans at 10 bps with 20 as the stress case; regulatory fees are under $2 a month. Fixed cost is $3 to $7 a month, about $60 a year, 3 percent of the account. Break-even gross return is 3.6 to 4.2 percent a year at monthly rebalancing, 5.6 to 8.2 percent weekly, and 10.5 to 18 percent daily; the reference strategy's banded daily evaluation lands near the monthly line at roughly 5x annual turnover. Documented retail-replicable premia give 2 to 5 percent a year over buy-and-hold, $40 to $100 gross, so the honest net expectation is $0 to $100 a year and a losing year is ordinary: a zero-edge book at 15 percent vol shows an expected 19 percent drawdown, $375. One live year has a Sharpe standard error of 1.0; rejecting zero at a true Sharpe of 1.0 takes about 4 years, at 0.5 about 15 to 16. The live period proves the plumbing; the backtest carries the edge; the account's job is to make the plumbing's mistakes cheap.
