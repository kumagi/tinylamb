# `docs/` — normative specs and decision records

Read before designing, cite when changing behavior. Most binding first:

- `lock_order.md` — normative lock ordering (storage + txn + OOC reclaim/spill work must obey).
- `recovery_invariants.md`, `wal_format.md` — committed-snapshot recovery invariants +
  on-disk WAL layout (`LogRecord::kCurrentVersion = 7`).
- `page_format.md`, `page_checksum.md`, `value_storage.md` — on-disk formats (`<db>.db` snapshot v4, `.blocks.<gen>`, `.lsm.<gen>`, TLCS v2).
- `expression_evaluation.md` — AST/Bytecode/JIT equivalence contract.
- `cascades_optimizer.md`, `optimizer_improvements.md`, `optimizer_rule_audit.md` —
  optimizer design, rule registry, and historical work.
- `commit_durability.md`, `lock_timeout.md` — commit/wait/statement-timeout semantics.
- `environment_variables.md` — every `TINYLAMB_*` env var the code reads
  (A/B hatches, OOC budgets, fault-injection probes, fuzzer knobs).
- `adr/` — accepted ADRs (`0001` WAL, `0003`–`0012` Out-of-Core) and experimental
  proposals (`0002`).
- `executor/` — physical execution engine manual (`00-index.md`..`90-data-modification.md`).
- `opt/` — Cascades optimizer guide (`00-index.md` + `part1-foundations/` + `part2-rules/`).
- `jit_profile.md` — JIT measurements. `distributed*.md` (+
  `distributed.md` at root) — unimplemented distributed Raft/membership/migration design archive (`review/` holds its reviews).
- `design.md` — historical baseline design notes (D1–D11; storage/txn/WAL/optimizer).
- Root planning/backlog baselines: `../TODO.md` (pipeline execution & roadmap) and `../ooca-v2.md` (Out-of-Core architecture & phases P0–P6b).

Rule: a behavior change that contradicts a normative doc (`lock_order`,
`recovery_invariants`, `wal_format`, `expression_evaluation`) must update the
doc in the same change.
