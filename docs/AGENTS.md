# `docs/` — normative specs and decision records

Read before designing, cite when changing behavior. Most binding first:

- `lock_order.md` — normative lock ordering (storage + txn work must obey).
- `recovery_invariants.md`, `wal_format.md` — ARIES invariants + on-disk WAL
  layout (`kWalRecordVersion = 3`).
- `page_format.md`, `page_checksum.md`, `pax_page_format.md`,
  `value_storage.md` — on-disk formats.
- `expression_evaluation.md` — AST/Bytecode/JIT equivalence contract.
- `cascades_optimizer.md` (+ `optimizer_todo.md`,
  `optimizer_improvements.md`) — optimizer design + open work.
- `commit_durability.md`, `lock_timeout.md` — commit/wait semantics.
- `adr-proposal.md` + `review/` (`*_review.md`) — decision records. (There is
  no `docs/adr/` directory; create `NNN-<topic>.md` there only if establishing
  a standing ADR location.)
- `next-actions.md` — improvement backlog. `todo.md`, `executor_todo.md`,
  `bug_audit_2026-08.md` — triage lists, check before claiming "known issue".
- `jit_profile.md` — JIT measurements. `distributed*.md` (+
  `distributed.md` at root) — raft/membership/migration designs.
- `design.md` — broad design notes (D3/D4/D9 cover storage/txn/WAL).

Rule: a behavior change that contradicts a normative doc (`lock_order`,
`recovery_invariants`, `wal_format`, `expression_evaluation`) must update the
doc in the same change.
