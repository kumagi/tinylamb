# Recovery

The current storage engine uses a logical WAL, committed-state fuzzy snapshots,
and two-pass commit-filtered restart recovery.

- `Logger` appends CRC-protected version-7 `LogRecord`s to numbered segments.
  Buffered, written and durable LSNs are distinct; only a successful durability
  wait permits a synchronous commit acknowledgement.
- `CheckpointManager` captures a replay anchor bounded by the oldest active
  transaction, waits for WAL durability, writes the committed row/LSM snapshot,
  and recycles eligible segments. Background work is requested by compaction;
  it does not emit periodic checkpoint WAL records, pin a buffer pool, persist
  a DPT/ATT or publish a `.last_checkpoint` sidecar.
- `RecoveryManager` loads the snapshot and validates the WAL tail. Its first
  pass discovers `kCommit` owners; its second pass replays their records in LSN
  order. Aborted and unfinished transactions are skipped, without restart undo.
- Runtime abort reverses `staged_records_` in memory and appends `kAbort` when
  needed. It does not append a commit terminator or a per-operation CLR chain.
  `kCompensateUpdate` remains for the physical row repair path.

Normative details: [recovery invariants](../docs/recovery_invariants.md),
[WAL format](../docs/wal_format.md), and
[commit durability](../docs/commit_durability.md).
