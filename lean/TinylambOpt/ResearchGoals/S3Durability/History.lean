/-!
Models of the S3 WAL history in `docs/primary-s3.md`. One epoch's keys
`wal/<epoch>/<seq>` form a create-only store (PutObject with
`If-None-Match: *`). A restore reads the chain of chunks from sequence 1 while
each chunk starts where the previous one ended, and stops at a free key, a seal
marker or a broken link. These goals state, for any number of records and
chunks, the facts the TLA+ model `tlaplus/S3Wal.tla` checks for small bounds:
the history a restore reads only grows, a writer's contiguous acknowledged
prefix is part of it, a seal fixes it, and acknowledging past a free key is
unsafe. They let the SQL server acknowledge COMMIT from the S3 durable LSN.
-/

namespace TinylambOpt.ResearchGoals.S3Durability.History

/-- A WAL chunk object: first and last record position, and the records. -/
structure Chunk where
  lo : Nat
  hi : Nat
  recs : List Nat
  deriving DecidableEq, Repr

/-- An object under `wal/<epoch>/<seq>`: a chunk or the epoch's seal (terminal marker). -/
inductive Obj where
  | chunk (c : Chunk)
  | terminal
  deriving DecidableEq, Repr

/-- One epoch's keys: sequence number to the stored object, if any. -/
abbrev Store := Nat → Option Obj

/-- `PutObject(If-None-Match: *)`: store `o` under `k` only if the key is free. -/
def create (st : Store) (k : Nat) (o : Obj) : Store :=
  fun j => if j = k ∧ st j = none then some o else st j

/-- The restore walk from sequence `s`, expecting the next record at `lo`.
`fuel` bounds the number of keys read. -/
def walk : Nat → Store → Nat → Nat → List Nat
  | 0, _, _, _ => []
  | fuel + 1, st, s, lo =>
    match st s with
    | some (.chunk c) => if c.lo = lo then c.recs ++ walk fuel st (s + 1) (c.hi + 1) else []
    | _ => []

/-- `cs` is a chain of chunks whose first chunk starts at `lo`. -/
def Chained : List Chunk → Nat → Prop
  | [], _ => True
  | c :: cs, lo => c.lo = lo ∧ Chained cs (c.hi + 1)

/-- `cs` is stored at consecutive keys starting at `s`. -/
def Placed (st : Store) : Nat → List Chunk → Prop
  | _, [] => True
  | s, c :: cs => st s = some (.chunk c) ∧ Placed st (s + 1) cs

namespace WalkMonotone
/-- Any conditional create (a late chunk of a dead writer, a hedged retry, a seal)
only extends what a restore reads, so the official history is append-only and a
standby may apply any prefix it has read. -/
def Goal : Prop :=
  ∀ fuel st k o s lo, walk fuel st s lo <+: walk fuel (create st k o) s lo
end WalkMonotone

namespace AckedPrefixOfficial
/-- When every chunk of a writer's contiguous acknowledged prefix is stored, its
records are a prefix of what a restore reads, whatever else lands later. -/
def Goal : Prop :=
  ∀ cs st s lo fuel, Chained cs lo → Placed st s cs → cs.length ≤ fuel →
    (cs.flatMap Chunk.recs) <+: walk fuel st s lo
end AckedPrefixOfficial

namespace SealFinal
/-- A seal stored right after a chain fixes the epoch: no later create, at any
key, changes what a restore reads. A zombie writer can no longer extend it. -/
def Goal : Prop :=
  ∀ cs st s lo fuel k o, Chained cs lo → Placed st s cs →
    st (s + cs.length) = some .terminal →
    walk fuel (create st k o) s lo = walk fuel st s lo
end SealFinal

namespace NonContiguousAck
/-- Claim (false): a stored chunk's records belong to the history even when an
earlier key of the epoch is still free. Acknowledging on any 200 is unsafe. -/
def Goal : Prop :=
  ∀ (st : Store) (c : Chunk) (fuel x : Nat), st 1 = some (.chunk c) → x ∈ c.recs →
    x ∈ walk fuel st 0 0
end NonContiguousAck

end TinylambOpt.ResearchGoals.S3Durability.History
