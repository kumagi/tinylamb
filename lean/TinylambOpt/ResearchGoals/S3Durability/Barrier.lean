/-!
The reply barrier of `docs/primary-s3.md`: when a SQL reply may leave the
server. A commit is visible (MVCC publish) before its WAL is durable. Each
published commit first raises the published-modification LSN of every object it
changed (a table heap, an index tree, a catalog tree). A statement waits until
the durable LSN reaches the largest such LSN over the objects it read. These
goals state that this target covers every visible commit on those objects, so a
reply that observed a row's absence through an index still waits for the
deleting commit, and that the target never waits for WAL that no published
commit wrote. `tlaplus/ReplyBarrier.tla` checks the same rule against the
existing per-version rule.
-/

namespace TinylambOpt.ResearchGoals.S3Durability.Barrier

/-- A published commit: the end LSN of its commit record and the objects it
modified. -/
structure Commit where
  lsn : Nat
  objs : List Nat
  deriving DecidableEq, Repr

/-- Published-modification LSN of object `o` after the commits in `raised`. -/
def modLsn (raised : List Commit) (o : Nat) : Nat :=
  raised.foldl (fun m c => max m (if o ∈ c.objs then c.lsn else 0)) 0

/-- The reply target: the largest modification LSN over the objects read. -/
def target (raised : List Commit) (read : List Nat) : Nat :=
  read.foldl (fun m o => max m (modLsn raised o)) 0

/-- The highest commit LSN among the raised commits. -/
def maxLsn (raised : List Commit) : Nat :=
  raised.foldl (fun m c => max m c.lsn) 0

namespace ModLsnComplete
/-- Raise before visible: every commit the snapshot sees has raised its LSN.
Then the target covers each visible commit that changed an object read. -/
def Goal : Prop :=
  ∀ (raised vis : List Commit) (read : List Nat), (∀ c ∈ vis, c ∈ raised) →
    ∀ c ∈ vis, ∀ o ∈ read, o ∈ c.objs → c.lsn ≤ target raised read
end ModLsnComplete

namespace ReplyRevealsOnlyDurable
/-- With a durable prefix at least the target, every visible commit on an object
the reply depends on is durable when the reply leaves. -/
def Goal : Prop :=
  ∀ (raised vis : List Commit) (read : List Nat) (durable : Nat),
    (∀ c ∈ vis, c ∈ raised) → target raised read ≤ durable →
    ∀ c ∈ vis, ∀ o ∈ read, o ∈ c.objs → c.lsn ≤ durable
end ReplyRevealsOnlyDurable

namespace TargetWithinPublished
/-- The target never exceeds the highest raised commit LSN: unlike waiting for
the buffered WAL end, a read never waits for records of unfinished
transactions or of commits published after the snapshot. -/
def Goal : Prop := ∀ raised read, target raised read ≤ maxLsn raised
end TargetWithinPublished

end TinylambOpt.ResearchGoals.S3Durability.Barrier
