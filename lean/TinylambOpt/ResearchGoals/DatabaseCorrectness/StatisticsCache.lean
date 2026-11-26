import Std

/-! Thread-local statistics lookup and context-local table-name invalidation.
Owner stands for a live catalog identity, not a schema name. The oracle supplies
the statistics visible at a snapshot and epoch. Names are already normalized
in the thread cache; a separate model covers invalidating case aliases. -/
namespace TinylambOpt.ResearchGoals.DatabaseCorrectness.StatisticsCache

structure View where
  owner : Nat
  epoch : Nat
  snapshot : Nat
  deriving DecidableEq, Repr
abbrev Stats := Option Nat
structure Cache where
  view : View
  entries : List (Nat × Nat)
  deriving DecidableEq, Repr

def lookup (oracle : View → Nat → Stats) (cache : Cache) (view : View) (key : Nat) : Stats × Cache :=
  let entries := if cache.view == view then cache.entries else []
  match entries.lookup key with
  | some hit => (some hit,⟨view,entries⟩)
  | none => match oracle view key with
    | none => (none,⟨view,entries⟩)
    | some v => (some v,⟨view,(key,v)::entries⟩)
def faithful (oracle : View → Nat → Stats) (c : Cache) :=
  ∀ k v, c.entries.lookup k = some v → oracle c.view k = some v

/-- All cache returns agree with a fresh catalog lookup while preserving consistency for the next reader. -/
def ThreadStatisticsRefinesCatalog.Goal : Prop := ∀ oracle c view key, faithful oracle c →
  let (v,next) := lookup oracle c view key
  v = oracle view key ∧ faithful oracle next

/-- Readers pinned to different snapshots can share the same live catalog epoch without sharing statistics values. -/
def SameLiveEpochDifferentSnapshot.Goal : Prop := ∀ oracle c view key,
  view.owner = c.view.owner → view.epoch = c.view.epoch → view.snapshot ≠ c.view.snapshot →
  (lookup oracle c view key).1 = oracle view key

/-- A failed catalog fetch must not convert a later success into a cached absence on the same thread. -/
def CatalogLookupFailureCanRetry.Goal : Prop := ∀ before after c view key value,
  (lookup before c view key).1 = none → after view key = some value →
  (lookup after (lookup before c view key).2 view key).1 = some value

abbrev Names := List (String × Nat)
def purge (xs : Names) (n : String) := xs.filter (fun (k,_) => k.toLower != n.toLower)

/-- All ASCII case aliases of a DDL target are invalidated, including duplicates in a context cache. -/
def CaseAliasInvalidationComplete.Goal : Prop := ∀ xs n alias, n.toLower = alias.toLower →
  (purge xs n).lookup alias = none

/-- Invalidating a renamed table under both old and new names preserves unrelated aliases and their first visible bindings. -/
def RenameInvalidationPreservesOtherTables.Goal : Prop := ∀ xs old fresh k,
  k.toLower ≠ old.toLower → k.toLower ≠ fresh.toLower →
  (purge (purge xs old) fresh).lookup k = xs.lookup k

/-- Epoch invalidation after aborted DDL drops a schema-dependent result without requiring the old snapshot to advance. -/
def AbortDdlEpochRevalidatesSnapshot.Goal : Prop := ∀ c oracle k,
  let after := {c.view with epoch := c.view.epoch+1}
  (lookup oracle c after k).1 = oracle after k

end TinylambOpt.ResearchGoals.DatabaseCorrectness.StatisticsCache
