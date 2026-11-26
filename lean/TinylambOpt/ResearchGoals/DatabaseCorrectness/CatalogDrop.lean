import Std

/-! One parent table incarnation and referencing child constraints. The catalog
gate orders reference acquisition with marking the parent as closing; old read
leases can drain after the mark. Handles carry the table incarnation. -/
namespace TinylambOpt.ResearchGoals.DatabaseCorrectness.CatalogDrop

inductive Mode where | openTable | closing | absent
  deriving DecidableEq, Repr
structure Catalog where
  mode : Mode
  generation : Nat
  leases : List Nat
  children : List Nat
  deriving DecidableEq, Repr
def beginDrop (c : Catalog) : Option Catalog :=
  if c.mode == .openTable && c.children.isEmpty then some {c with mode := .closing} else none
def reference (c : Catalog) (child : Nat) : Option Catalog :=
  if c.mode == .openTable then some {c with children := child::c.children} else none
def acquire (c : Catalog) (owner : Nat) : Option Catalog :=
  if c.mode == .openTable then some {c with leases := owner::c.leases} else none
def release (c : Catalog) (owner gen : Nat) :=
  if c.generation == gen then {c with leases := c.leases.erase owner} else c
def finishDrop (c : Catalog) : Option Catalog :=
  if c.mode == .closing && c.leases.isEmpty then some {c with mode := .absent} else none
def abortDrop (c : Catalog) := if c.mode == .closing then {c with mode := .openTable} else c
def recreate (c : Catalog) : Option Catalog :=
  if c.mode == .absent then some ⟨.openTable,c.generation+1,[],[]⟩ else none

/-- The two serialization orders of creating a referencing child and closing its parent cannot both succeed. -/
def ForeignReferenceDropInterlock.Goal : Prop := ∀ c child,
  ((beginDrop c).bind (reference · child)) = none ∧ ((reference c child).bind beginDrop) = none

/-- Existing leases drain one acquisition at a time. A repeated acquisition by one reader still prevents drop after one release. -/
def RepeatedTableLeaseBlocksReclaim.Goal : Prop := ∀ c closing owner, beginDrop c = some closing →
  1 < c.leases.count owner → finishDrop (release closing owner c.generation) = none

/-- If every preexisting lease is released exactly once in any order, final drop succeeds with no dangling child references. -/
def QuiescentDropDrainsExactMultiset.Goal : Prop := ∀ c q (order : List Nat), beginDrop c = some q → order.Perm c.leases →
  let drained := order.foldl (fun t id => release t id c.generation) q
  ∃ gone, finishDrop drained = some gone ∧ gone.children = [] ∧ gone.mode = .absent

/-- An aborted close cannot resurrect a released lease or reopen an unrelated table incarnation. -/
def AbortedCloseKeepsConcurrentReleases.Goal : Prop := ∀ c q owner, beginDrop c = some q →
  abortDrop (release q owner q.generation) = release c owner c.generation

/-- A stale handle from a dropped table must not decrement a lease granted by the reincarnated table of the same name. -/
def RecreatedNameRejectsOldLease.Goal : Prop := ∀ old fresh held owner, recreate old = some fresh →
  acquire fresh owner = some held → release held owner old.generation = held

/-- Closing the gate for a drop prevents late readers from extending a previously established quiescence wait. -/
def NoLeaseAdmissionAfterReferenceCheck.Goal : Prop := ∀ c q owner old, beginDrop c = some q →
  acquire (release q old q.generation) owner = none

end TinylambOpt.ResearchGoals.DatabaseCorrectness.CatalogDrop
