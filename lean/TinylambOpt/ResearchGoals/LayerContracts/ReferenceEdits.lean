import Std

namespace TinylambOpt.ResearchGoals.LayerContracts.ReferenceEdits

/-! Referential existence over parent keys and nullable child references. Parent
multiplicity models the presence index; parent-key uniqueness, cascades and
deferred checking must be justified separately by a caller. Edits below are
explicit proposed DML sequences. Erase removes one occurrence. -/
structure DB where
  parents : List Nat := []
  children : List (Option Nat) := []
  deriving BEq, Inhabited
inductive Edit where
  | addP (k : Nat) | delP (k : Nat) | addC (k : Option Nat) | delC (k : Option Nat)
  | rewrite (old : Nat) (target : Option Nat) | purge (k : Nat)
def step (s : DB) : Edit → DB
  | .addP k => {s with parents := k::s.parents}
  | .delP k => {s with parents := s.parents.erase k}
  | .addC k => {s with children := k::s.children}
  | .delC k => {s with children := s.children.erase k}
  | .rewrite k v => {s with children := s.children.map (fun x => if x == some k then v else x)}
  | .purge k => {s with children := s.children.filter (· != some k)}
def run (s : DB) (ops : List Edit) := ops.foldl step s
def bad (s : DB) (k : Nat) := s.children.contains (some k) && !s.parents.contains k
def violations (s : DB) := (s.children.filterMap id).eraseDups.filter (bad s)
def ok (s : DB) := (violations s).isEmpty
def sameBag (a b : DB) := a.parents.Perm b.parents ∧ a.children.Perm b.children
structure Model where
  ops : Nat → Nat → List Edit
  watch : Nat → Nat → List Nat
def finish (m : Model) (s : DB) (a b : Nat) := run s (m.ops a b)
def immediate (m : Model) (s : DB) (a b : Nat) := ((m.ops a b).scanl step s).all ok
def localOK (m : Model) (s : DB) (a b : Nat) :=
  !(m.watch a b).any (bad (finish m s a b))

/-- A previously valid database needs revalidation only on the declared key set. -/
def deltaCheck (m : Model) : Prop := ∀ s a b, ok s → localOK m s a b = ok (finish m s a b)

/-- References outside the write footprint preserve their violation status. -/
def frameKeys (m : Model) : Prop := ∀ s a b k, k ∉ m.watch a b → bad (finish m s a b) k = bad s k

/-- Can an immediate checker be replaced by an end-of-statement check? -/
def defer (m : Model) : Prop := ∀ s a b, ok s → immediate m s a b = ok (finish m s a b)

/-- Blind retry of the statement after lost acknowledgement preserves both bags. -/
def replay (m : Model) : Prop := ∀ s a b, let t := finish m s a b
  sameBag (finish m t a b) t

/-- Disjoint logical key footprints allow two statements to commute up to bag order. -/
def independent (m : Model) : Prop := ∀ s a b c d, (∀ k ∈ m.watch a b, k ∉ m.watch c d) →
  sameBag (finish m (finish m s a b) c d) (finish m (finish m s c d) a b)

/-- Physical permutation of input rows preserves all final parent and child counts. -/
def stableRows (m : Model) : Prop := ∀ s t a b, sameBag s t → sameBag (finish m s a b) (finish m t a b)

/-- Precision of the incremental violation report, even on already dirty input. -/
def newViolations (m : Model) : Prop := ∀ s a b, let t := finish m s a b
  ∀ k, k ∈ (violations t).filter (fun x => !bad s x) ↔ k ∈ m.watch a b ∧ bad t k ∧ !bad s k

/-- Can checking just the first affected key authorize the complete statement? -/
def singleProbe (m : Model) : Prop := ∀ s a b, ok s →
  (!(m.watch a b).head?.any (bad (finish m s a b))) = ok (finish m s a b)

namespace ParentFirst

/-- Install a parent before a possibly different child. -/
def model : Model :=
  ⟨fun a b => [.addP a,.addC (some b)], fun a b => [a,b]⟩

def DeltaCheck.Goal : Prop := deltaCheck model
def FrameKeys.Goal : Prop := frameKeys model
def Defer.Goal : Prop := defer model
def Replay.Goal : Prop := replay model
def Independent.Goal : Prop := independent model
def StableRows.Goal : Prop := stableRows model
def NewViolations.Goal : Prop := newViolations model
def SingleProbe.Goal : Prop := singleProbe model

def NeedsTarget.Goal : Prop := ∀ s a b, ok s → (ok (finish model s a b) = true ↔ a=b ∨ b ∈ s.parents)

def ParentCount.Goal : Prop := ∀ s a b, (finish model s a b).parents.count a = s.parents.count a+1

end ParentFirst

namespace ChildFirst

/-- Install a child and repair its parent later in the statement. -/
def model : Model :=
  ⟨fun a b => [.addC (some a),.addP a,.addP b], fun a b => [a,b]⟩

def DeltaCheck.Goal : Prop := deltaCheck model
def FrameKeys.Goal : Prop := frameKeys model
def Defer.Goal : Prop := defer model
def Replay.Goal : Prop := replay model
def Independent.Goal : Prop := independent model
def StableRows.Goal : Prop := stableRows model
def NewViolations.Goal : Prop := newViolations model
def SingleProbe.Goal : Prop := singleProbe model

def FinalValid.Goal : Prop := ∀ s a b, ok s → ok (finish model s a b)

def FirstBarrier.Goal : Prop := ∀ s a b, ok s → (immediate model s a b = true ↔ a ∈ s.parents)

end ChildFirst

namespace Reparent

/-- Retarget all matching children, then remove one parent occurrence. -/
def model : Model :=
  ⟨fun a b => [.rewrite a (some b),.delP a], fun a b => [a,b]⟩

def DeltaCheck.Goal : Prop := deltaCheck model
def FrameKeys.Goal : Prop := frameKeys model
def Defer.Goal : Prop := defer model
def Replay.Goal : Prop := replay model
def Independent.Goal : Prop := independent model
def StableRows.Goal : Prop := stableRows model
def NewViolations.Goal : Prop := newViolations model
def SingleProbe.Goal : Prop := singleProbe model

def MoveAll.Goal : Prop := ∀ s a b, a ≠ b → (finish model s a b).children.count (some a) = 0

def KeepNulls.Goal : Prop := ∀ s a b, (finish model s a b).children.count none = s.children.count none

end Reparent

namespace Cascade

/-- Explicitly purge matching children before removing a parent. -/
def model : Model :=
  ⟨fun a _ => [.purge a,.delP a], fun a _ => [a]⟩

def DeltaCheck.Goal : Prop := deltaCheck model
def FrameKeys.Goal : Prop := frameKeys model
def Defer.Goal : Prop := defer model
def Replay.Goal : Prop := replay model
def Independent.Goal : Prop := independent model
def StableRows.Goal : Prop := stableRows model
def NewViolations.Goal : Prop := newViolations model
def SingleProbe.Goal : Prop := singleProbe model

def NoOrphan.Goal : Prop := ∀ s a b, ok s → immediate model s a b

def DroppedRows.Goal : Prop := ∀ s a b, (finish model s a b).children.length + s.children.count (some a) = s.children.length

end Cascade

namespace SetNull

/-- Null out a reference, keeping all child rows. -/
def model : Model :=
  ⟨fun a _ => [.rewrite a none,.delP a], fun a _ => [a]⟩

def DeltaCheck.Goal : Prop := deltaCheck model
def FrameKeys.Goal : Prop := frameKeys model
def Defer.Goal : Prop := defer model
def Replay.Goal : Prop := replay model
def Independent.Goal : Prop := independent model
def StableRows.Goal : Prop := stableRows model
def NewViolations.Goal : Prop := newViolations model
def SingleProbe.Goal : Prop := singleProbe model

def RowCount.Goal : Prop := ∀ s a b, (finish model s a b).children.length = s.children.length

def NewNulls.Goal : Prop := ∀ s a b, (finish model s a b).children.count none = s.children.count none + s.children.count (some a)

end SetNull

namespace ReplaceParent

/-- Delete and restore a parent while preserving its children. -/
def model : Model :=
  ⟨fun a _ => [.delP a,.addP a], fun a _ => [a]⟩

def DeltaCheck.Goal : Prop := deltaCheck model
def FrameKeys.Goal : Prop := frameKeys model
def Defer.Goal : Prop := defer model
def Replay.Goal : Prop := replay model
def Independent.Goal : Prop := independent model
def StableRows.Goal : Prop := stableRows model
def NewViolations.Goal : Prop := newViolations model
def SingleProbe.Goal : Prop := singleProbe model

def Restore.Goal : Prop := ∀ s a b, a ∈ s.parents → sameBag (finish model s a b) s

def LastCopy.Goal : Prop := ∀ s a b, ok s → s.parents.count a=1 → some a ∈ s.children → immediate model s a b = false

end ReplaceParent

namespace Adopt

/-- Install a replacement parent, rewrite all references, remove the old key. -/
def model : Model :=
  ⟨fun a b => [.addP b,.rewrite a (some b),.delP a], fun a b => [a,b]⟩

def DeltaCheck.Goal : Prop := deltaCheck model
def FrameKeys.Goal : Prop := frameKeys model
def Defer.Goal : Prop := defer model
def Replay.Goal : Prop := replay model
def Independent.Goal : Prop := independent model
def StableRows.Goal : Prop := stableRows model
def NewViolations.Goal : Prop := newViolations model
def SingleProbe.Goal : Prop := singleProbe model

def AlwaysValid.Goal : Prop := ∀ s a b, ok s → immediate model s a b

def MovedCount.Goal : Prop := ∀ s a b, a ≠ b → (finish model s a b).children.count (some b) = s.children.count (some a)+s.children.count (some b)

end Adopt

namespace DropPair

/-- Drop one child occurrence then delete one occurrence of the matching parent. -/
def model : Model :=
  ⟨fun a _ => [.delC (some a),.delP a], fun a _ => [a]⟩

def DeltaCheck.Goal : Prop := deltaCheck model
def FrameKeys.Goal : Prop := frameKeys model
def Defer.Goal : Prop := defer model
def Replay.Goal : Prop := replay model
def Independent.Goal : Prop := independent model
def StableRows.Goal : Prop := stableRows model
def NewViolations.Goal : Prop := newViolations model
def SingleProbe.Goal : Prop := singleProbe model

def DuplicateChild.Goal : Prop := ∀ s a b, s.parents.count a=1 → 1 < s.children.count (some a) → bad (finish model s a b) a

def SurvivingParent.Goal : Prop := ∀ s a b, 1 < s.parents.count a → bad (finish model s a b) a = false

end DropPair

namespace SwapReferences

/-- Redirect a to b and then b to a, testing sequential versus simultaneous intent. -/
def model : Model :=
  ⟨fun a b => [.rewrite a (some b),.rewrite b (some a)], fun a b => [a,b]⟩

def DeltaCheck.Goal : Prop := deltaCheck model
def FrameKeys.Goal : Prop := frameKeys model
def Defer.Goal : Prop := defer model
def Replay.Goal : Prop := replay model
def Independent.Goal : Prop := independent model
def StableRows.Goal : Prop := stableRows model
def NewViolations.Goal : Prop := newViolations model
def SingleProbe.Goal : Prop := singleProbe model

def TrueSwap.Goal : Prop := ∀ s a b, a ≠ b → (finish model s a b).children.count (some a) = s.children.count (some b)

def SecondSeesFirst.Goal : Prop := ∀ s a b, a ≠ b → (finish model s a b).children.count (some b) = 0

end SwapReferences

namespace RekeyLate

/-- Rename a parent first and repair child rows last. -/
def model : Model :=
  ⟨fun a b => [.delP a,.addP b,.rewrite a (some b)], fun a b => [a,b]⟩

def DeltaCheck.Goal : Prop := deltaCheck model
def FrameKeys.Goal : Prop := frameKeys model
def Defer.Goal : Prop := defer model
def Replay.Goal : Prop := replay model
def Independent.Goal : Prop := independent model
def StableRows.Goal : Prop := stableRows model
def NewViolations.Goal : Prop := newViolations model
def SingleProbe.Goal : Prop := singleProbe model

def Repaired.Goal : Prop := ∀ s a b, ok s → ok (finish model s a b)

def PrefixFault.Goal : Prop := ∀ s a b, s.parents.count a=1 → some a ∈ s.children → immediate model s a b = false

end RekeyLate

#guard ok (finish ChildFirst.model {} 3 4)
#guard !(immediate ChildFirst.model {} 3 4)
#guard (finish SwapReferences.model ⟨[1,2],[some 1,some 2]⟩ 1 2).children == [some 1,some 1]

end TinylambOpt.ResearchGoals.LayerContracts.ReferenceEdits
