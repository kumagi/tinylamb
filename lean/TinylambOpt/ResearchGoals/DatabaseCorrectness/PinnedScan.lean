import TinylambOpt.ResearchGoals.ResourcePolicies.VersionVacuum

/-! One slot of a read-only full scan. A registered chain contains committed
history (newest first); a writer may change physical bytes only after registration
and after the reader releases its shared page latch. Option values encode deletes. -/
namespace TinylambOpt.ResearchGoals.DatabaseCorrectness.PinnedScan
open TinylambOpt.ResearchGoals.ResourcePolicies.VersionVacuum (Version readAt ordered)

structure Slot where
  physical : Option Nat
  registered : Bool
  versions : List Version
  deriving DecidableEq, Repr
def resolve (s : Slot) (snapshot : Nat) :=
  if s.registered then readAt s.versions snapshot else s.physical
def pin (s : Slot) := !s.registered
def readWithVerdict (direct : Bool) (s : Slot) (snapshot : Nat) :=
  if direct then s.physical else resolve s snapshot
def stage (s : Slot) (v : Option Nat) := {s with registered := true, physical := v}
def install (s : Slot) (t : Nat) (v : Option Nat) :=
  {s with physical := v, registered := true, versions := ⟨t,v⟩::s.versions}

/-- Registering a chain while the reader holds a shared latch cannot falsify an earlier direct verdict
if the saved committed history agrees with the pinned bytes at that reader's timestamp. -/
def RegisteredBeforePhysicalMutation.Goal : Prop := ∀ s snap, !s.registered →
  readAt s.versions snap = s.physical →
  readWithVerdict (pin s) {s with registered := true} snap = resolve {s with registered := true} snap

/-- A fresh verdict on a re-pin resolves the old snapshot even when the next physical value belongs to an uncommitted writer. -/
def RepinAfterUncommittedOverwrite.Goal : Prop := ∀ s snap v, let next := stage s v
  readWithVerdict (pin next) next snap = readAt s.versions snap

/-- Reusing a positive verdict across an unlatch/re-pin is a stronger condition to try to falsify. -/
def ReusingVerdictAcrossRepin.Goal : Prop := ∀ s snap v, !s.registered → readAt s.versions snap = s.physical →
  readWithVerdict (pin s) (stage s v) snap = readAt s.versions snap

/-- A committed deletion beyond the reader's timestamp must not remove a row from an in-progress scan. -/
def LaterTombstoneDuringScan.Goal : Prop := ∀ s snap t, snap < t →
  resolve (install s t none) snap = readAt s.versions snap

/-- Erasing the chain after its head is stable for all remaining snapshots is observationally safe
only with a matching physical head image and no pending changes. -/
def RetiredChainDirectRead.Goal : Prop := ∀ s (head : Version) rest oldest snap, s.registered →
  s.versions = head::rest → ordered s.versions → s.physical = head.value → head.stamp ≤ oldest → oldest ≤ snap →
  resolve {s with registered := false} snap = resolve s snap

/-- A morsel split may re-pin and resolve each half separately; newer publications between the halves must not fracture the result. -/
def SnapshotScanAcrossMorselBoundary.Goal : Prop := ∀ (a b : List Slot) snap t v, snap < t →
  (∀ s ∈ b, resolve s snap = readAt s.versions snap) →
  a.map (resolve · snap) ++ (b.map (fun s => resolve (install s t v) snap)) = (a++b).map (resolve · snap)

#guard
  let old : Slot := ⟨some 7,false,[⟨1,some 7⟩]⟩
  let changed := stage old (some 8)
  readWithVerdict (pin old) changed 1 == some 8 && readWithVerdict (pin changed) changed 1 == some 7

end TinylambOpt.ResearchGoals.DatabaseCorrectness.PinnedScan
