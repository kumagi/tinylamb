import Std

/-! SIMD probes inspect an ordered ring of control bytes. Vacant slots terminate linear probing, tombstones do not. A fingerprint nominates candidates for full-key comparisons. -/
namespace TinylambOpt.ResearchGoals.JitKernels.ControlByteProbe

inductive Slot where
  | empty | tomb | live (tag key payload : Nat)
  deriving DecidableEq, Repr
def ring (xs : List Slot) (home : Nat) := (List.range xs.length).filterMap (fun i => xs[(home+i)%xs.length]?)
def value (key : Nat) : Slot → Option Nat
  | .live _ k v => if key == k then some v else none
  | _ => none
def untilEmpty (xs : List Slot) := xs.takeWhile (· != .empty)
def probe (xs : List Slot) (key : Nat) := (untilEmpty xs).findSome? (value key)
def tagHit (tag : Nat) : Slot → Bool
  | .live t _ _ => t == tag
  | _ => false
def coherent (f : Nat → Nat) (xs : List Slot) := ∀ t k v, .live t k v ∈ xs → t = f k

/-- Mask candidates at and beyond the first vacant lane before exact comparison. -/
def FirstEmptyMasksPackedHits.Goal : Prop := ∀ xs k f, coherent f xs → probe xs k = ((untilEmpty xs).filter (tagHit (f k))).findSome? (value k)

/-- Exit an entire SIMD group on any empty byte without examining earlier matches. -/
def WholeControlGroupTermination.Goal : Prop := ∀ xs k, Slot.empty ∈ xs → probe xs k = none

/-- Compact out deleted lanes in registers before deciding the first live match. -/
def TombstoneErasureInRegister.Goal : Prop := ∀ xs k, probe (xs.filter (· != .tomb)) k = probe xs k

/-- Treat a tombstone with no preceding match as a vacant terminator. -/
def DeletedSlotAsProbeFence.Goal : Prop := ∀ a b k, probe a k = none → probe (a++[.tomb]++b) k = none

/-- A vector group can hand off to the next group exactly when neither hit nor vacancy settled it. -/
def ProbeContinuationAtVectorEdge.Goal : Prop := ∀ a b k, probe (a++b) k = (probe a k).orElse (fun _ => if a.contains .empty then none else probe b k)

/-- A unique occurrence of the query fingerprint in a group permits omitting the full key load. -/
def TagUniquenessInsideGroup.Goal : Prop := ∀ xs k f, coherent f xs → ((untilEmpty xs).filter (tagHit (f k))).length = 1 → (probe xs k).isSome

/-- A mirrored control array supports one contiguous, wrap-safe read at every home slot. -/
def RingMirrorUnalignedFetch.Goal : Prop := ∀ xs h n, h < xs.length → n ≤ xs.length → (ring xs h).take n = ((xs++xs).drop h).take n

/-- Reuse another probe's full group scan when only the low lane bits of their homes differ. -/
def AlignedProbeStartReuse.Goal : Prop := ∀ xs a b w k, 0 < w → a < xs.length → b < xs.length → a/w = b/w → probe (ring xs a) k = probe (ring xs b) k

/-- A negative comparison ballot over a whole group suffices to bypass its payload reads, even across a tombstone. -/
def TagNegativeSkipsPayloadPage.Goal : Prop := ∀ xs f k, coherent f xs → xs.any (tagHit (f k)) = false → probe xs k = none

/-- Loading all matching payloads in a control group never exceeds scalar payload traffic before termination. -/
def ProbeGroupSpeculativeReadBudget.Goal : Prop := ∀ xs f k, coherent f xs → (xs.filter (tagHit (f k))).length ≤ ((untilEmpty xs).filter (tagHit (f k))).length

#guard probe [.tomb,.live 1 9 7,.empty] 9 == some 7

end TinylambOpt.ResearchGoals.JitKernels.ControlByteProbe
