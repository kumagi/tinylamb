import Std

/-!
Page allocation for stored SQL rows must preserve the byte ranges, alignment, and live
data required by the modeled layout.
-/

namespace TinylambOpt.ResearchGoals.LayerContracts.SlotPlacement

/-! One bounded page with byte occupancy. A request carries payload size, old slot,
and stripe width. The candidate allocator probes only the first aligned position
and positions after occupied bytes. The reference tests every starting offset. -/
structure Req where
  bytes : Nat
  old : Nat
  stripe : Nat
structure Model where
  footprint : Req → Nat → List Nat
  legal : Req → Nat → Bool
  snap : Req → Nat → Nat

def band (i n : Nat) := (List.range n).map (i+·)
def align (i base step : Nat) := base + (i-base+step-1)/step*step
def fits (m : Model) (r : Req) (cap : Nat) (used : List Nat) (i : Nat) :=
  i < cap && m.legal r i && (m.footprint r i).all (fun x => x < cap && !used.contains x)
def exhaustive (m : Model) (r : Req) (cap : Nat) (used : List Nat) :=
  (List.range cap).find? (fits m r cap used)
def probes (m : Model) (r : Req) (used : List Nat) :=
  ((0 :: used.map (·+1)).map (m.snap r)).eraseDups.mergeSort
def quick (m : Model) (r : Req) (cap : Nat) (used : List Nat) :=
  (probes m r used).find? (fits m r cap used)

/-- Return the same lowest address as the dense allocator, including failure. -/
def firstFit (m : Model) : Prop := ∀ r cap u, quick m r cap u = exhaustive m r cap u

/-- Can a missed probe establish exhaustion without scanning the page? -/
def exactFailure (m : Model) : Prop := ∀ r cap u, quick m r cap u = none ↔ ∀ i, fits m r cap u i = false

/-- If reusing the original base is legal, a prefix-limited search can find a fit. -/
def growInPlace (m : Model) : Prop := ∀ r cap u, fits m r cap u r.old → ∃ i, quick m r cap u = some i ∧ i ≤ r.old

/-- Reserving the returned footprint prevents overlapping later allocations. -/
def reservation (m : Model) : Prop := ∀ a b cap u i j, quick m a cap u = some i →
  quick m b cap (m.footprint a i ++ u) = some j →
  ∀ x ∈ m.footprint a i, x ∉ m.footprint b j

/-- Shrinking payload size cannot invalidate an already viable address. -/
def payloadMonotone (m : Model) : Prop := ∀ r n cap u i, n ≤ r.bytes → fits m r cap u i → fits m {r with bytes := n} cap u i

/-- Strong admission hypothesis: enough unoccupied bytes always allow this request. -/
def capacityOnly (m : Model) : Prop := ∀ r cap u, 0 < r.bytes → (u.filter (· < cap)).eraseDups.length + (m.footprint r (m.snap r 0)).length ≤ cap →
  (quick m r cap u).isSome

/-- From a cursor, skip to the rounded base. Every feasible placement lies
at or beyond it; if it fits, a restricted dense search must return it first. -/
def nextBoundary (m : Model) : Prop := ∀ r cap used i j, i ≤ j → fits m r cap used j →
  let first := m.snap r i
  i ≤ first ∧ first ≤ j ∧ m.legal r first ∧
    (fits m r cap used first →
      ((List.range cap).drop i).find? (fits m r cap used) = some first)

/-- Removing an occupied byte may reveal an earlier first-fit address. -/
def afterFree (m : Model) : Prop := ∀ r cap u x i, quick m r cap u = some i →
  ∃ j, quick m r cap (u.filter (· != x)) = some j ∧ j ≤ i

namespace Contiguous

/-- Plain row payload without padding. -/
def model : Model :=
  ⟨fun r i => band i r.bytes,
   fun _ _ => true, fun _ i => i⟩

def FirstFit.Goal : Prop := firstFit model
def ExactFailure.Goal : Prop := exactFailure model
def GrowInPlace.Goal : Prop := growInPlace model
def Reservation.Goal : Prop := reservation model
def PayloadMonotone.Goal : Prop := payloadMonotone model
def CapacityOnly.Goal : Prop := capacityOnly model
def NextBoundary.Goal : Prop := nextBoundary model
def AfterFree.Goal : Prop := afterFree model

def TouchEnd.Goal : Prop := ∀ r i, 0 < r.bytes → (model.footprint r i).getLast? = some (i+r.bytes-1)

def DensePrefix.Goal : Prop := ∀ r n cap, 0 < r.bytes → n+r.bytes ≤ cap → quick model r cap (band 0 n) = some n

end Contiguous

namespace AlignedWord

/-- Eight-byte-aligned payload. -/
def model : Model :=
  ⟨fun r i => band i r.bytes,
   fun _ i => i%8 == 0, fun _ i => align i 0 8⟩

def FirstFit.Goal : Prop := firstFit model
def ExactFailure.Goal : Prop := exactFailure model
def GrowInPlace.Goal : Prop := growInPlace model
def Reservation.Goal : Prop := reservation model
def PayloadMonotone.Goal : Prop := payloadMonotone model
def CapacityOnly.Goal : Prop := capacityOnly model
def NextBoundary.Goal : Prop := nextBoundary model
def AfterFree.Goal : Prop := afterFree model

def Padding.Goal : Prop := ∀ r i, model.snap r i - i < 8

def WordBase.Goal : Prop := ∀ r cap u i, quick model r cap u = some i → i%8 = 0

end AlignedWord

namespace TrailerWord

/-- Payload and a trailing four-byte slot tag. -/
def model : Model :=
  ⟨fun r i => band i (r.bytes+4),
   fun _ _ => true, fun _ i => i⟩

def FirstFit.Goal : Prop := firstFit model
def ExactFailure.Goal : Prop := exactFailure model
def GrowInPlace.Goal : Prop := growInPlace model
def Reservation.Goal : Prop := reservation model
def PayloadMonotone.Goal : Prop := payloadMonotone model
def CapacityOnly.Goal : Prop := capacityOnly model
def NextBoundary.Goal : Prop := nextBoundary model
def AfterFree.Goal : Prop := afterFree model

def TrailerCost.Goal : Prop := ∀ r i, (model.footprint r i).length = r.bytes+4

def EmptyTag.Goal : Prop := ∀ r, r.bytes = 0 → exhaustive model r 3 [] = none

end TrailerWord

namespace FenceStripe

/-- Payload may not cross a stripe boundary. -/
def model : Model :=
  ⟨fun r i => band i r.bytes,
   fun r i => i%(r.stripe+1) + r.bytes ≤ r.stripe+1, fun r i => if i%(r.stripe+1)+r.bytes ≤ r.stripe+1 then i else (i/(r.stripe+1)+1)*(r.stripe+1)⟩

def FirstFit.Goal : Prop := firstFit model
def ExactFailure.Goal : Prop := exactFailure model
def GrowInPlace.Goal : Prop := growInPlace model
def Reservation.Goal : Prop := reservation model
def PayloadMonotone.Goal : Prop := payloadMonotone model
def CapacityOnly.Goal : Prop := capacityOnly model
def NextBoundary.Goal : Prop := nextBoundary model
def AfterFree.Goal : Prop := afterFree model

def TooWide.Goal : Prop := ∀ r cap u, r.stripe+1 < r.bytes → exhaustive model r cap u = none

def FreshStripe.Goal : Prop := ∀ r i, r.bytes ≤ r.stripe+1 → model.legal r (i*(r.stripe+1))

end FenceStripe

namespace ReserveHeader

/-- Header space remains unavailable even on an empty page. -/
def model : Model :=
  ⟨fun r i => band i r.bytes,
   fun r i => r.stripe ≤ i, fun r i => max r.stripe i⟩

def FirstFit.Goal : Prop := firstFit model
def ExactFailure.Goal : Prop := exactFailure model
def GrowInPlace.Goal : Prop := growInPlace model
def Reservation.Goal : Prop := reservation model
def PayloadMonotone.Goal : Prop := payloadMonotone model
def CapacityOnly.Goal : Prop := capacityOnly model
def NextBoundary.Goal : Prop := nextBoundary model
def AfterFree.Goal : Prop := afterFree model

def HeaderFloor.Goal : Prop := ∀ r cap u i, quick model r cap u = some i → r.stripe ≤ i

def ExactPage.Goal : Prop := ∀ r, 0 < r.bytes → exhaustive model r (r.stripe+r.bytes) [] = some r.stripe

end ReserveHeader

namespace TwoFragments

/-- Store a header byte and a discontiguous payload. -/
def model : Model :=
  ⟨fun r i => i :: band (i+r.stripe+1) r.bytes,
   fun _ _ => true, fun _ i => i⟩

def FirstFit.Goal : Prop := firstFit model
def ExactFailure.Goal : Prop := exactFailure model
def GrowInPlace.Goal : Prop := growInPlace model
def Reservation.Goal : Prop := reservation model
def PayloadMonotone.Goal : Prop := payloadMonotone model
def CapacityOnly.Goal : Prop := capacityOnly model
def NextBoundary.Goal : Prop := nextBoundary model
def AfterFree.Goal : Prop := afterFree model

def HoleFree.Goal : Prop := ∀ r i x, i < x → x ≤ i+r.stripe → x ∉ model.footprint r i

def Charged.Goal : Prop := ∀ r i, (model.footprint r i).length = r.bytes+1

end TwoFragments

namespace ForwardGrowth

/-- Resize at the old offset or relocate to its right. -/
def model : Model :=
  ⟨fun r i => band i r.bytes,
   fun r i => r.old ≤ i, fun r i => max r.old i⟩

def FirstFit.Goal : Prop := firstFit model
def ExactFailure.Goal : Prop := exactFailure model
def GrowInPlace.Goal : Prop := growInPlace model
def Reservation.Goal : Prop := reservation model
def PayloadMonotone.Goal : Prop := payloadMonotone model
def CapacityOnly.Goal : Prop := capacityOnly model
def NextBoundary.Goal : Prop := nextBoundary model
def AfterFree.Goal : Prop := afterFree model

def NoBackfill.Goal : Prop := ∀ r cap u i, quick model r cap u = some i → r.old ≤ i

def Reuse.Goal : Prop := ∀ r cap u, fits model r cap u r.old → quick model r cap u = some r.old

end ForwardGrowth

namespace Interleave

/-- Column stripe owns every second byte. -/
def model : Model :=
  ⟨fun r i => (List.range r.bytes).map (fun j => i+2*j),
   fun r i => i%2 == r.old%2, fun r i => align i (r.old%2) 2⟩

def FirstFit.Goal : Prop := firstFit model
def ExactFailure.Goal : Prop := exactFailure model
def GrowInPlace.Goal : Prop := growInPlace model
def Reservation.Goal : Prop := reservation model
def PayloadMonotone.Goal : Prop := payloadMonotone model
def CapacityOnly.Goal : Prop := capacityOnly model
def NextBoundary.Goal : Prop := nextBoundary model
def AfterFree.Goal : Prop := afterFree model

def Parity.Goal : Prop := ∀ r i x, x ∈ model.footprint r i → x%2 = i%2

def FreeOtherLane.Goal : Prop := ∀ r cap u i, fits model r cap u i → fits model r cap ((band 0 cap).filter (fun x => x%2 != i%2) ++ u) i

end Interleave

namespace SlotGroup

/-- Reserve complete blocks of four bytes for variable-width slots. -/
def model : Model :=
  ⟨fun r i => band i (align r.bytes 0 4),
   fun _ i => i%4 == 0, fun _ i => align i 0 4⟩

def FirstFit.Goal : Prop := firstFit model
def ExactFailure.Goal : Prop := exactFailure model
def GrowInPlace.Goal : Prop := growInPlace model
def Reservation.Goal : Prop := reservation model
def PayloadMonotone.Goal : Prop := payloadMonotone model
def CapacityOnly.Goal : Prop := capacityOnly model
def NextBoundary.Goal : Prop := nextBoundary model
def AfterFree.Goal : Prop := afterFree model

def Waste.Goal : Prop := ∀ r i, (model.footprint r i).length-r.bytes < 4

def FullGroups.Goal : Prop := ∀ r i, (model.footprint r i).length % 4 = 0

end SlotGroup

namespace PhaseAligned

/-- Slot base matches the old slot modulo the stripe size. -/
def model : Model :=
  ⟨fun r i => band i r.bytes,
   fun r i => i%(r.stripe+1) == r.old%(r.stripe+1), fun r i => align i (r.old%(r.stripe+1)) (r.stripe+1)⟩

def FirstFit.Goal : Prop := firstFit model
def ExactFailure.Goal : Prop := exactFailure model
def GrowInPlace.Goal : Prop := growInPlace model
def Reservation.Goal : Prop := reservation model
def PayloadMonotone.Goal : Prop := payloadMonotone model
def CapacityOnly.Goal : Prop := capacityOnly model
def NextBoundary.Goal : Prop := nextBoundary model
def AfterFree.Goal : Prop := afterFree model

def OldPhase.Goal : Prop := ∀ r, model.snap r r.old = r.old

def Period.Goal : Prop := ∀ r i, model.snap r (i+r.stripe+1) = model.snap r i+r.stripe+1

end PhaseAligned

private def gap : Req := ⟨1,0,1⟩
#guard quick TwoFragments.model gap 4 [2] == none
#guard exhaustive TwoFragments.model gap 4 [2] == some 1

end TinylambOpt.ResearchGoals.LayerContracts.SlotPlacement
