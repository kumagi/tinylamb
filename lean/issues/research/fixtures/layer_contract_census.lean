import TinylambOpt.ResearchGoals.LayerContracts.SlotPlacement
import TinylambOpt.ResearchGoals.LayerContracts.FrameRetractions

/-! Small finite differential experiments on the actual research models. -/
namespace LayerCensus
open TinylambOpt.ResearchGoals.LayerContracts

def layouts : List (String × SlotPlacement.Model) :=
  open SlotPlacement in
  [("Contiguous", Contiguous.model), ("AlignedWord", AlignedWord.model),
   ("TrailerWord", TrailerWord.model), ("FenceStripe", FenceStripe.model),
   ("ReserveHeader", ReserveHeader.model), ("TwoFragments", TwoFragments.model),
   ("ForwardGrowth", ForwardGrowth.model), ("Interleave", Interleave.model),
   ("SlotGroup", SlotGroup.model), ("PhaseAligned", PhaseAligned.model)]

def placement (m : SlotPlacement.Model) : Nat × Nat := Id.run do
  let mut missed := 0
  let mut later := 0
  for mask in [:32] do
    let used := (List.range 5).filter mask.testBit
    for bytes in [1,2,3] do
      for old in [:3] do
        for stripe in [:3] do
          let r : SlotPlacement.Req := ⟨bytes,old,stripe⟩
          let best := SlotPlacement.exhaustive m r 5 used
          let quick := SlotPlacement.quick m r 5 used
          if best.isSome && quick.isNone then missed := missed + 1
          if quick.isSome && quick != best then later := later + 1
  return (missed, later)

def aggregates : List (String × FrameRetractions.Model) :=
  open FrameRetractions in
  [("Sum", Sum.model), ("Product", Product.model), ("Minimum", Minimum.model),
   ("Maximum", Maximum.model), ("BitUnion", BitUnion.model), ("BitParity", BitParity.model),
   ("DistinctCount", DistinctCount.model), ("FloorMean", FloorMean.model),
   ("RangeWidth", RangeWidth.model), ("LowerMedian", LowerMedian.model)]

def frames : Nat → List (List Nat)
  | 0 => [[]]
  | n+1 => (frames n).flatMap fun xs => [0,1,2,3].map (· :: xs)

def retraction (m : FrameRetractions.Model) : Nat × Nat × Nat := Id.run do
  let mut tested := 0
  let mut fallback := 0
  let mut mismatch := 0
  for n in [1,2,3,4] do
    for xs in frames n do
      for x in xs do
        tested := tested + 1
        match FrameRetractions.dropOne m xs x with
        | none => fallback := fallback + 1
        | some guess =>
          if guess != FrameRetractions.value m (xs.erase x) then mismatch := mismatch + 1
  return (tested, fallback, mismatch)

end LayerCensus

def main : IO Unit := do
  IO.println "Layout: missed fit, later than first fit (864 trials each)"
  for (name,m) in LayerCensus.layouts do
    let (missed, later) := LayerCensus.placement m
    IO.println s!"{name}: {missed}, {later}"
  IO.println "Aggregate: eviction trials, requested rescan, accepted but wrong"
  for (name,m) in LayerCensus.aggregates do
    let (tested, fallback, mismatch) := LayerCensus.retraction m
    IO.println s!"{name}: {tested}, {fallback}, {mismatch}"
