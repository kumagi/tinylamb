import Std

/-! Spill quota decisions distinguish logical bytes, block allocation and the highest
live physical block. Extents are half-open; failed or canceled writes have no published
logical payload until their new directory is installed. -/
namespace TinylambOpt.ResearchGoals.ResourcePolicies.SpillExtents

def blocks (unit n : Nat) := (n+unit-1)/unit
def roof (ids : List Nat) := (ids.max?).map (·+1) |>.getD 0
def tailCharge (unit : Nat) (lengths : List Nat) := (lengths.map (fun n => unit*blocks unit n)).sum

/-- Sharing the unused tail between two consecutive segments saves at most one block including alignment. -/
def SegmentTailSharingBound.Goal : Prop := ∀ u a b, 0 < u →
  let separate := blocks u a + blocks u b
  blocks u (a+b) ≤ separate ∧ separate ≤ blocks u (a+b)+1

/-- Peak active logical bytes alone suffice for reserving physical space under segment churn. -/
def LogicalHighWaterPaysFragmentation.Goal : Prop := ∀ u ls budget, 0 < u → ls.sum ≤ budget →
  tailCharge u ls ≤ u*blocks u budget

/-- Reusable holes plus append headroom determine whether a scatter-write can fit without exceeding the file quota. -/
def ScatterAdmissionAtFileLimit.Goal : Prop := ∀ (holes : List Nat) requested used cap,
  holes.Nodup → (∀ h ∈ holes, h < used) → used ≤ cap →
  (used + (requested-holes.length) ≤ cap ↔ requested ≤ holes.length + (cap-used))

/-- It suffices to truncate at the highest still-referenced block, even when some segments share immutable blocks. -/
def SharedExtentTruncateFence.Goal : Prop := ∀ (segments : List (List Nat)) limit,
  roof segments.flatten ≤ limit ↔ ∀ seg ∈ segments, ∀ id ∈ seg, id < limit

/-- Relocating the last live block into a lower hole strictly shrinks the minimum backing-file length. -/
def TailEvacuationShrinksFile.Goal : Prop := ∀ (ids : List Nat) last hole,
  ids.max? = some last → ¬ hole ∈ ids → hole < last →
  roof (ids.map (fun i => if i == last then hole else i)) < roof ids

/-- A per-segment truncate based only on its active neighbors is proposed as a replacement for a global live-block scan. -/
def LocalDirectoryTruncateFence.Goal : Prop := ∀ (retained : List (List Nat)) (recent : List Nat),
  recent ∈ retained → roof retained.flatten ≤ roof recent

/-- Fusing appends into one reservation only changes the number of allocation calls, not the additional blocks required. -/
def BunchedAppendBlockDebt.Goal : Prop := ∀ u n a b, 0 < u →
  (blocks u (n+a)-blocks u n) + (blocks u (n+a+b)-blocks u (n+a)) = blocks u (n+a+b)-blocks u n

/-- Rollback of a short append must preserve the old partial tail as well as the old logical length. -/
def PartialTailRollback.Goal : Prop := ∀ (old payload : List Nat) written,
  ((old ++ payload.take written).take old.length) = old

/-- Copying a live extent need only move the byte span overlapping a request, including the final partial block. -/
def ScatterReadSlice.Goal : Prop := ∀ (chunks : List (List Nat)) start len,
  let bases := (chunks.map List.length).scanl (·+·) 0
  let selected := (bases.zip chunks).flatMap (fun (p,bs) =>
    ((bs.drop (start-p)).take (start+len-max p start)))
  selected = (chunks.flatten.drop start).take len

/-- Equal occupied byte totals imply equal temporary peak during a copy-and-publish compaction. -/
def LiveTotalPredictsRelocationPeak.Goal : Prop := ∀ (a b : List Nat), a.sum = b.sum →
  a.sum + (a.max?).getD 0 = b.sum + (b.max?).getD 0

/-- Coalescing adjacent reads trades a fixed I/O start cost against the holes read as payload. -/
def CoalescedReadGapBreakEven.Goal : Prop := ∀ (sizes gaps : List Nat) startCost,
  sizes.length = gaps.length+1 →
  (startCost+sizes.sum+gaps.sum ≤ sizes.length*startCost+sizes.sum ↔ gaps.sum ≤ gaps.length*startCost)

/-- A reader pinned to the old mapping observes identical bytes after private compaction if every live block is copied before publication. -/
def ExtentGenerationReadEquivalence.Goal : Prop := ∀ (directory : List Nat) (old new : Nat → List Nat)
    (relocate : Nat → Nat) start len,
  (∀ id ∈ directory, new (relocate id) = old id) →
  (((directory.map relocate).flatMap new).drop start).take len = ((directory.flatMap old).drop start).take len

#guard tailCharge 65536 [1,1] == 131072 && blocks 65536 2 == 1

end TinylambOpt.ResearchGoals.ResourcePolicies.SpillExtents
