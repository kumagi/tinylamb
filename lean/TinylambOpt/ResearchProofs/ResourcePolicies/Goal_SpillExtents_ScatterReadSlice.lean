import TinylambOpt.ResearchProofs.Batch5.Proof_ResourcePolicies_SpillExtents_ScatterReadSlice

/-!
Registers the existing checked result for `SpillExtents.ScatterReadSlice.Goal`.

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

The theorem is contributed by `TinylambOpt.ResearchProofs.Batch5.ResourcePolicies.SpillExtents.Goal_ScatterReadSlice` and re-exported here so that
`SpillExtents_All.lean` registers the goal. SQL execution may rely on the
modeled result only under the assumptions stated above.
-/

namespace TinylambOpt.ResearchProofs.ResourcePolicies.Goal_SpillExtents_ScatterReadSlice

theorem sound : TinylambOpt.ResearchGoals.ResourcePolicies.SpillExtents.ScatterReadSlice.Goal :=
  TinylambOpt.ResearchProofs.Batch5.ResourcePolicies.SpillExtents.Goal_ScatterReadSlice.sound

end TinylambOpt.ResearchProofs.ResourcePolicies.Goal_SpillExtents_ScatterReadSlice
