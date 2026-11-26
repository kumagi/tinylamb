import TinylambOpt.ResearchProofs.Batch5.Proof_ResourcePolicies_SlabReuse_SizeClassRefinement

/-!
Registers the existing checked result for `SlabReuse.SizeClassRefinement.Goal`.

/-- A completed trace charges exactly the requested bytes, independent of internal fragmentation. -/
def SuccessfulTraceByteConservation.Goal : Prop := ∀ hs rs final,
  place hs rs = some final → final.sum + rs.sum = hs.sum

/-- The largest free extent suffices to decide admission of one nonzero contiguous grant. -/
def LargestHoleSingleAdmission.Goal : Prop := ∀ hs n, 0 < n →
  (alloc n hs).isSome = decide (n ≤ hs.max?.getD 0)

/-- Descending-size placement is a sufficient exact admission test for a batch that fits in some order. -/
def DescendingSizeBatchAdmission.Goal : Prop := ∀ hs (rs order : List Nat), rs.Perm order →
  (place hs order).isSome = true → (place hs (rs.mergeSort (fun a b => decide (b ≤ a)))).isSome = true

/-- Best-fit placement never loses an allocation trace that first-fit can finish. -/
def BestFitTraceDominance.Goal : Prop := ∀ hs rs, (place hs rs).isSome = true → (bestFit hs rs).isSome = true

/-- Coalescing neighboring extents never loses a previously feasible first-fit trace in the same list order. -/
def NeighborCoalesceTraceDominance.Goal : Prop := ∀ front a b back rs,
  (place (front++[a,b]++back) rs).isSome = true → (place (front++[a+b]++back) rs).isSome = true

/-- Allowing scatter grants over arbitrary holes admits a trace whenever the sum fits after one ideal compaction. -/
def CompactedArenaTraceAdmission.Goal : Prop := ∀ (hs rs : List Nat),
  ((place [hs.sum] rs).isSome = true ↔ rs.sum ≤ hs.sum)

def rounded (classes : List Nat) (n : Nat) := (classes.filter (fun c => n ≤ c)).min?
def charge (classes requests : List Nat) := (requests.mapM (rounded classes)).map List.sum

/-- Adding an intermediate size class cannot increase reserved bytes for an already supported trace. -/

The theorem is contributed by `TinylambOpt.ResearchProofs.Batch5.ResourcePolicies.SlabReuse.Goal_SizeClassRefinement` and re-exported here so that
`SlabReuse_All.lean` registers the goal. SQL execution may rely on the
modeled result only under the assumptions stated above.
-/

namespace TinylambOpt.ResearchProofs.ResourcePolicies.Goal_SlabReuse_SizeClassRefinement

theorem sound : TinylambOpt.ResearchGoals.ResourcePolicies.SlabReuse.SizeClassRefinement.Goal :=
  TinylambOpt.ResearchProofs.Batch5.ResourcePolicies.SlabReuse.Goal_SizeClassRefinement.sound

end TinylambOpt.ResearchProofs.ResourcePolicies.Goal_SlabReuse_SizeClassRefinement
