import Std
import TinylambOpt.ResearchGoals.ResourcePolicies.SlabReuse

/-!
Proof contribution for ResourcePolicies_SlabReuse_CompactedArenaTraceAdmission. The theorem below
records a planning or execution decision that preserves the modeled
SQL result, or an explicit counterexample when the proposed rewrite is
unsound.
-/


namespace TinylambOpt.ResearchProofs.Batch5.ResourcePolicies.SlabReuse.Goal_CompactedArenaTraceAdmission

open TinylambOpt.ResearchGoals.ResourcePolicies.SlabReuse


namespace SlabReuseProof

open TinylambOpt.ResearchGoals.ResourcePolicies.SlabReuse

/-- First-fit into a single extent succeeds exactly when the request sum fits in that extent. -/
theorem place_single_isSome (N : Nat) : ∀ rs : List Nat,
    (place [N] rs).isSome = decide (rs.sum ≤ N) := by
  intro rs
  induction rs generalizing N with
  | nil =>
      simp [place]
  | cons n rs ih =>
      by_cases hn : n ≤ N
      · have halloc : alloc n [N] = some [N - n] := by simp [alloc, hn]
        simp [place, List.foldlM_cons, halloc]
        change (place [N - n] rs).isSome = decide (n + rs.sum ≤ N)
        rw [ih]
        by_cases hsum : rs.sum ≤ N - n
        · have hsum2 : n + rs.sum ≤ N := by omega
          simp [hsum, hsum2]
        · have hsum2 : ¬ n + rs.sum ≤ N := by omega
          simp [hsum, hsum2]
      · have halloc : alloc n [N] = none := by simp [alloc, hn]
        simp [place, List.foldlM_cons, halloc, List.sum_cons]
        by_cases hsum : n + rs.sum ≤ N
        · have : n ≤ N := by omega
          exact (hn this).elim
        · simp [hsum]

end SlabReuseProof

open TinylambOpt.ResearchGoals.ResourcePolicies.SlabReuse

/-- A single compacted extent admits a scatter trace iff the trace's byte sum fits. -/
theorem sound : TinylambOpt.ResearchGoals.ResourcePolicies.SlabReuse.CompactedArenaTraceAdmission.Goal := by
  intro hs rs
  rw [SlabReuseProof.place_single_isSome]
  simp
end TinylambOpt.ResearchProofs.Batch5.ResourcePolicies.SlabReuse.Goal_CompactedArenaTraceAdmission
