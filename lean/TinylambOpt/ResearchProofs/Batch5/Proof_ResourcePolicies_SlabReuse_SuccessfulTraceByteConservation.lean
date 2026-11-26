import Std
import TinylambOpt.ResearchGoals.ResourcePolicies.SlabReuse

/-!
Proof contribution for ResourcePolicies_SlabReuse_SuccessfulTraceByteConservation. The theorem below
records a planning or execution decision that preserves the modeled
SQL result, or an explicit counterexample when the proposed rewrite is
unsound.
-/


namespace TinylambOpt.ResearchProofs.Batch5.ResourcePolicies.SlabReuse.Goal_SuccessfulTraceByteConservation

open TinylambOpt.ResearchGoals.ResourcePolicies.SlabReuse


namespace SlabReuseProof

open TinylambOpt.ResearchGoals.ResourcePolicies.SlabReuse

/-- Every successful grant removes exactly the requested byte count from the hole list. -/
theorem alloc_sum (n : Nat) : ∀ hs hs', alloc n hs = some hs' → hs'.sum + n = hs.sum := by
  intro hs
  induction hs with
  | nil =>
      intro hs' h
      simp [alloc] at h
  | cons head hs ih =>
      intro hs' h
      by_cases hn : n ≤ head
      · simp [alloc, hn] at h
        subst hs'
        simp [List.sum_cons]
        omega
      · cases hopt : alloc n hs with
        | none => simp [alloc, hn, hopt] at h
        | some t =>
            simp [alloc, hn, hopt] at h
            subst hs'
            have ht := ih t hopt
            simp [List.sum_cons] at ht ⊢
            omega

end SlabReuseProof

open TinylambOpt.ResearchGoals.ResourcePolicies.SlabReuse

/-- A completed trace charges exactly the requested bytes: residual plus requests equals capacity. -/
theorem sound : TinylambOpt.ResearchGoals.ResourcePolicies.SlabReuse.SuccessfulTraceByteConservation.Goal := by
  intro hs rs final hfinal
  induction rs generalizing hs with
  | nil =>
      simp [place] at hfinal
      subst final
      simp
  | cons n rs ih =>
      simp only [place, List.foldlM_cons] at hfinal
      cases hopt : alloc n hs with
      | none => simp [hopt] at hfinal
      | some init =>
          simp [hopt] at hfinal
          have hinit := SlabReuseProof.alloc_sum n hs init hopt
          have hrest := ih init hfinal
          simp [List.sum_cons] at hinit hrest ⊢
          omega
end TinylambOpt.ResearchProofs.Batch5.ResourcePolicies.SlabReuse.Goal_SuccessfulTraceByteConservation
