import TinylambOpt.ResearchGoals.ResourcePolicies.ClusteredPages

/-!
Proof contribution for ResourcePolicies_ClusteredPages_IndependentZonesCertifyMatchingPair. The theorem below
records a planning or execution decision that preserves the modeled
SQL result, or an explicit counterexample when the proposed rewrite is
unsound.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
namespace TinylambOpt.ResearchProofs.ResourcePolicies.ClusteredPages.Goal_IndependentZonesCertifyMatchingPair

open TinylambOpt.ResearchGoals.ResourcePolicies.ClusteredPages


theorem refute : ¬ TinylambOpt.ResearchGoals.ResourcePolicies.ClusteredPages.IndependentZonesCertifyMatchingPair.Goal := by
  intro h
  have hx : candidate 5 5 ⟨[(0, 10), (10, 0)].map Prod.fst, 0⟩ = true := by decide
  have hy : candidate 5 5 ⟨[(0, 10), (10, 0)].map Prod.snd, 0⟩ = true := by decide
  have hres := h [(0, 10), (10, 0)] 5 5 hx hy
  have hbad : ¬ (5, 5) ∈ ([(0, 10), (10, 0)] : List (Nat × Nat)) := by decide
  exact hbad hres

end TinylambOpt.ResearchProofs.ResourcePolicies.ClusteredPages.Goal_IndependentZonesCertifyMatchingPair
