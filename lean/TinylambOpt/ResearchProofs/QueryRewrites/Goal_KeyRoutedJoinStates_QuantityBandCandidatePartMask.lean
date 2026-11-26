import TinylambOpt.ResearchGoals.QueryRewrites.KeyRoutedJoinStates

/-!
# Goal_KeyRoutedJoinStates_QuantityBandCandidatePartMask

Intersecting candidate quantity branches with eligible part branches implements
the disjunction without double counting: membership in the candidate index list
is exactly the per-row band predicate.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.QueryRewrites.Goal_KeyRoutedJoinStates_QuantityBandCandidatePartMask

open TinylambOpt.ResearchGoals.QueryRewrites

theorem sound :
    TinylambOpt.ResearchGoals.QueryRewrites.KeyRoutedJoinStates.QuantityBandCandidatePartMask.Goal := by
  intro qty eligible
  dsimp only [TinylambOpt.ResearchGoals.QueryRewrites.KeyRoutedJoinStates.QuantityBandCandidatePartMask.Goal]
  let bands : List (Nat × Nat) := [(5, 15), (15, 25), (29, 39)]
  let candidates := (bands.zipIdx.filter (fun ((l, h), _) => l ≤ qty && qty ≤ h)).map Prod.snd
  let hit : Nat → Bool := fun i => bands[i]?.any (fun (l, h) => l ≤ qty && qty ≤ h)
  have hcand : ∀ i, candidates.contains i = hit i := by
    intro i
    by_cases h0 : i = 0
    · subst i
      simp [candidates, bands, hit]
    · by_cases h1 : i = 1
      · subst i
        simp [candidates, bands, hit, h0]
      · by_cases h2 : i = 2
        · subst i
          simp [candidates, bands, hit, h0, h1]
        · have hi : 3 ≤ i := by omega
          simp [candidates, bands, hit, h0, h1, h2, hi]
  have hleft : (members eligible candidates).isEmpty = true ↔ ∀ i ∈ eligible, candidates.contains i = false := by
    rw [List.isEmpty_iff, List.eq_nil_iff_forall_not_mem]
    constructor
    · intro h i hi
      by_cases hci : candidates.contains i = true
      · exfalso
        apply h i
        rw [members]
        exact List.mem_filter.mpr ⟨List.mem_eraseDups.mpr hi, hci⟩
      · simpa using hci
    · intro h c hc
      have hc' := List.mem_filter.mp hc
      have hcfalse := h c (List.mem_eraseDups.mp hc'.1)
      have hc2 := hc'.2
      rw [hcfalse] at hc2
      contradiction
  have hright : (!eligible.any hit) = true ↔ ∀ i ∈ eligible, hit i = false := by
    simp
  apply Bool.eq_iff_iff.mpr
  constructor
  · intro h
    rw [hright]
    intro i hi
    have hc := hleft.mp h i hi
    rwa [← hcand i]
  · intro h
    rw [hleft]
    intro i hi
    have hh : hit i = false := hright.mp h i hi
    rwa [hcand i]

end TinylambOpt.ResearchProofs.QueryRewrites.Goal_KeyRoutedJoinStates_QuantityBandCandidatePartMask
