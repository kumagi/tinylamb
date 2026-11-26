import TinylambOpt.ResearchGoals.QueryRewrites.KeyRoutedJoinStates

/-!
# Goal_KeyRoutedJoinStates_OwnerElisionForEmptyCommenterIntersection

An owner with no commenter in the qualified set contributes no group: checking
the commenter intersection after flattening is equivalent to filtering each
owner's commenters before flattening.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.QueryRewrites.Goal_KeyRoutedJoinStates_OwnerElisionForEmptyCommenterIntersection

open TinylambOpt.ResearchGoals.QueryRewrites

theorem sound :
    TinylambOpt.ResearchGoals.QueryRewrites.KeyRoutedJoinStates.OwnerElisionForEmptyCommenterIntersection.Goal := by
  intro posts commenters qualified
  change (members (posts.flatMap commenters) qualified).isEmpty =
    ((posts.flatMap (fun p => (commenters p).filter qualified.contains)).isEmpty)
  have hmem : ∀ c : Nat,
      c ∈ members (posts.flatMap commenters) qualified ↔
        c ∈ posts.flatMap (fun p => (commenters p).filter qualified.contains) := by
    intro c
    constructor
    · intro hc
      rw [members] at hc
      rcases List.mem_filter.mp hc with ⟨hcerase, hcq⟩
      rw [List.mem_eraseDups] at hcerase
      rcases List.mem_flatMap.mp hcerase with ⟨p, hp, hcp⟩
      exact List.mem_flatMap.mpr ⟨p, hp, List.mem_filter.mpr ⟨hcp, hcq⟩⟩
    · intro hc
      rcases List.mem_flatMap.mp hc with ⟨p, hp, hcp⟩
      rcases List.mem_filter.mp hcp with ⟨hcp, hcq⟩
      rw [members]
      apply List.mem_filter.mpr
      constructor
      · rw [List.mem_eraseDups]
        exact List.mem_flatMap.mpr ⟨p, hp, hcp⟩
      · exact hcq
  apply Bool.eq_iff_iff.mpr
  constructor
  · intro h
    rw [List.isEmpty_iff] at h ⊢
    apply List.eq_nil_iff_forall_not_mem.mpr
    intro c hc
    have hc' : c ∈ members (posts.flatMap commenters) qualified := (hmem c).mpr hc
    simpa [h] using hc'
  · intro h
    rw [List.isEmpty_iff] at h ⊢
    apply List.eq_nil_iff_forall_not_mem.mpr
    intro c hc
    have hc' : c ∈ posts.flatMap (fun p => (commenters p).filter qualified.contains) := (hmem c).mp hc
    simpa [h] using hc'

end TinylambOpt.ResearchProofs.QueryRewrites.Goal_KeyRoutedJoinStates_OwnerElisionForEmptyCommenterIntersection
