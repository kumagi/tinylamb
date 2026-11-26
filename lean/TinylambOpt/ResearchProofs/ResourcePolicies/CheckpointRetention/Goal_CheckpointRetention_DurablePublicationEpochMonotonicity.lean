import TinylambOpt.ResearchGoals.ResourcePolicies.CheckpointRetention
set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
namespace TinylambOpt.ResearchProofs.ResourcePolicies.Goal_CheckpointRetention_DurablePublicationEpochMonotonicity
open TinylambOpt.ResearchGoals.ResourcePolicies.CheckpointRetention

private theorem ready_mono {w1 w2 : Nat} {a b : List Nat} {g : Generation}
    (hw : w1 ≤ w2) (hf : ∀ f ∈ a, f ∈ b) (h : ready w1 a g = true) : ready w2 b g = true := by
  simp only [ready, Bool.and_eq_true, decide_eq_true_eq, List.all_eq_true] at h ⊢
  constructor
  · omega
  · intro f hfmem
    have := h.2 f hfmem
    simp only [List.contains_iff_mem, decide_eq_true_eq] at this ⊢
    exact hf _ this

private theorem find?_some_epoch_ge {gs : List Generation} {pred : Generation → Bool} {g x : Generation}
    (hfind : gs.find? pred = some x) (hg : g ∈ gs) (hpred : pred g = true)
    (hpw : gs.Pairwise (fun p q => q.epoch < p.epoch)) : g.epoch ≤ x.epoch := by
  induction gs with
  | nil => cases hg
  | cons y ys ih =>
    simp only [List.find?_cons] at hfind
    by_cases hy : pred y = true
    · simp [hy] at hfind
      subst hfind
      rcases List.mem_cons.mp hg with rfl | hg
      · exact Nat.le_refl _
      · have hpw' := List.pairwise_cons.mp hpw
        exact Nat.le_of_lt (hpw'.1 _ hg)
    · simp [hy] at hfind
      rcases List.mem_cons.mp hg with rfl | hg
      · simp [hy] at hpred
      · exact ih hfind hg (List.pairwise_cons.mp hpw).2

theorem sound : DurablePublicationEpochMonotonicity.Goal := by
  intro gs w1 w2 a b x y hpw hw hf hx hy
  have hxmem : x ∈ gs := List.mem_of_find?_eq_some hx
  have hxready : ready w1 a x = true := by
    have := List.find?_some hx
    simpa using this
  have hxready2 : ready w2 b x = true := ready_mono hw hf hxready
  exact find?_some_epoch_ge hy hxmem hxready2 hpw
end TinylambOpt.ResearchProofs.ResourcePolicies.Goal_CheckpointRetention_DurablePublicationEpochMonotonicity
