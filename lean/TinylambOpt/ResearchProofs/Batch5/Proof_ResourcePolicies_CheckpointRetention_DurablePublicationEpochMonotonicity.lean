import TinylambOpt.ResearchGoals.ResourcePolicies.CheckpointRetention

namespace TinylambOpt.ResearchProofs.Batch5.ResourcePolicies.CheckpointRetention.Goal_DurablePublicationEpochMonotonicity

open TinylambOpt.ResearchGoals.ResourcePolicies.CheckpointRetention


/-! Once extra WAL is durable and extra snapshot files are present, every
previously ready generation stays ready. `find?` then selects at or before the
old choice in the epoch-descending list, so the published epoch cannot regress. -/

open TinylambOpt.ResearchGoals.ResourcePolicies.CheckpointRetention

private theorem ready_mono (w₁ w₂ : Nat) (a b : List Nat) (g : Generation)
    (hw : w₁ ≤ w₂) (hab : ∀ f ∈ a, f ∈ b) :
    ready w₁ a g = true → ready w₂ b g = true := by
  intro h
  unfold ready at h ⊢
  have hparts : decide (g.cut ≤ w₁) = true ∧ g.files.all (fun f => a.contains f) = true := by
    simpa [Bool.and_eq_true] using h
  have hcut : g.cut ≤ w₁ := of_decide_eq_true hparts.1
  have hall : ∀ f ∈ g.files, a.contains f = true := List.all_eq_true.mp hparts.2
  rw [Bool.and_eq_true]
  constructor
  · simp [Nat.le_trans hcut hw]
  · apply List.all_eq_true.mpr
    intro f hf
    have hfa : List.elem f a = true := hall f hf
    rw [List.elem_eq_mem] at hfa
    have hfm : f ∈ a := of_decide_eq_true hfa
    change List.elem f b = true
    rw [List.elem_eq_mem]
    simp [hab f hfm]

private theorem find?_epoch_ge (gs : List Generation)
    (hpw : gs.Pairwise (fun p q : Generation => q.epoch < p.epoch))
    (pred : Generation → Bool) (x y : Generation)
    (hxmem : x ∈ gs) (hxpred : pred x = true)
    (hyfind : List.find? pred gs = some y) : x.epoch ≤ y.epoch := by
  induction gs generalizing x y pred with
  | nil => simp at hxmem
  | cons g gs ih =>
      by_cases hg : pred g = true
      · have hy : y = g := by
          rw [List.find?_cons_of_pos hg] at hyfind
          cases hyfind
          rfl
        subst y
        rcases List.mem_cons.mp hxmem with hxeq | hxmem'
        · subst x
          omega
        · have hrel : x.epoch < g.epoch :=
            (List.pairwise_cons.mp hpw).1 x hxmem'
          exact Nat.le_of_lt hrel
      · have hxmem_tail : x ∈ gs := by
          rcases List.mem_cons.mp hxmem with hxg | hxgs
          · subst x
            exact False.elim (hg hxpred)
          · exact hxgs
        have hfind : List.find? pred gs = some y := by
          rw [List.find?_cons_of_neg hg] at hyfind
          exact hyfind
        exact ih (List.pairwise_cons.mp hpw).2 pred x y hxmem_tail hxpred hfind

theorem sound : TinylambOpt.ResearchGoals.ResourcePolicies.CheckpointRetention.DurablePublicationEpochMonotonicity.Goal := by
  intro gs w₁ w₂ a b x y hpw hw hab hx hy
  change List.find? (ready w₁ a) gs = some x at hx
  change List.find? (ready w₂ b) gs = some y at hy
  have hxmem : x ∈ gs := List.mem_of_find?_eq_some hx
  have hxready₁ : ready w₁ a x = true := List.find?_some hx
  have hxready₂ : ready w₂ b x = true := ready_mono w₁ w₂ a b x hw hab hxready₁
  exact find?_epoch_ge gs hpw (ready w₂ b) x y hxmem hxready₂ hy
end TinylambOpt.ResearchProofs.Batch5.ResourcePolicies.CheckpointRetention.Goal_DurablePublicationEpochMonotonicity
