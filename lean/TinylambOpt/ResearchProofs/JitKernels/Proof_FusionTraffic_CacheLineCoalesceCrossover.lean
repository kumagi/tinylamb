import TinylambOpt.ResearchGoals.JitKernels.FusionTraffic

/-! SOUND: Speculative dense loading beats lazy gathering exactly when the saved gather
dispatch covers the extra lines. Since `sel` stays inside the first `n` rows, the sparse
footprint is a subset of the dense footprint, so with `dense = sparse + k` both sides are
connected by `line*dense = line*sparse + line*k` and the equivalence is linear arithmetic. -/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unusedSimpArgs false
set_option linter.deprecated false

namespace TinylambOpt.ResearchProofs.JitKernels.FusionTraffic_CacheLineCoalesceCrossover
open TinylambOpt.ResearchGoals.JitKernels.FusionTraffic

/-- `eraseDups` of any list is duplicate-free. -/
private theorem nodup_eraseDups (l : List Nat) : l.eraseDups.Nodup := by
  have key : ∀ n : Nat, ∀ l : List Nat, l.length ≤ n → l.eraseDups.Nodup := by
    intro n
    induction n with
    | zero =>
        intro m hle
        match m with
        | [] => exact List.Pairwise.nil
        | a :: as =>
          rw [List.length_cons] at hle
          exact absurd hle (by omega)
    | succ k ih =>
        intro m hle
        match m with
        | [] => exact List.Pairwise.nil
        | a :: as =>
          have has : as.length ≤ k := by rw [List.length_cons] at hle; omega
          rw [List.eraseDups_cons, List.nodup_cons]
          refine ⟨fun hx => ?_,
            ih (as.filter fun b => !(b == a)) (Nat.le_trans (List.length_filter_le _ as) has)⟩
          rw [List.mem_eraseDups, List.mem_filter] at hx
          obtain ⟨_, hp⟩ := hx
          rw [Nat.not_beq_eq_true_eq] at hp
          exact hp rfl
  exact key l.length l (Nat.le_refl l.length)

/-- The crossover comparison is sound by cancellative linear arithmetic. -/
theorem sound : CacheLineCoalesceCrossover.Goal := by
  intro base stride w sel n issue gatherFee line hi
  show issue + line * (footprint base stride w (List.range n)).length
        ≤ gatherFee * sel.length + line * (footprint base stride w sel).length ↔
      issue + line * ((footprint base stride w (List.range n)).length
        - (footprint base stride w sel).length) ≤ gatherFee * sel.length
  have hsub : footprint base stride w sel ⊆ footprint base stride w (List.range n) := by
    intro x hx
    simp only [footprint] at hx ⊢
    rw [List.mem_eraseDups, List.mem_map] at hx ⊢
    obtain ⟨i, hisel, rfl⟩ := hx
    exact ⟨i, List.mem_range.mpr (hi i hisel), rfl⟩
  obtain ⟨k, hk⟩ : ∃ k, (footprint base stride w (List.range n)).length
      = (footprint base stride w sel).length + k :=
    Nat.exists_eq_add_of_le
      (List.Nodup.length_le_of_subset (nodup_eraseDups _) hsub)
  have hdiff : (footprint base stride w (List.range n)).length
      - (footprint base stride w sel).length = k := by omega
  constructor
  · intro hgoal
    rw [hdiff]
    rw [hk, Nat.mul_add] at hgoal
    omega
  · intro hgoal
    rw [hdiff] at hgoal
    rw [hk, Nat.mul_add]
    omega

end TinylambOpt.ResearchProofs.JitKernels.FusionTraffic_CacheLineCoalesceCrossover
