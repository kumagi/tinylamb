import TinylambOpt.ResearchGoals.ResourcePolicies.BudgetedPlans
set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
namespace TinylambOpt.ResearchProofs.ResourcePolicies.Goal_BudgetedPlans_BudgetFrontierCover
open TinylambOpt.ResearchGoals.ResourcePolicies.BudgetedPlans BudgetFrontierCover

private theorem mem_cheapest_iff (ps : List Profile) (cap w : Nat) :
    cheapest ps cap = some w ↔
      (∃ p ∈ ps, p.peak ≤ cap ∧ p.work = w) ∧
      (∀ p ∈ ps, p.peak ≤ cap → w ≤ p.work) := by
  unfold cheapest
  constructor
  · intro h
    have h' := List.min?_eq_some_iff.mp h
    rcases h' with ⟨hm, hle⟩
    rcases List.mem_map.mp hm with ⟨p, hp, rfl⟩
    have hpf := List.mem_filter.mp hp
    refine ⟨⟨p, hpf.1, by simpa using hpf.2, rfl⟩, ?_⟩
    intro q hq hqc
    have : q.work ∈ (ps.filter (fun p => p.peak ≤ cap)).map Profile.work :=
      List.mem_map.mpr ⟨q, List.mem_filter.mpr ⟨hq, by simpa using hqc⟩, rfl⟩
    exact hle _ this
  · intro h
    rcases h with ⟨⟨p, hp, hpc, rfl⟩, hall⟩
    apply List.min?_eq_some_iff.mpr
    constructor
    · exact List.mem_map.mpr ⟨p, List.mem_filter.mpr ⟨hp, by simpa using hpc⟩, rfl⟩
    · intro a ha
      rcases List.mem_map.mp ha with ⟨q, hq, rfl⟩
      have hqf := List.mem_filter.mp hq
      exact hall q hqf.1 (by simpa using hqf.2)

theorem sound : Goal := by
  intro all kept hsub hcov cap
  cases hk : cheapest kept cap with
  | none =>
    cases ha : cheapest all cap with
    | none => rfl
    | some w =>
      exfalso
      have hwa := (mem_cheapest_iff all cap w).mp ha
      rcases hwa.1 with ⟨p, hp, hpc, rfl⟩
      rcases hcov p hp with ⟨q, hqk, hdom⟩
      rcases hdom with ⟨_hwq, _, hpq, _⟩
      have hqc : q.peak ≤ cap := Nat.le_trans hpq hpc
      have hmem : q.work ∈ (kept.filter (fun p => p.peak ≤ cap)).map Profile.work :=
        List.mem_map.mpr ⟨q, List.mem_filter.mpr ⟨hqk, by simpa using hqc⟩, rfl⟩
      unfold cheapest at hk
      rw [List.min?_eq_none_iff] at hk
      have : q.work ∈ ([] : List Nat) := hk ▸ hmem
      simp at this
  | some w =>
    symm
    apply (mem_cheapest_iff all cap w).mpr
    have hwk := (mem_cheapest_iff kept cap w).mp hk
    constructor
    · rcases hwk.1 with ⟨p, hp, hpc, rfl⟩
      exact ⟨p, hsub p hp, hpc, rfl⟩
    · intro p hp hpc
      rcases hcov p hp with ⟨q, hqk, hdom⟩
      rcases hdom with ⟨hwq, _, hpq, _⟩
      have hqc : q.peak ≤ cap := Nat.le_trans hpq hpc
      exact Nat.le_trans (hwk.2 q hqk hqc) hwq
end TinylambOpt.ResearchProofs.ResourcePolicies.Goal_BudgetedPlans_BudgetFrontierCover
