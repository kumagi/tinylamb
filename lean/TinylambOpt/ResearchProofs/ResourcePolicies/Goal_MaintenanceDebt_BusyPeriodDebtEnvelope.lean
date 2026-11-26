import TinylambOpt.ResearchGoals.ResourcePolicies.MaintenanceDebt
set_option linter.unusedSectionVars false

/-! Proof/counterexample for `TinylambOpt.ResearchGoals.ResourcePolicies.MaintenanceDebt.BusyPeriodDebtEnvelope.Goal`. The theorem below certifies the resource-policy claim (or refutes the strong hypothesis) in the model defined by that module, preserving the modeled result. -/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false
set_option linter.unusedSimpArgs false
set_option linter.deprecated false

namespace TinylambOpt.ResearchProofs.ResourcePolicies.Goal_MaintenanceDebt_BusyPeriodDebtEnvelope
open TinylambOpt.ResearchGoals.ResourcePolicies.MaintenanceDebt

private theorem foldl_max_le : ∀ (cap : Nat), ∀ xs : List Nat, ∀ x : Nat,
    x ≤ cap → (∀ z, z ∈ xs → z ≤ cap) → List.foldl max x xs ≤ cap := by
  intro cap xs
  induction xs with
  | nil => intro x hx _; exact hx
  | cons y ys ih =>
      intro x hx hz
      have hxy : max x y ≤ cap := Nat.max_le.mpr ⟨hx, hz y (by simp)⟩
      exact ih (max x y) hxy (fun z hzz => hz z (List.mem_cons.mpr (Or.inr hzz)))

private theorem maxgetD_le : ∀ (l : List Nat) (cap : Nat),
    (∀ z, z ∈ l → z ≤ cap) → (l.max?).getD 0 ≤ cap := by
  intro l cap h
  cases l with
  | nil => simp
  | cons x xs =>
      rw [List.max?_cons']
      simp only [Option.getD_some]
      exact foldl_max_le cap xs x (h x (by simp)) (fun z hz => h z (List.mem_cons.mpr (Or.inr hz)))

private theorem sE_ge : ∀ (ts : List Tick),
    ((ts.map Tick.added).sum - (ts.map Tick.capacity).sum) ≤ suffixExcess ts := by
  intro ts
  apply List.le_max?_getD_of_mem
  apply List.mem_map.mpr
  refine ⟨0, List.mem_range.mpr (by simp), ?_⟩
  simp [excess]

private theorem sE_cons : ∀ (t : Tick) (ts : List Tick),
    suffixExcess (t :: ts) = max (excess (t :: ts)) (suffixExcess ts) := by
  intro t ts
  apply Nat.le_antisymm
  · apply maxgetD_le
    intro y hy
    obtain ⟨k, hk, rfl⟩ := (List.mem_map).mp hy
    rw [List.mem_range] at hk
    rcases Nat.eq_zero_or_pos k with hk0 | hk0
    · rw [hk0]
      apply Nat.le_max_left
    · have h1 : excess ((t :: ts).drop k) = excess (ts.drop (k - 1)) := by
        rw [List.drop_cons hk0]
      have hle : excess (ts.drop (k - 1)) ≤ (List.map (fun k => excess (ts.drop k))
          (List.range (ts.length + 1))).max?.getD 0 :=
        List.le_max?_getD_of_mem
          (List.mem_map.mpr ⟨k - 1, List.mem_range.mpr (by rw [List.length_cons] at hk; omega),
            rfl⟩)
      rw [h1]
      exact Nat.le_trans hle (Nat.le_max_right _ _)
  · apply Nat.max_le.mpr
    constructor
    · apply List.le_max?_getD_of_mem
      apply List.mem_map.mpr
      exact ⟨0, List.mem_range.mpr (by simp), by rw [List.drop_zero]⟩
    · apply maxgetD_le
      intro z hz
      obtain ⟨k, hk, rfl⟩ := (List.mem_map).mp hz
      rw [List.mem_range] at hk
      refine Nat.le_trans (List.le_max?_getD_of_mem (List.mem_map.mpr
        ⟨k + 1, List.mem_range.mpr (by simpa using hk), ?_⟩)) (Nat.le_refl _)
      rw [List.drop_cons (Nat.succ_pos k), Nat.succ_sub_one]

private theorem endDebt_gen : ∀ (ts : List Tick) (o : Nat),
    endDebt o ts = max (o + (ts.map Tick.added).sum - (ts.map Tick.capacity).sum)
      (suffixExcess ts) := by
  intro ts
  induction ts with
  | nil => intro o; simp [endDebt, suffixExcess, excess]
  | cons t ts ih =>
      intro o
      rw [show endDebt o (t :: ts) = endDebt (step o t) ts from rfl]
      rw [ih, sE_cons]
      have hS := sE_ge ts
      simp only [excess, step, List.map_cons, List.sum_cons] at *
      omega

/--
Starting from zero debt, the final backlog equals the largest suffix excess of
the trace (the last busy period): the generalized recursion
endDebt o ts = max (o + added ts - capacity ts) (suffixExcess ts) shows the WAL
replay only ever owes what the worst unflushed suffix demanded, so the debt is
computable from suffix excesses alone.
-/
theorem sound : BusyPeriodDebtEnvelope.Goal := by
  intro ts
  rw [endDebt_gen]
  have hge := sE_ge ts
  omega

end TinylambOpt.ResearchProofs.ResourcePolicies.Goal_MaintenanceDebt_BusyPeriodDebtEnvelope
