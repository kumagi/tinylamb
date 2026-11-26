import TinylambOpt.ResearchGoals.ResourcePolicies.RestartDirectory
set_option linter.unusedSimpArgs false
set_option linter.unnecessarySimpa false
set_option linter.deprecated false
set_option linter.unusedVariables false
set_option linter.unusedSectionVars false

/-! Proof/counterexample for `TinylambOpt.ResearchGoals.ResourcePolicies.RestartDirectory.RelativeRestartVarintBudget.Goal`. The theorem below certifies the resource-policy claim (or refutes the strong hypothesis) in the model defined by that module, preserving the modeled result. -/
set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.ResourcePolicies.Goal_RestartDirectory_RelativeRestartVarintBudget

open TinylambOpt.ResearchGoals.ResourcePolicies.RestartDirectory
open TinylambOpt.ResearchGoals.Compression.Wire

private theorem width_mono {n m : Nat} (h : n ≤ m) : width n ≤ width m := by
  unfold width
  split <;> split <;> rename_i hn hm <;> simp_all
  · -- n != 0, m != 0
    have hn0 : n ≠ 0 := by intro h0; simp [h0] at hn
    have hm0 : m ≠ 0 := by intro h0; subst h0; omega
    have hpow : 2 ^ n.log2 ≤ n := (Nat.le_log2 hn0).mp (Nat.le_refl _)
    have hpow2 : 2 ^ n.log2 ≤ m := Nat.le_trans hpow h
    have hle : n.log2 ≤ m.log2 := (Nat.le_log2 hm0).mpr hpow2
    omega

private theorem ubytes_mono {n m : Nat} (h : n ≤ m) : ubytes n ≤ ubytes m := by
  unfold ubytes
  have hw := width_mono h
  omega

theorem goal : RelativeRestartVarintBudget.Goal := by
  intro ds base
  simp only [RelativeRestartVarintBudget.Goal, streamBytes, List.map_map]
  induction ds with
  | nil => simp
  | cons d ds ih =>
      simp [List.map_cons, List.sum_cons]
      exact Nat.add_le_add (ubytes_mono (Nat.sub_le d base)) ih

end TinylambOpt.ResearchProofs.ResourcePolicies.Goal_RestartDirectory_RelativeRestartVarintBudget
