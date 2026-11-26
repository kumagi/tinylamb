import TinylambOpt.ResearchGoals.ResourcePolicies.MergeScheduling
set_option linter.unusedSimpArgs false
set_option linter.unnecessarySimpa false
set_option linter.deprecated false
set_option linter.unusedVariables false
set_option linter.unusedSectionVars false

/-! Proof/counterexample for `TinylambOpt.ResearchGoals.ResourcePolicies.MergeScheduling.HotKeyResumeCoverage.Goal`. The theorem below certifies the resource-policy claim (or refutes the strong hypothesis) in the model defined by that module, preserving the modeled result. -/

namespace TinylambOpt.ResearchProofs.ResourcePolicies.Goal_MergeScheduling_HotKeyResumeCoverage

open TinylambOpt.ResearchGoals.ResourcePolicies.MergeScheduling


/-! Soundness of `HotKeyResumeCoverage`: rank tiling from 0 and from any spill
    cut concatenates to the full left-to-right hot-key cross product. -/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

open TinylambOpt.ResearchGoals.ResourcePolicies.MergeScheduling

private theorem div_mod_tile (l r x : Nat) (hr : 0 < r) (hx : x ∈ List.range r) :
    ((l * r + x) / r, (l * r + x) % r) = (l, x) := by
  have hxlt : x < r := List.mem_range.mp hx
  have hdiv : (l * r + x) / r = l := by
    rw [Nat.mul_comm l r, Nat.mul_add_div hr]
    simp [Nat.div_eq_of_lt hxlt]
  have hmod : (l * r + x) % r = x := by
    rw [Nat.mul_comm l r, Nat.mul_add_mod, Nat.mod_eq_of_lt hxlt]
  simp [hdiv, hmod]

private theorem range_mul_map_div_mod : ∀ l r, 0 < r →
    (List.range (l * r)).map (fun k => (k / r, k % r)) =
      (List.range l).flatMap (fun i => (List.range r).map (fun j => (i, j))) := by
  intro l
  induction l with
  | zero =>
      intro r hr
      simp
  | succ l ih =>
      intro r hr
      rw [Nat.succ_mul, List.range_add, List.map_append, ih r hr]
      rw [List.range_succ, List.flatMap_append]
      simp only [List.flatMap_cons, List.flatMap_nil, List.append_nil]
      congr 1
      rw [List.map_map]
      apply List.map_congr_left
      intro x hx
      exact div_mod_tile l r x hr hx

private theorem rankPairs_split (l r cut : Nat) (hcut : cut ≤ l * r) :
    rankPairs r 0 cut ++ rankPairs r cut (l * r - cut) =
      (List.range (l * r)).map (fun k => (k / r, k % r)) := by
  unfold rankPairs
  have hsum : cut + (l * r - cut) = l * r := Nat.add_sub_of_le hcut
  have hrange := List.range_add (n := cut) (m := l * r - cut)
  rw [hsum] at hrange
  rw [hrange, List.map_append]
  simp [List.map_map, Function.comp_def]

theorem sound : TinylambOpt.ResearchGoals.ResourcePolicies.MergeScheduling.HotKeyResumeCoverage.Goal := by
  intro l r cut hr hcut
  rw [rankPairs_split l r cut hcut]
  exact range_mul_map_div_mod l r hr
end TinylambOpt.ResearchProofs.ResourcePolicies.Goal_MergeScheduling_HotKeyResumeCoverage
