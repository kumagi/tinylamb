import TinylambOpt.ResearchGoals.ResourcePolicies.MorselRetirement
set_option linter.unusedSectionVars false

/-! Proof/counterexample for `TinylambOpt.ResearchGoals.ResourcePolicies.MorselRetirement.EmptyMorselRetirement.Goal`. The theorem below certifies the resource-policy claim (or refutes the strong hypothesis) in the model defined by that module, preserving the modeled result. -/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false
set_option linter.unusedSimpArgs false
set_option linter.deprecated false

/-!
A completed empty morsel advances the retirement frontier without fabricating rows.

Claim (`EmptyMorselRetirement.Goal`): if id `start` has reported the empty row list,
then `emit rs start (n+1) = emit rs (start+1) n`: the empty result contributes no
rows to the ordered output, so retirement proceeds exactly as if the window had
started at the next morsel. This distinguishes "completed with zero rows" from
"missing" in cancellation and retry logic.
-/

namespace TinylambOpt.ResearchProofs.ResourcePolicies.Goal_MorselRetirement_EmptyMorselRetirement
open TinylambOpt.ResearchGoals.ResourcePolicies.MorselRetirement

private theorem cands_one (s n : Nat) :
    (List.range (n+1)).map (s + ·) = s :: (List.range n).map (s + 1 + ·) := by
  rw [show n+1 = 1+n from by omega, List.range_add, List.map_append]
  simp only [List.map_cons, List.map_nil, List.range_one, List.append_nil]
  rw [List.map_map]
  simp [Nat.add_assoc]

theorem sound : EmptyMorselRetirement.Goal := by
  intro rs start n h
  have hp : (fun i => (lookup rs i).isSome) start = true := by
    show (lookup rs start).isSome = true
    rw [h]
    decide
  have hready : readyIds rs start (n+1) = start :: readyIds rs (start+1) n := by
    show ((List.range (n+1)).map (start + ·)).takeWhile (fun i => (lookup rs i).isSome)
        = _
    rw [cands_one]
    show (start :: (List.range n).map (start + 1 + ·)).takeWhile _
        = start :: ((List.range n).map (start + 1 + ·)).takeWhile _
    simp only [List.takeWhile, hp, if_pos]
  have hd : (lookup rs start).getD [] = [] := by rw [h]; rfl
  show (readyIds rs start (n+1)).flatMap (fun i => (lookup rs i).getD [])
      = (readyIds rs (start+1) n).flatMap (fun i => (lookup rs i).getD [])
  rw [hready, List.flatMap_cons, hd, List.nil_append]

end TinylambOpt.ResearchProofs.ResourcePolicies.Goal_MorselRetirement_EmptyMorselRetirement
