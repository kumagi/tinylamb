import TinylambOpt.ResearchGoals.EngineBoundaries.PlanEnvelope

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.EngineBoundaries.Goal_PlanEnvelope_ParetoPrune

open TinylambOpt.ResearchGoals.EngineBoundaries.PlanEnvelope

private theorem min_cons_of_le (a b : Nat) (xs : List Nat) (h : a ≤ b) :
    (a :: b :: xs).min? = (a :: xs).min? := by
  cases hx : xs.min? with
  | none =>
      have hnil : xs = [] := List.min?_eq_none_iff.mp hx
      subst xs
      simp [h]
  | some m =>
      rw [List.min?_cons]
      rw [List.min?_cons, hx]
      rw [List.min?_cons, hx]
      simp only [Option.some.injEq, Option.elim_some]
      omega

theorem sound : ParetoPrune.Goal := by
  intro p q rest n g hcheap
  unfold best
  have hle : cost p n g ≤ cost q n g := by simpa [cheaper] using hcheap
  simpa using min_cons_of_le (cost p n g) (cost q n g) (List.map (fun r => cost r n g) rest) hle

end TinylambOpt.ResearchProofs.EngineBoundaries.Goal_PlanEnvelope_ParetoPrune
