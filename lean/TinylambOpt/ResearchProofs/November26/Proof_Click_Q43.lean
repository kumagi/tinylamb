import TinylambOpt.ResearchGoals.November26.Click

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false


namespace TinylambOpt.ResearchProofs.November26.Click.Goal_Q43
open TinylambOpt.ResearchGoals.November26.Click

theorem div_eq_of_bounds {lo s hi : Int} (h1 : lo ≤ s) (h2 : s ≤ hi)
    (h3 : lo / 60 = hi / 60) : s / 60 = lo / 60 := by
  have a := Int.ediv_le_ediv (by decide : (0 : Int) < 60) h1
  have b := Int.ediv_le_ediv (by decide : (0 : Int) < 60) h2
  omega

theorem sound : TinylambOpt.ResearchGoals.November26.Click.Q43.Goal := by
  intro seconds lo hi hbound heq
  induction seconds with
  | nil => simp
  | cons s ss ih =>
      simp only [List.map_cons, List.length_cons, List.replicate_succ]
      have hs : lo ≤ s ∧ s ≤ hi := hbound s (by simp)
      have hss : ∀ t ∈ ss, lo ≤ t ∧ t ≤ hi := by
        intro t ht
        exact hbound t (by simp [ht])
      rw [div_eq_of_bounds hs.1 hs.2 heq, ih hss]
end TinylambOpt.ResearchProofs.November26.Click.Goal_Q43
