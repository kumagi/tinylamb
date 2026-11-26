import TinylambOpt.ResearchGoals.ResourcePolicies.MergeScheduling

namespace TinylambOpt.ResearchProofs.ResourcePolicies.MergeScheduling.RewriteMinimumAlsoMinimizesPeak

open TinylambOpt.ResearchGoals.ResourcePolicies.MergeScheduling

theorem refute : ¬ RewriteMinimumAlsoMinimizesPeak.Goal := by
  intro h
  let a : Run := .source 1
  let b : Run := .source 1
  let c : Run := .merge (.source 1) (.source 1)
  have hw : writes (.merge (.merge a b) c) ≤ writes (.merge a (.merge b c)) := by native_decide
  have hp := h a b c hw
  simp [a, b, c, peak, temp, size] at hp

end TinylambOpt.ResearchProofs.ResourcePolicies.MergeScheduling.RewriteMinimumAlsoMinimizesPeak
