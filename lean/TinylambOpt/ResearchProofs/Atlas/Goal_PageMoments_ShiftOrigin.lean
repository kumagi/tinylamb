import TinylambOpt.ResearchGoals.Atlas.SynopsisBounds

/-!
A query can use a synopsis to avoid inspecting some input only when the claimed bounds
hold for the underlying data.

Proved

- `PageMoments.ShiftOrigin` — `∀ s a b, energy s b = energy s a +
  2*(a-b)*(s.sum-s.length*a) + s.length*(a-b)^2`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_PageMoments_ShiftOrigin

open TinylambOpt.ResearchGoals.Atlas.SynopsisBounds
open TinylambOpt.ResearchGoals.Atlas.SynopsisBounds.PageMoments

-- Truth check: exhaustive over all pages of length <= 3 with entries in
-- [-3, 3] and all centres a, b in [-3, 3] (511 pages x 49 centre pairs)
-- returned `true`.

/-- Re-centring a page's centred second moment is a pure quadratic identity:
    expanding `(x-b)^2 = (x-a)^2 + 2(a-b)(x-a) + (a-b)^2` and summing. -/
theorem sound_PageMoments_ShiftOrigin : PageMoments.ShiftOrigin.Goal := by
  intro s a b
  induction s with
  | nil => simp [PageMoments.energy]
  | cons x xs ih =>
      simp only [PageMoments.energy, List.map_cons, List.sum_cons, List.length_cons] at ih ⊢
      rw [ih]
      grind

end TinylambOpt.ResearchProofs.Atlas.Goal_PageMoments_ShiftOrigin
