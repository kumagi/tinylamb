import TinylambOpt.ResearchGoals.EngineBoundaries.PeerWindows

/-!
Window execution must preserve peer membership and frame results at ordering boundaries.

Proved

- `PercentDenominator` — Appending later peers preserves PERCENT_RANK's numerator but
  rescales its denominator.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.EngineBoundaries.Goal_PeerWindows_PercentDenominator

open TinylambOpt.ResearchGoals.EngineBoundaries.PeerWindows

theorem sound : PercentDenominator.Goal := by
  intro xs more k hlen hmore
  have hlow : lower more k = [] := by
    unfold lower
    apply List.eq_nil_iff_forall_not_mem.mpr
    intro x hx
    have hxmem := (List.mem_filter.mp hx).1
    have hxnot := hmore x hxmem
    have hbtrue := (List.mem_filter.mp hx).2
    rw [hxnot] at hbtrue
    contradiction
  have hxsle : ¬ xs.length ≤ 1 := by omega
  have hlenapp : (xs ++ more).length = xs.length + more.length := by simp
  have hmle : ¬ xs.length + more.length ≤ 1 := by omega
  simp [percentRank, rankOf, lower, hxsle, hmle, hlenapp]
  constructor
  · exact hmore
  · omega

end TinylambOpt.ResearchProofs.EngineBoundaries.Goal_PeerWindows_PercentDenominator
