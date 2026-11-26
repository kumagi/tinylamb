import TinylambOpt.ResearchGoals.EngineBoundaries.PeerWindows

/-!
Window execution must preserve peer membership and frame results at ordering boundaries.

Proved

- `NullPeerFront` — NULL peers at the front all have rank one; their cumulative
  fraction counts the full run.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.EngineBoundaries.Goal_PeerWindows_NullPeerFront

open TinylambOpt.ResearchGoals.EngineBoundaries.PeerWindows

theorem sound : NullPeerFront.Goal := by
  intro xs hx
  have hbefore : ∀ a : Key, (compare a none == Ordering.lt) = false := by
    intro a
    cases a <;> rfl
  have hfilt : (xs.filter fun a => before a none) = [] := by
    apply List.eq_nil_iff_forall_not_mem.mpr
    intro a ha
    have hb : before a none = false := by simpa [before] using hbefore a
    have hbtrue := (List.mem_filter.mp ha).2
    rw [hb] at hbtrue
    contradiction
  constructor
  · simp [dense, lower, hfilt]
  · constructor
    · simp [rankOf, lower, hfilt]
    · simp [cume, lower, hfilt]

end TinylambOpt.ResearchProofs.EngineBoundaries.Goal_PeerWindows_NullPeerFront
