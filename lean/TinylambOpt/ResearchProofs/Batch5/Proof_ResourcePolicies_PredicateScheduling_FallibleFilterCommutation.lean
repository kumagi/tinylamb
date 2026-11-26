import TinylambOpt.ResearchGoals.ResourcePolicies.PredicateScheduling

namespace TinylambOpt.ResearchProofs.Batch5.ResourcePolicies.PredicateScheduling.Goal_FallibleFilterCommutation

open TinylambOpt.ResearchGoals.ResourcePolicies.PredicateScheduling


/-! Two fallible filters commute whenever every row on which both are
non-passing yields the same observable outcome; passing rows simply fall
through to the rest of the conjunction. -/

namespace Scratch.FallibleFilterCommutation

open TinylambOpt.ResearchGoals.ResourcePolicies.PredicateScheduling

theorem sound : TinylambOpt.ResearchGoals.ResourcePolicies.PredicateScheduling.FallibleFilterCommutation.Goal := by
  intro a b rest rows h
  induction rows with
  | nil => simp
  | cons r rs ih =>
      have hr := h r (by simp)
      have htail : ∀ x ∈ rs, a x ≠ .pass → b x ≠ .pass → a x = b x := by
        intro x hx hax hbx
        exact h x (by simp [hx]) hax hbx
      have ihres := ih htail
      simp only [List.map_cons]
      congr 1
      cases ha : a r <;> cases hb : b r <;> simp [evaluate, ha, hb] at hr ⊢ <;> assumption

end Scratch.FallibleFilterCommutation
end TinylambOpt.ResearchProofs.Batch5.ResourcePolicies.PredicateScheduling.Goal_FallibleFilterCommutation
