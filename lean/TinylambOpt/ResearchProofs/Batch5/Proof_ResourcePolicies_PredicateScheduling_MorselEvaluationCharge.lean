import TinylambOpt.ResearchGoals.ResourcePolicies.PredicateScheduling

namespace TinylambOpt.ResearchProofs.Batch5.ResourcePolicies.PredicateScheduling.Goal_MorselEvaluationCharge

open TinylambOpt.ResearchGoals.ResourcePolicies.PredicateScheduling


/-! Evaluation work is additive over disjoint morsels: both the per-row charge
and the recursively filtered rows split across an append. -/

namespace Scratch.MorselEvaluationCharge

open TinylambOpt.ResearchGoals.ResourcePolicies.PredicateScheduling

theorem sound : TinylambOpt.ResearchGoals.ResourcePolicies.PredicateScheduling.MorselEvaluationCharge.Goal := by
  intro ts
  induction ts with
  | nil => intro a b; simp [work]
  | cons t ts ih =>
      intro a b
      simp [work, List.filter_append, ih]
      omega

end Scratch.MorselEvaluationCharge
end TinylambOpt.ResearchProofs.Batch5.ResourcePolicies.PredicateScheduling.Goal_MorselEvaluationCharge
