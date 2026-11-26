import TinylambOpt.ResearchGoals.ResourcePolicies.SharedVocabulary

namespace TinylambOpt.ResearchProofs.Batch5.ResourcePolicies.SharedVocabulary.Goal_FrontCodePredecessorSufficiency

open TinylambOpt.ResearchGoals.ResourcePolicies.SharedVocabulary


/-! Scratch proof for the FrontCodePredecessorSufficiency resource-policy goal. It records the
relationship between dictionary ids and SQL-visible scan results. -/


open TinylambOpt.ResearchGoals.ResourcePolicies.SharedVocabulary

private theorem common_take (prev s : Symbol) :
    prev.take (common prev s) = s.take (common prev s) := by
  induction prev generalizing s with
  | nil => simp [common]
  | cons p ps ih =>
    cases s with
    | nil => simp [common]
    | cons x xs =>
      simp only [common]
      split
      · rename_i h
        have hpx : p = x := by simpa using h
        subst hpx
        rw [show 1 + common ps xs = common ps xs + 1 by omega, List.take_succ_cons,
          List.take_succ_cons, ih xs]
      · simp

theorem sound : TinylambOpt.ResearchGoals.ResourcePolicies.SharedVocabulary.FrontCodePredecessorSufficiency.Goal := by
  intro prev s
  simp only [expand, front]
  have hc := common_take prev s
  calc prev.take (common prev s) ++ s.drop (common prev s)
      = s.take (common prev s) ++ s.drop (common prev s) := by rw [hc]
    _ = s := List.take_append_drop (common prev s) s
end TinylambOpt.ResearchProofs.Batch5.ResourcePolicies.SharedVocabulary.Goal_FrontCodePredecessorSufficiency
