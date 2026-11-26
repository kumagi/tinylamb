import TinylambOpt.ResearchGoals.ResourcePolicies.SharedVocabulary

namespace TinylambOpt.ResearchProofs.Batch5.ResourcePolicies.SharedVocabulary.Goal_AbsentLiteralSkipsIdStream

open TinylambOpt.ResearchGoals.ResourcePolicies.SharedVocabulary


/-! Scratch proof for the AbsentLiteralSkipsIdStream resource-policy goal. It records the
relationship between dictionary ids and SQL-visible scan results. -/


open TinylambOpt.ResearchGoals.ResourcePolicies.SharedVocabulary

theorem sound : TinylambOpt.ResearchGoals.ResourcePolicies.SharedVocabulary.AbsentLiteralSkipsIdStream.Goal := by
  intro xs lit hnot
  rw [List.filter_eq_nil_iff]
  intro s hs
  simp only [beq_iff_eq]
  intro heq
  subst heq
  exact hnot (List.mem_eraseDups.mpr hs)
end TinylambOpt.ResearchProofs.Batch5.ResourcePolicies.SharedVocabulary.Goal_AbsentLiteralSkipsIdStream
