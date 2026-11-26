import TinylambOpt.ResearchGoals.ResourcePolicies.SharedVocabulary

namespace TinylambOpt.ResearchProofs.ResourcePolicies.SharedVocabulary.AbsentLiteralSkipsIdStream

open TinylambOpt.ResearchGoals.ResourcePolicies.SharedVocabulary

theorem sound : AbsentLiteralSkipsIdStream.Goal := by
  intro xs lit hnot
  rw [List.filter_eq_nil_iff]
  intro s hs
  simp only [beq_iff_eq]
  intro heq
  subst heq
  exact hnot (List.mem_eraseDups.mpr hs)

end TinylambOpt.ResearchProofs.ResourcePolicies.SharedVocabulary.AbsentLiteralSkipsIdStream
