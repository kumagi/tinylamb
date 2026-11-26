import TinylambOpt.ResearchGoals.ResourcePolicies.SharedVocabulary

namespace TinylambOpt.ResearchProofs.Batch5.ResourcePolicies.SharedVocabulary.Goal_DictionaryInsertLocalReprice

open TinylambOpt.ResearchGoals.ResourcePolicies.SharedVocabulary


/-! Scratch proof for the DictionaryInsertLocalReprice resource-policy goal. It records the
relationship between dictionary ids and SQL-visible scan results. -/


open TinylambOpt.ResearchGoals.ResourcePolicies.SharedVocabulary

theorem sound : TinylambOpt.ResearchGoals.ResourcePolicies.SharedVocabulary.DictionaryInsertLocalReprice.Goal := by
  intro prev entry next tail
  simp only [chain]
  omega
end TinylambOpt.ResearchProofs.Batch5.ResourcePolicies.SharedVocabulary.Goal_DictionaryInsertLocalReprice
