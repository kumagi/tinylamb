import TinylambOpt.ResearchGoals.ResourcePolicies.SharedVocabulary

namespace TinylambOpt.ResearchProofs.ResourcePolicies.SharedVocabulary.DictionaryInsertLocalReprice

open TinylambOpt.ResearchGoals.ResourcePolicies.SharedVocabulary

theorem sound : DictionaryInsertLocalReprice.Goal := by
  intro prev entry next tail
  simp only [chain]
  omega

end TinylambOpt.ResearchProofs.ResourcePolicies.SharedVocabulary.DictionaryInsertLocalReprice
