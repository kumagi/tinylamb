import TinylambOpt.ResearchGoals.ResourcePolicies.SharedVocabulary

namespace TinylambOpt.ResearchProofs.Batch5.ResourcePolicies.SharedVocabulary.Goal_OverlapPaysWiderIds

open TinylambOpt.ResearchGoals.ResourcePolicies.SharedVocabulary


/-! Scratch counterexample for the OverlapPaysWiderIds goal. Ten identical rows
share a one-byte dictionary entry; adding a second group that reuses that entry
widens every row id, so dictionary overlap alone does not imply a disk saving.
The counterexample preserves the SQL-visible rows while increasing frame bytes. -/

open TinylambOpt.ResearchGoals.ResourcePolicies.SharedVocabulary

theorem refute : ¬ TinylambOpt.ResearchGoals.ResourcePolicies.SharedVocabulary.OverlapPaysWiderIds.Goal := by
  intro h
  have hov :
      (vocab (List.replicate 10 [97])).any
          (fun s => (vocab ([[97], [98], [99]] : List Symbol)).contains s) = true := by
    decide
  have hc := h (List.replicate 10 [97]) [[97], [98], [99]] hov
  have hnot :
      ¬ charge (List.replicate 10 [97] ++ [[97], [98], [99]]) ≤
          charge (List.replicate 10 [97]) + charge [[97], [98], [99]] := by
    decide
  exact hnot hc
end TinylambOpt.ResearchProofs.Batch5.ResourcePolicies.SharedVocabulary.Goal_OverlapPaysWiderIds
