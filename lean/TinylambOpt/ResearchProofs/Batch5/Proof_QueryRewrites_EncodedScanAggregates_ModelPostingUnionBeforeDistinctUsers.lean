import TinylambOpt.ResearchProofs.QueryRewrites.KeyGrouping
import TinylambOpt.ResearchGoals.QueryRewrites.EncodedScanAggregates

namespace TinylambOpt.ResearchProofs.Batch5.QueryRewrites.EncodedScanAggregates.Goal_ModelPostingUnionBeforeDistinctUsers

open TinylambOpt.ResearchGoals.QueryRewrites.EncodedScanAggregates


/-!
Aliased model codes still select the same rows when the selection predicate
depends only on the decoded model. Grouping the fact rows by distinct codes
before taking distinct users therefore yields the same user set; SQL may push
the model filter into the code-to-posting lookup without double counting.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false

open TinylambOpt.ResearchProofs.QueryRewrites.KeyGrouping
open TinylambOpt.ResearchGoals.QueryRewrites

namespace EncodedScanAggregateModelPostings

private theorem eraseDups_perm {α : Type} [BEq α] [LawfulBEq α] {l₁ l₂ : List α}
    (p : l₁.Perm l₂) : l₁.eraseDups.Perm l₂.eraseDups := by
  apply List.perm_iff_count.mpr
  intro x
  have hn₁ := nodup_eraseDups l₁
  have hn₂ := nodup_eraseDups l₂
  rw [hn₁.count, hn₂.count]
  have hmem : x ∈ l₁.eraseDups ↔ x ∈ l₂.eraseDups := by
    simp [p.mem_iff]
  by_cases h : x ∈ l₁.eraseDups
  · simp [h, hmem.mp h]
  · have h' : x ∉ l₂.eraseDups := by
      intro hx
      exact h (hmem.mpr hx)
    simp [h, h']

end EncodedScanAggregateModelPostings

open EncodedScanAggregateModelPostings
open TinylambOpt.ResearchProofs.QueryRewrites.KeyGrouping

theorem sound : TinylambOpt.ResearchGoals.QueryRewrites.EncodedScanAggregates.ModelPostingUnionBeforeDistinctUsers.Goal := by
  intro rows model target
  dsimp only
  let p : Nat → Bool := fun c => model c == target
  have hperm := forKey_filter_perm (p := p) rows
  change
    ((((rows.map Prod.fst).eraseDups.filter p).flatMap (forKey rows)).eraseDups).Perm
      (((rows.filter (fun r => p r.1)).map Prod.snd).eraseDups)
  exact (eraseDups_perm hperm).symm
end TinylambOpt.ResearchProofs.Batch5.QueryRewrites.EncodedScanAggregates.Goal_ModelPostingUnionBeforeDistinctUsers
