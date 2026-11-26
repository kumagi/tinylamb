import TinylambOpt.ResearchGoals.QueryRewrites.EncodedScanAggregates

set_option linter.unusedSimpArgs false
set_option linter.deprecated false


/-!
# DeferredPayloadAcrossCountTies

Filtering the fact rows by a shortlist of composite keys preserves payload
order for every shortlisted key. This allows payload lookups to be deferred
until after the key shortlist is known, without changing per-key payloads.
-/

namespace TinylambOpt.ResearchProofs.QueryRewrites.EncodedScanAggregates.Goal_DeferredPayloadAcrossCountTies

open TinylambOpt.ResearchGoals.QueryRewrites

theorem sound : TinylambOpt.ResearchGoals.QueryRewrites.EncodedScanAggregates.DeferredPayloadAcrossCountTies.Goal := by
  intro rs keep k hk
  simp only [forKey, List.filter_filter, List.mem_filter, Bool.and_eq_true, decide_eq_true_eq]
  congr 1
  apply List.filter_congr
  intro r _
  by_cases hr : r.1 == k
  · have hkeq : r.1 = k := eq_of_beq hr
    subst hkeq
    simp [hk]
  · simp [hr]


end TinylambOpt.ResearchProofs.QueryRewrites.EncodedScanAggregates.Goal_DeferredPayloadAcrossCountTies
