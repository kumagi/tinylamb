import TinylambOpt.ResearchGoals.QueryRewrites.Common
import TinylambOpt.ResearchGoals.ResourcePolicies.AggregateReplay

namespace TinylambOpt.ResearchProofs.ResourcePolicies.AggregateReplay.Goal_ValueDeduplicatedPartials

open TinylambOpt.ResearchGoals.ResourcePolicies.AggregateReplay


/-! Scratch proof: deduplicating partials by value rather than identity loses
    independent contributions of distinct retry records. -/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

open TinylambOpt.ResearchGoals.QueryRewrites
open TinylambOpt.ResearchGoals.ResourcePolicies.AggregateReplay

/-- Two records with different identities but equal key and equal partial state
    are legal (their ids are distinct). Collapsing them by value halves SUM. -/
theorem refute : ¬ TinylambOpt.ResearchGoals.ResourcePolicies.AggregateReplay.ValueDeduplicatedPartials.Goal := by
  intro h
  have hbad := h [{ id := 0, key := 0, partialState := (1, 1) },
                  { id := 1, key := 0, partialState := (1, 1) }] 0 (by decide)
  exact (by decide : ¬ replay (List.eraseDupsBy (fun a b : Record => a.key == b.key && a.partialState == b.partialState)
      [{ id := 0, key := 0, partialState := (1, 1) }, { id := 1, key := 0, partialState := (1, 1) }]) 0 =
    replay [{ id := 0, key := 0, partialState := (1, 1) },
            { id := 1, key := 0, partialState := (1, 1) }] 0) hbad
end TinylambOpt.ResearchProofs.ResourcePolicies.AggregateReplay.Goal_ValueDeduplicatedPartials
