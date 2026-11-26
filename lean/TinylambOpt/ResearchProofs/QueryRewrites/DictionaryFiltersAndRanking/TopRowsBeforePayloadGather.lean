import TinylambOpt.ResearchGoals.QueryRewrites.DictionaryFiltersAndRanking

/-!
# TopRowsBeforePayloadGather

The top ten row IDs selected using the full ranking key determine the same output when
payload columns are gathered afterward. The executor can defer wide-row materialization.
-/


set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.QueryRewrites.DictionaryFiltersAndRanking.TopRowsBeforePayloadGather
open Std
open TinylambOpt.ResearchGoals.QueryRewrites
open TinylambOpt.ResearchGoals.QueryRewrites.DictionaryFiltersAndRanking

theorem sound : TinylambOpt.ResearchGoals.QueryRewrites.DictionaryFiltersAndRanking.TopRowsBeforePayloadGather.Goal := by
  intro ids key payload
  let cmp : Nat → Nat → Bool := fun a b => decide (key a ≤ key b)
  let pair : Nat → Nat × String := fun i => (i, payload i)
  have hsort : (ids.map pair).mergeSort (fun p q => cmp p.1 q.1) = (ids.mergeSort cmp).map pair := by
    symm
    exact List.map_mergeSort (r := cmp) (s := fun p q : Nat × String => cmp p.1 q.1)
      (f := pair) (l := ids) (by
        intro a ha b hb
        rfl)
  change ((ids.mergeSort cmp).take 10).map payload = (((ids.map pair).mergeSort (fun p q => cmp p.1 q.1)).take 10).map Prod.snd
  rw [hsort]
  rw [← List.map_take]
  simp [pair, Function.comp_def]
end TinylambOpt.ResearchProofs.QueryRewrites.DictionaryFiltersAndRanking.TopRowsBeforePayloadGather
