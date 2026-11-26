import TinylambOpt.ResearchGoals.QueryRewrites.SalesAndReturns

/-!
# MergeShippingDelayHistograms

Adding local delay histograms by warehouse, shipping method, and call center preserves
their modeled bucket counts. The executor can merge partial states instead of sending
each row to the global aggregate.
-/


set_option maxHeartbeats 0
namespace TinylambOpt.ResearchProofs.QueryRewrites.SalesAndReturns.MergeShippingDelayHistograms
open TinylambOpt.ResearchGoals.QueryRewrites
open TinylambOpt.ResearchGoals.QueryRewrites.SalesAndReturns

theorem sound : TinylambOpt.ResearchGoals.QueryRewrites.SalesAndReturns.MergeShippingDelayHistograms.Goal := by
  intro parts groupKey
  dsimp only
  let classify : List (Nat × Option Int) → List Nat := fun p =>
    ((p.filter (fun r => r.1 == groupKey)).filterMap Prod.snd).map (bucket [30,60,90,120])
  have happ : ∀ p q : List (Nat × Option Int), classify (p ++ q) = classify p ++ classify q := by
    intro p q
    simp [classify, List.filter_append, List.filterMap_append]
  have hflat : classify parts.flatten = (parts.map classify).flatten := by
    induction parts with
    | nil => simp [classify]
    | cons p ps ih =>
        change classify (p ++ ps.flatten) = (classify p :: ps.map classify).flatten
        simp [happ, ih]
  have hcount : ∀ (c : Nat) (ps : List (List Nat)),
      ps.flatten.count c = (ps.map (fun p => p.count c)).sum := by
    intro c ps
    induction ps with
    | nil => simp
    | cons p ps ih =>
        simp [List.count_append, ih]
  change (List.range 5).map (fun c => (parts.map (fun p => (classify p).count c)).sum) =
    (List.range 5).map (classify parts.flatten).count
  rw [hflat]
  apply List.map_congr_left
  intro c hc
  have hh := hcount c (parts.map classify)
  simp only [List.map_map, Function.comp_apply] at hh
  exact hh.symm

end TinylambOpt.ResearchProofs.QueryRewrites.SalesAndReturns.MergeShippingDelayHistograms

#print axioms TinylambOpt.ResearchProofs.QueryRewrites.SalesAndReturns.MergeShippingDelayHistograms.sound

-- C++ references to this model or its proof.
-- executor/detail/benchmark_rematch.hpp
