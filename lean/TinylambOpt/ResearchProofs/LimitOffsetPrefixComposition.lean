import TinylambOpt.ResearchGoals.LimitOffsetPrefixComposition

/-!
Proved

- `Goal` — Limit/TopN prefix-offset algebra (`plan/limit_plan.hpp` `prior_offset_`,
  `plan/topn_plan.hpp`, `plan/cascades.cpp` `window_halo_topn` /
  `topn_push_through_projection`): when an inner operator already skipped `p ≤ O`
  prefix rows and retained at least `m ≥ (O - p) + L` rows, an outer `limitOp L (O -
  p)` recovers the exact slice `limitOp L O` (and commutes with any row-wise
  projection `f`).

The reference model is defined in
`TinylambOpt.ResearchGoals.LimitOffsetPrefixComposition`.
-/

namespace TinylambOpt.ResearchProofs.LimitOffsetPrefixComposition

open TinylambOpt.ResearchGoals.LimitOffsetPrefixComposition

/-!
# Limit/TopN prefix-offset composition

An inner operator skipped `p ≤ O` prefix rows and kept at least
`(O - p) + L ≤ m` rows. The outer window `limitOp L (O - p)` applied to the
inner result re-derives exactly `limitOp L O`, and the identity commutes
with any row-wise projection. Unfolding `limitOp`, this is pure
`List.take`/`List.drop` arithmetic: `List.drop_take` turns the dropped
prefix of a take into a shortened take, `List.take_take` collapses the two
takes with a `Nat.min` that `omega` resolves, and `List.drop_drop` fuses
the two offsets back to `O`. Projection commutation is `List.map_take` /
`List.map_drop`.
-/

/-- Core window composition: with `p ≤ O` and `(O - p) + L ≤ m`,
`((xs.drop p).take m).drop (O - p)` followed by `.take L` equals
`(xs.drop O).take L`. -/
private theorem limitOp_comp {α : Type} (xs : List α) (L O p m : Nat)
    (hpo : p ≤ O) (hml : (O - p) + L ≤ m) :
    limitOp L (O - p) (limitOp m p xs) = limitOp L O xs := by
  have hop : p + (O - p) = O := by omega
  have hmin : min L (m - (O - p)) = L := Nat.min_eq_left (by omega)
  simp only [limitOp]
  rw [List.drop_take, List.take_take, hmin, List.drop_drop, hop]

/-- Projection commutes with one `limitOp` window. -/
private theorem limitOp_map {α β : Type} (xs : List α) (f : α → β) (lim off : Nat) :
    (limitOp lim off xs).map f = limitOp lim off (xs.map f) := by
  simp only [limitOp, List.map_take, List.map_drop]

theorem sound : TinylambOpt.ResearchGoals.LimitOffsetPrefixComposition.Goal := by
  intro α β xs f L O p m hpo hml
  constructor
  · exact limitOp_comp xs L O p m hpo hml
  · rw [limitOp_map, limitOp_comp (xs.map f) L O p m hpo hml, ← limitOp_map]

end TinylambOpt.ResearchProofs.LimitOffsetPrefixComposition
