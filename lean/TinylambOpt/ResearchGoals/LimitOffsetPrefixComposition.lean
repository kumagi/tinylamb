import Std

namespace TinylambOpt.ResearchGoals.LimitOffsetPrefixComposition

def limitOp {α : Type} (lim off : Nat) (xs : List α) : List α :=
  (xs.drop off).take lim

/-- Limit/TopN prefix-offset algebra (`plan/limit_plan.hpp` `prior_offset_`,
    `plan/topn_plan.hpp`, `plan/cascades.cpp` `window_halo_topn` /
    `topn_push_through_projection`): when an inner operator already skipped `p ≤ O`
    prefix rows and retained at least `m ≥ (O - p) + L` rows, an outer
    `limitOp L (O - p)` recovers the exact slice `limitOp L O` (and commutes with
    any row-wise projection `f`). -/
def Goal : Prop :=
  ∀ {α β : Type} (xs : List α) (f : α → β) (L O p m : Nat),
    p ≤ O → (O - p) + L ≤ m →
    limitOp L (O - p) (limitOp m p xs) = limitOp L O xs ∧
    limitOp L (O - p) ((limitOp m p xs).map f) = (limitOp L O xs).map f

example :
    let xs := List.range 300
    limitOp 20 0 (limitOp 220 100 xs) = limitOp 20 100 xs ∧
    limitOp 20 0 (limitOp 220 100 xs) = List.range' 100 20 := by
  decide

example :
    let xs := List.range 300
    let inner := limitOp 220 100 xs
    let outerFixed := limitOp 20 0 inner
    limitOp 20 100 outerFixed = [] := by
  decide

example :
    let xs := List.range 200
    limitOp 15 (100 - 40) ((limitOp 75 40 xs).map (· + 10)) =
      (limitOp 15 100 xs).map (· + 10) := by
  decide

end TinylambOpt.ResearchGoals.LimitOffsetPrefixComposition
