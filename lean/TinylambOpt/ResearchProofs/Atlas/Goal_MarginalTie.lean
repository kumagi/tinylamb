import TinylambOpt.ResearchGoals.Atlas.PlanSearch

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_MarginalTie

open TinylambOpt.ResearchGoals.Atlas.PlanSearch.PredicateOrder

/-- `issues/research/atlas.md` の「具体的な反例を残した 10 件」より。
  相異なる二述語 `x == 0` と `x == 1` は `[0,1]` を同じ 1 行ずつ通すが、
  先行ゲート `x == 0` が 0 行だけを残すと、後ろの二述語の順序で
  評価費用が 4 と 3 に分かれる。 -/
theorem disproof_MarginalTie : ¬ MarginalTie.Goal := by
  intro h
  have hcnt :
      [0, 1].countP (fun x : Nat => x == 0) =
        [0, 1].countP (fun x : Nat => x == 1) := by
    native_decide
  have heq :=
    h [0, 1]
      (fun x : Nat => x == 0) (fun x : Nat => x == 1)
      (fun x : Nat => x == 0) hcnt
  have h4 :
      actual
          [(1, fun x : Nat => x == 0), (1, fun x : Nat => x == 0),
            (1, fun x : Nat => x == 1)]
          [0, 1] = 4 := by
    native_decide
  have h3 :
      actual
          [(1, fun x : Nat => x == 0), (1, fun x : Nat => x == 1),
            (1, fun x : Nat => x == 0)]
          [0, 1] = 3 := by
    native_decide
  rw [h4, h3] at heq
  omega

end TinylambOpt.ResearchProofs.Atlas.Goal_MarginalTie
