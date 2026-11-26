import TinylambOpt.ResearchGoals.November26.Tpcds

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false

namespace TinylambOpt.ResearchProofs.November26.Tpcds.Goal_Q40
open TinylambOpt.ResearchGoals.November26

theorem forKey_filter_true {α β : Type} [BEq α] [LawfulBEq α] (rows : List (α × β))
    (p : α → Bool) (k : α) (hp : p k = true) :
    forKey (rows.filter (fun r => p r.1)) k = forKey rows k := by
  unfold forKey
  rw [List.filter_filter]
  congr 1
  apply List.filter_congr
  intro r hr
  by_cases h : r.1 == k
  · have hrk : r.1 = k := beq_iff_eq.mp h
    simp [h, hrk, hp]
  · simp [h]

theorem outer_filter_eq {α β : Type} [BEq α] [LawfulBEq α] (sold : List α) (rows : List (α × β))
    (p : α → Bool) (hp : ∀ k ∈ sold, p k = true) :
    sold.flatMap (fun k =>
      let vs := forKey (rows.filter (fun r => p r.1)) k
      if vs.isEmpty then [none] else vs.map some) =
    sold.flatMap (fun k =>
      let vs := forKey rows k
      if vs.isEmpty then [none] else vs.map some) := by
  induction sold with
  | nil => rfl
  | cons k ks ih =>
    simp only [List.flatMap_cons]
    rw [forKey_filter_true rows p k (hp k (by simp))]
    rw [ih (fun k' hk' => hp k' (by simp [hk']))]

theorem sound : TinylambOpt.ResearchGoals.November26.Tpcds.Q40.Goal := by
  intro sold returns
  dsimp
  exact outer_filter_eq sold returns (fun k => sold.contains k)
    (by intro k hk; simp [hk])
end TinylambOpt.ResearchProofs.November26.Tpcds.Goal_Q40
