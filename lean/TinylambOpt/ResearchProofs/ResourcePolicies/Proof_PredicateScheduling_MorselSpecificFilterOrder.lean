import TinylambOpt.ResearchGoals.ResourcePolicies.PredicateScheduling

namespace TinylambOpt.ResearchProofs.ResourcePolicies.PredicateScheduling.Goal_MorselSpecificFilterOrder

open TinylambOpt.ResearchGoals.ResourcePolicies.PredicateScheduling


/-! Conjunction of pure row predicates is commutative and associative, so each
morsel may use its own equivalent filter order without changing survivors. -/

namespace Scratch.MorselSpecificFilterOrder

open TinylambOpt.ResearchGoals.ResourcePolicies.PredicateScheduling

theorem perm_all_keep : ∀ {as ts : List Test} (_h : as.Perm ts) (r : Nat),
    as.all (fun t => t.keep r) = ts.all (fun t => t.keep r) := by
  intro as ts h
  induction h with
  | nil => intro r; simp
  | cons x h ih => intro r; simp [ih r]
  | swap x y xs => intro r; simp [Bool.and_left_comm]
  | trans h1 h2 ih1 ih2 => intro r; rw [ih1 r, ih2 r]

theorem filter_perm : ∀ (as ts : List Test) (rows : List Nat), as.Perm ts →
    rows.filter (fun r => as.all (fun t => t.keep r)) =
      rows.filter (fun r => ts.all (fun t => t.keep r)) := by
  intro as ts rows h
  induction rows with
  | nil => simp
  | cons x xs ih =>
      simp [List.filter, perm_all_keep h x, ih]

theorem sound : TinylambOpt.ResearchGoals.ResourcePolicies.PredicateScheduling.MorselSpecificFilterOrder.Goal := by
  intro ts as bs a b ha hb
  simp [survivors, List.filter_append, filter_perm as ts a ha, filter_perm bs ts b hb]

end Scratch.MorselSpecificFilterOrder
end TinylambOpt.ResearchProofs.ResourcePolicies.PredicateScheduling.Goal_MorselSpecificFilterOrder
