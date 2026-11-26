import TinylambOpt.ResearchGoals.BilateralEagerAggJoinPushdown

/-!
These models describe plan alternatives for analytical SQL, with row multiplicity and
aggregate state made explicit.

Sufficient state for group presence, COUNT(*), nullable SUM, and the numerator and
denominator of AVG. Arithmetic here is widened and exact.

The modeled inputs and results come from
`TinylambOpt.ResearchGoals.BilateralEagerAggJoinPushdown`.
-/

namespace TinylambOpt.ResearchGoals.AnalyticsWorkloads

/-- Sufficient state for group presence, COUNT(*), nullable SUM, and the numerator
and denominator of AVG. Arithmetic here is widened and exact. -/
abbrev Totals := BilateralEagerAggJoinPushdown.PreAgg
def zero : Totals := ⟨0,0,0⟩
def weighted (n : Nat) (v : Option Int) : Totals :=
  ⟨n, if v.isSome then n else 0, (n:Int) * v.getD 0⟩
def plus (a b : Totals) : Totals := ⟨a.cnt+b.cnt, a.nn+b.nn, a.sum+b.sum⟩
def summarize (vs : List (Option Int)) : Totals :=
  vs.foldl (fun a v => plus a (weighted 1 v)) zero

def histogram {α : Type} [BEq α] [Hashable α] (xs : List α) : Std.HashMap α Nat :=
  xs.foldl (fun m x => m.insert x (m.getD x 0 + 1)) {}

end TinylambOpt.ResearchGoals.AnalyticsWorkloads
