import TinylambOpt.ResearchGoals.QueryRewrites.Common

/-! Single-key ROLLUP with COUNT(*) and nullable exact SUM. Distinguish a
detail group whose SQL key is NULL from the synthetic NULL on a subtotal.
The group-level tag is preserved until GROUPING and HAVING have run. -/
namespace TinylambOpt.ResearchGoals.DatabaseCorrectness.GroupingOutput
open TinylambOpt.ResearchGoals.QueryRewrites (nsum forKey)

abbrev Key := Option Nat
abbrev Input := List (Key × Option Int)
structure Out where
  rolled : Bool
  key : Key
  count : Nat
  total : Option Int
  deriving DecidableEq, Repr
def detail (rs : Input) (k : Key) : Out :=
  let vs := forKey rs k
  ⟨false,k,vs.length,nsum vs⟩
def grand (rs : Input) : Out := ⟨true,none,rs.length,nsum (rs.map Prod.snd)⟩
def rollup (rs : Input) := (rs.map Prod.fst).eraseDups.map (detail rs) ++ [grand rs]

/-- An empty completed join still emits one row for the empty grouping set; HAVING runs on that row. -/
def EmptyJoinRollupHaving.Goal : Prop := ∀ (having : Out → Bool),
  (rollup []).filter having = if having ⟨true,none,0,none⟩ then [⟨true,none,0,none⟩] else []

/-- Projected NULLs from real input and from rollup may occur twice; GROUPING must distinguish them. -/
def NullDetailAndSubtotalDistinct.Goal : Prop := ∀ rs, none ∈ rs.map Prod.fst →
  ((rollup rs).filter (fun r => r.key.isNone)).map Out.rolled = [false,true]

/-- All-NULL measures give a nonzero COUNT(*) and a NULL SUM; empty partials cannot be counted as rows. -/
def AllNullMeasuresRetainGroupMultiplicity.Goal : Prop := ∀ rs k, (forKey rs k).all Option.isNone →
  (detail rs k).total = none ∧ (detail rs k).count = (rs.map Prod.fst).count k

/-- Combining partial grand totals handles empty and all-NULL partitions without synthesizing a zero SUM. -/
def GrandTotalAcrossSpillPartitions.Goal : Prop := ∀ (parts : List Input),
  grand parts.flatten = ⟨true,none,(parts.map (fun p => (grand p).count)).sum,
    nsum (parts.map (fun p => (grand p).total))⟩

/-- The full set of detail groups accounts for the exact bag counted by the subtotal, including NULL keys. -/
def DetailCountsAgreeWithSubtotal.Goal : Prop := ∀ rs,
  (((rollup rs).filter (fun r => !r.rolled)).map Out.count).sum = (grand rs).count

/-- Applying a HAVING predicate to the final aggregate preserves each qualifying grouping set separately. -/
def HavingDoesNotCoalesceNullGroups.Goal : Prop := ∀ rs (pass : Out → Bool),
  (((rollup rs).filter pass).countP (fun r => r.key.isNone)) =
    (if none ∈ rs.map Prod.fst ∧ pass (detail rs none) then 1 else 0) + (if pass (grand rs) then 1 else 0)

end TinylambOpt.ResearchGoals.DatabaseCorrectness.GroupingOutput
