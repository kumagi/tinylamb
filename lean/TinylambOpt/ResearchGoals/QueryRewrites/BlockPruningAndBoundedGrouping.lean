import TinylambOpt.ResearchGoals.AnalyticsWorkloads.Common

/-!
These claims cover skipping blocks with certified predicate summaries, using
string offsets to test emptiness, and bounding grouping work for LIMIT or
top-k. They specify when a SQL scan can avoid decoding a block or completing
groups that cannot affect the modeled output.
-/


namespace TinylambOpt.ResearchGoals.QueryRewrites.BlockPruningAndBoundedGrouping

open TinylambOpt.ResearchGoals.AnalyticsWorkloads (histogram)

/-! ## Wave 9: ClickBench High-Cardinality GROUP BY, Block Zone+Bloom Pruning, Empty-String Offset Filter

Targets:
- ClickBench Q37 (98.2ms vs 15.5ms) / Q38 (14.2ms vs 2.5ms) / Q20 (15.8ms vs 4.4ms): `BlockConjunctionZoneBitmapPrune`
- ClickBench Q18 (29.3ms vs CH 8.7ms): `GroupByLimitAnyOrderEarlyStop`
- ClickBench Q32 / Q37 / Q38 (`WHERE s <> ''`): `NonEmptyStringFilterBeforeDictDecode`
- ClickBench Q22 / Q28 / Q31 / Q32 (`GROUP BY ... ORDER BY COUNT(*) DESC LIMIT K`): `TopKCountPruneByRemainingRows`
-/

/-- ClickBench Q37 / Q38 / Q20. A block whose `[lo, hi]` zone map for ANY conjunct
is disjoint from the query range (or whose point-lookup Bloom bit is absent)
contains zero matching rows and can be skipped without decoding. -/
structure BlockZone where
  minA : Int
  maxA : Int
  minB : Int
  maxB : Int

def zoneMayMatch (z : BlockZone) (aEq loB hiB : Int) : Bool :=
  z.minA ≤ aEq && aEq ≤ z.maxA && !(z.maxB < loB || hiB < z.minB)

def BlockConjunctionZoneBitmapPrune.Goal : Prop :=
  ∀ (rows : List (Int × Int)) (z : BlockZone) (aEq loB hiB : Int),
    (∀ r ∈ rows, z.minA ≤ r.1 ∧ r.1 ≤ z.maxA ∧ z.minB ≤ r.2 ∧ r.2 ≤ z.maxB) →
    zoneMayMatch z aEq loB hiB = false →
    rows.filter (fun r => r.1 == aEq && loB ≤ r.2 && r.2 ≤ hiB) = []

/-- ClickBench Q18. For `SELECT k, COUNT(*) FROM t GROUP BY k LIMIT L` without
`ORDER BY` or `HAVING`, freezing insertion once `L` distinct keys are collected
and only incrementing counts of the `L` frozen keys returns an exact count for
every emitted key and at most `L` keys. -/
def frozenGroupCount (rows : List Nat) (limit : Nat) : Std.HashMap Nat Nat :=
  rows.foldl (fun h k =>
    match h[k]? with
    | some c => h.insert k (c + 1)
    | none => if h.size < limit then h.insert k 1 else h) {}

def GroupByLimitAnyOrderEarlyStop.Goal : Prop :=
  ∀ (rows : List Nat) (limit k : Nat),
    let out := frozenGroupCount rows limit
    out.size ≤ limit ∧
      (∀ c, out[k]? = some c → c = rows.count k ∧ 0 < c)

/-- ClickBench Q32 / Q37 / Q38. Checking `offsets[i+1] > offsets[i]` (with `!isNull`)
is equivalent to `s <> ''` without touching payload bytes. -/
def offsetNonEmpty (s : Option String) : Bool :=
  match s with
  | none => false
  | some txt => 0 < txt.length

def NonEmptyStringFilterBeforeDictDecode.Goal : Prop :=
  ∀ (s : Option String),
    offsetNonEmpty s = (match s with | none => false | some txt => txt != "")

/-- ClickBench Q22 / Q28 / Q31 / Q32. Once the `K`-th largest count `cK` strictly
exceeds the remaining unread rows `rem`, no unseen key can ever surpass `cK`, so
skipping insertions of unseen keys preserves the exact count of every key whose
final count is `> rem`. -/
def prunedTailCount (prefixRows suffixRows : List Nat) (cK : Nat) : Std.HashMap Nat Nat :=
  let base := histogram prefixRows
  let rem := suffixRows.length
  if cK > rem then
    suffixRows.foldl (fun h k =>
      match h[k]? with
      | some c => h.insert k (c + 1)
      | none => h) base
  else
    suffixRows.foldl (fun h k => h.insert k (h.getD k 0 + 1)) base

def TopKCountPruneByRemainingRows.Goal : Prop :=
  ∀ (pref suff : List Nat) (cK k : Nat),
    let full := histogram (pref ++ suff)
    let opt := prunedTailCount pref suff cK
    full.getD k 0 > suff.length → opt.getD k 0 = full.getD k 0

#guard zoneMayMatch ⟨10, 20, 100, 200⟩ 62 150 160 == false
#guard (frozenGroupCount [1, 2, 1, 3, 2, 1] 2).getD 1 0 == 3
#guard offsetNonEmpty (some "") == false && offsetNonEmpty (some "a") == true && offsetNonEmpty none == false
#guard (prunedTailCount [1, 1, 1, 2, 2] [1, 3] 2).getD 1 0 == 4

end TinylambOpt.ResearchGoals.QueryRewrites.BlockPruningAndBoundedGrouping
