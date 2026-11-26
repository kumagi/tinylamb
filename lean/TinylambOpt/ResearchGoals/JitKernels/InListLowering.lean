import Std

/-! Generated IN and NOT IN predicates distinguish NULL from a nonmatching value. A bitset uses a biased finite value range; a residual comparison handles out-of-domain entries. Empty lists model an empty IN subquery. -/
namespace TinylambOpt.ResearchGoals.JitKernels.InListLowering

abbrev Keys := List (Option Int)
def test (xs : Keys) (v : Option Int) : Option Bool :=
  if xs.isEmpty then some false else if v.isNone then none else
    if xs.contains v then some true else if xs.contains none then none else some false
def rangeKeys (xs : Keys) (lo hi : Int) := xs.filter (fun x => x.any (fun v => lo ≤ v && v < hi))
def disj : Option Bool → Option Bool → Option Bool
  | some true,_ | _,some true => some true
  | some false,b => b | a,_ => a
def keep (xs : Keys) (v : Option Int) := test xs v == some true

/-- Combine a hot bitset and a cold residual list with SQL three-valued OR, including empty partitions. -/
def InSetChunkedThreeValueOr.Goal : Prop := ∀ xs ys v, disj (test xs v) (test ys v) = test (xs++ys) v

/-- Drop NULL build entries entirely when the only observation is acceptance by WHERE IN. -/
def NullEntryFilterElision.Goal : Prop := ∀ xs v, keep (xs.filter Option.isSome) v = keep xs v

/-- The same NULL-elision also preserves acceptance of WHERE NOT IN for every probe. -/
def NullEntryAntiFilterElision.Goal : Prop := ∀ xs v, (test (xs.filter Option.isSome) v == some false) = (test xs v == some false)

/-- Implement the range check for a compact bitset by truncating a negative biased index to zero. -/
def BiasedBitsetOutlier.Goal : Prop := ∀ (xs : Keys) lo n x, 0 < n → (∀ v, some v ∈ xs → lo ≤ v ∧ v < lo+(n:Nat)) →
  (keep xs (some x) = keep xs (some (lo+(x-lo).toNat%n)))

/-- A positive hot bitset test short-circuits all remaining entries, including NULL entries that would affect misses. -/
def SkipColdResidualOnHotHit.Goal : Prop := ∀ xs lo hi v, keep (rangeKeys xs lo hi) v → test xs v = some true

/-- Resolve an exact negative lookup on a non-NULL key with one list-wide has-NULL flag. -/
def ConstantMissLateNullFlag.Goal : Prop := ∀ xs x, ¬ some x ∈ xs → test xs (some x) = (if xs.contains none then none else some false)

/-- Convert both predicates to WHERE acceptance before negating their SQL disjunction. -/
def CompositeAntiPredicateBooleanization.Goal : Prop := ∀ a b v, (disj (test a v) (test b v) == some false) = (!(keep a v) && !(keep b v))

/-- Replace a sorted non-NULL membership list by min/max when its distinct cardinality fills that closed interval. -/
def AdjacentIntegerIntervalReplacement.Goal : Prop := ∀ (vs : List Int) l h x, vs.min? = some l → vs.max? = some h →
  (vs.eraseDups.length:Int) = h-l+1 →
  keep (vs.map some) (some x) = decide (l ≤ x ∧ x ≤ h)

/-- A cached IN bitset may be shared by two lists with equal length, bounds and NULL presence. -/
def SameCardinalityConstantSetCache.Goal : Prop := ∀ (a b : Keys) v, a.length = b.length → (a.filterMap id).min? = (b.filterMap id).min? →
  (a.filterMap id).max? = (b.filterMap id).max? → a.contains none = b.contains none → test a v = test b v

end TinylambOpt.ResearchGoals.JitKernels.InListLowering
