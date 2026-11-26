import TinylambOpt.ResearchGoals.QueryRewrites.Common

/-! These goals concern support sets, ranking and window partitions. They model bag multiplicities, SQL equality of non-null join keys, and NULL-aware aggregate inputs separately. -/

namespace TinylambOpt.ResearchGoals.QueryRewrites.SparseSetAndWindowEvaluation

/-- Within a canonical name group and a bounded date dictionary, a three-way day-mask population equals the distinct intersection cardinality. -/
def NameDayBitmapTriplePopulation.Goal : Prop := ∀ (s c w : List Nat) n, (∀ d ∈ s, d < n) →
  countWhere (List.range n) (fun d => s.contains d && c.contains d && w.contains d) =
    (members (members s c) w).length

/-- Restricting expensive inner item classification to requested non-null manufacturer names preserves existential witnesses for every outer occurrence. -/
def DemandedManufacturerWitnessCache.Goal : Prop := ∀ (wanted : List (Option String)) (items : List (Option String × Nat)) (admit : Nat → Bool),
  let demanded := items.filter (fun r => wanted.any (hit r.1))
  let mark := fun (rs : List (Option String × Nat)) k => rs.any (fun r => hit k r.1 && admit r.2)
  wanted.map (mark demanded) = wanted.map (mark items)

/-- Unioning two independently built row-position lists by index preserves repeated equal fact values and counts a fact satisfying both arms once. -/
def DisjointPostalAndItemFactSelections.Goal : Prop := ∀ (facts : List (Option Int)) (postal item : Nat → Bool),
  let ids := List.range facts.length
  let a := ids.filter postal
  let b := ids.filter item
  (valuesAt facts (a ++ b.filter (!a.contains ·))).Perm
    (valuesAt facts (ids.filter (fun i => postal i || item i)))

/-- In a rank partition, a page strictly below a probe contributes its full size; an equal page contributes zero because SQL RANK counts strictly smaller keys. -/
def PeerRankBoundsFromPartitionExtrema.Goal : Prop := ∀ (pages : List (List Nat)) probe,
  let smaller := fun (p : List Nat) => if p.max?.any (· < probe) then p.length
    else if p.min?.any (probe ≤ ·) then 0 else countWhere p (· < probe)
  1+(pages.map smaller).sum = 1+countWhere pages.flatten (· < probe)

/-- For nonempty peer runs, competition ranks increase by the previous run length. Only singleton predecessor runs admit a rank-minus-one neighbor. -/
def PeerRunNeighborJoinCardinality.Goal : Prop := ∀ (before : List Nat) (prev curr next : Nat), 0 < prev → 0 < curr → 0 < next →
  let ranks := ((before++[prev,curr,next]).scanl (·+·) 1).take (before.length+3)
  let center := 1+before.sum+prev
  (∀ n ∈ before, 0 < n) →
  (ranks.contains (center-1) && ranks.contains (center+1)) = (prev == 1 && curr == 1)

/-- A failed reciprocal band between two channel totals can suppress all third-channel probes without changing the bag of accepted triples. -/
def RevenueBandEarlyPairFilter.Goal : Prop := ∀ (pairs : List (Nat × Nat)) (third : List Nat),
  let band := fun (a b : Nat) => decide (9*b ≤ 10*a ∧ 10*a ≤ 11*b ∧ 9*a ≤ 10*b ∧ 10*b ≤ 11*a)
  let complete := fun (p : Nat × Nat) => third.filter (fun z => band p.1 z && band p.2 z)
  (pairs.filter (fun p => band p.1 p.2)).flatMap complete =
    pairs.flatMap (fun p => third.filter (fun z => band p.1 p.2 && band p.1 z && band p.2 z))

/-- A bounded left week partition needs right-side sales only from the corresponding shifted interval, preserving all matches at inclusive boundaries. -/
def ShiftedStoreWeekIndexWithBoundaryHalo.Goal : Prop := ∀ (left right : List Nat) lo hi, (∀ k ∈ left, lo ≤ k ∧ k ≤ hi) →
  let halo := right.filter (fun w => lo+52 ≤ w && w ≤ hi+52)
  left.map (fun k => halo.count (k+52)) = left.map (fun k => right.count (k+52))

/-- A constant observed monthly total equals its average exactly; a positive-mean deviation filter cannot retain any such month. -/
def ManagerMonthlyEqualTotalsFastReject.Goal : Prop := ∀ (months : List (Option Int)) v, (∀ x ∈ months.filterMap id, x = v) →
  let xs := months.filterMap id
  xs.filter (fun x => decide (0 < xs.sum ∧ 10*(x*(xs.length:Int)-xs.sum).natAbs > xs.sum)) = []

/-- A certified upper bound on unread prior-year net payments can disqualify a customer before reading second-year measures. -/
def ZeroPriorSalesGroupEarlyAbandon.Goal : Prop := ∀ (seen pending : List Int) cap, (∀ x ∈ pending, x ≤ cap) →
  seen.sum + (pending.length:Int)*cap ≤ 0 → ¬ 0 < (seen++pending).sum

/-- Sorting non-null customer subtotals within a state makes a strict average threshold an exact suffix, including tied boundary amounts. -/
def ReturnStateThresholdSortedSuffix.Goal : Prop := ∀ (xs : List Int), xs.Pairwise (· ≤ ·) →
  let weight : Int := 5*xs.length
  xs.dropWhile (fun v => weight*v ≤ 6*xs.sum) = xs.filter (fun v => 6*xs.sum < weight*v)

/-- A block-local date dictionary can reject a returns page if it contains no selected date, even when the selected weeks overlap. -/
def CalendarBlockDateWitnessSemijoin.Goal : Prop := ∀ (pages : List (List Nat)) (days : List Nat),
  (pages.filter (fun p => !(members p days).isEmpty)).flatMap (fun p => p.filter days.contains) =
    pages.flatten.filter days.contains

/-- For a fixed category, merging leaf sums by class preserves the nullable subtotal, including a real null class treated as its own key. -/
def RollupParentTotalsFromLeafPartitions.Goal : Prop := ∀ (leaves : List (Option Nat × Option Int)),
  nsum ((leaves.map Prod.fst).eraseDups.map (fun c => nsum (forKey leaves c))) = nsum (leaves.map Prod.snd)

/-- A no-false-negative membership filter lets definite misses bypass both exclusion tables without changing EXCEPT DISTINCT output. -/
def NegativeFilterShortcutForSetDifference.Goal : Prop := ∀ (s c w : List Nat) (maybeSeen : Nat → Bool),
  (∀ k ∈ c++w, maybeSeen k = true) →
  s.eraseDups.filter (fun k => !maybeSeen k || !(c.contains k || w.contains k)) =
    s.eraseDups.filter (fun k => !(c++w).contains k)

/-- Restricting both time windows to the same eligible household/page rows preserves both counts, including repeated sales. -/
def SharedDimensionFilterBeforeTimeSplit.Goal : Prop := ∀ (facts : List (Nat × Bool)) (hour : Nat → Nat),
  let eligible := (facts.filter Prod.snd).map Prod.fst
  let inBand := fun low k => decide (low ≤ hour k ∧ hour k ≤ low+1)
  [8,19].map (fun lo => countWhere eligible (inBand lo)) =
    [8,19].map (fun lo => countWhere facts (fun (k,ok) => ok && inBand lo k))

end TinylambOpt.ResearchGoals.QueryRewrites.SparseSetAndWindowEvaluation
