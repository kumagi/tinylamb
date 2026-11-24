import TinylambOpt.ResearchGoals.AnalyticsWorkloads.Common

namespace TinylambOpt.ResearchGoals.Wave9.TpcdsMultiChannelAndTpch

open TinylambOpt.ResearchGoals.AnalyticsWorkloads (Totals zero weighted plus summarize histogram)

/-! ## Wave 9: Multi-Channel Year-Pivot Self-Join Elimination, Two-Pass Group Avg, Semijoin Chain

Targets:
- TPC-DS Q04 / Q11 / Q74 / Q75 (325/217/205/248ms): `YearPairPivotSelfJoinElim`
- TPC-DS Q65 (146.5ms vs DuckDB 55.4ms): `GroupAvgThresholdTwoPassSplit`
- TPC-H Q08 (131.4ms vs DuckDB 52.3ms): `SelectiveDimSemijoinFilterChain`
- TPC-DS Q39 (128.6ms vs DuckDB 57.5ms): `CteVarianceCoeffSelfJoinPivot`
-/

/-- TPC-DS Q04 / Q11 / Q74 / Q75. Replace a 2-year CTE self-join
(`t1.id = t2.id AND t1.yr = y AND t2.yr = y+1 AND t1.total > 0`) with a single
conditional-aggregation pass maintaining `(y1, y2)` Totals per `id`. -/
abbrev YrRow := Nat × Int × Option Int -- (id, year, amount)

def pivotYears (rows : List YrRow) (y : Int) (id : Nat) : Option (Totals × Totals) :=
  let m : Std.HashMap Nat (Totals × Totals) := rows.foldl (fun h (k, yr, v) =>
    if yr == y then
      let (a, b) := h.getD k (zero, zero)
      h.insert k (plus a (weighted 1 v), b)
    else if yr == y + 1 then
      let (a, b) := h.getD k (zero, zero)
      h.insert k (a, plus b (weighted 1 v))
    else h) {}
  match m[id]? with
  | some (a, b) => if a.cnt > 0 && b.cnt > 0 && a.nn > 0 && a.sum > 0 then some (a, b) else none
  | none => none

def refYearSelfJoin (rows : List YrRow) (y : Int) (id : Nat) : Option (Totals × Totals) :=
  let a := summarize ((rows.filter fun (k, yr, _) => k == id && yr == y).map fun r => r.2.2)
  let b := summarize ((rows.filter fun (k, yr, _) => k == id && yr == y + 1).map fun r => r.2.2)
  if a.cnt > 0 && b.cnt > 0 && a.nn > 0 && a.sum > 0 then some (a, b) else none

def YearPairPivotSelfJoinElim.Goal : Prop :=
  ∀ (rows : List YrRow) (y : Int) (id : Nat),
    pivotYears rows y id = refYearSelfJoin rows y id

/-- TPC-DS Q65. Compute `(store, item) -> sum(rev)` once, then derive each store's
mean item revenue from the grouped table instead of scanning `store_sales` twice.
Threshold test `10 * rev <= avg_num / avg_den` is expressed in exact integers:
`10 * rev * n_items <= store_total`. -/
abbrev SaleRev := Nat × Nat × Int -- (store, item, revenue)

def singleScanStoreItems (sales : List SaleRev) (store : Nat) : List (Nat × Int) :=
  let byPair : Std.HashMap (Nat × Nat) Int :=
    sales.foldl (fun m (s, i, v) => m.insert (s, i) (m.getD (s, i) 0 + v)) {}
  let items := byPair.toList.filterMap fun ((s, i), rev) =>
    if s == store then some (i, rev) else none
  let n := items.length
  let tot := (items.map Prod.snd).sum
  items.filter fun (_, rev) => 10 * rev * (n : Int) ≤ tot

def refDoubleScanStoreItems (sales : List SaleRev) (store : Nat) : List (Nat × Int) :=
  let sSales := sales.filter fun (s, _, _) => s == store
  let itemIds := (sSales.map fun (_, i, _) => i).eraseDups
  let revs := itemIds.map fun i =>
    (i, ((sSales.filter fun (_, j, _) => j == i).map fun (_, _, v) => v).sum)
  let n := revs.length
  let tot := (revs.map Prod.snd).sum
  revs.filter fun (_, rev) => 10 * rev * (n : Int) ≤ tot

def GroupAvgThresholdTwoPassSplit.Goal : Prop :=
  ∀ (sales : List SaleRev) (store : Nat),
    (singleScanStoreItems sales store).Perm (refDoubleScanStoreItems sales store)

/-- TPC-H Q08. Pushing a selective dimension-chain semijoin (`region -> nation n1 ->
customer -> orders`) onto `lineitem` before joining `part`, `supplier`, and the
second `nation n2` alias preserves exact conditional market-share numerators and
denominators per year. -/
structure Line where
  orderKey : Nat
  partKey : Nat
  suppKey : Nat
  vol : Int

def semijoinMarketShare (lines : List Line) (orderYear : Nat → Option Int)
    (orderOk : Nat → Bool) (partOk : Nat → Bool) (suppNation : Nat → Nat)
    (targetNation : Nat) (yr : Int) : Int × Int :=
  lines.foldl (fun (num, den) l =>
    if !orderOk l.orderKey || !partOk l.partKey then (num, den) else
      match orderYear l.orderKey with
      | some y =>
        if y == yr then
          let addNum := if suppNation l.suppKey == targetNation then l.vol else 0
          (num + addNum, den + l.vol)
        else (num, den)
      | none => (num, den)) (0, 0)

def refMarketShare (lines : List Line) (orderYear : Nat → Option Int)
    (orderOk : Nat → Bool) (partOk : Nat → Bool) (suppNation : Nat → Nat)
    (targetNation : Nat) (yr : Int) : Int × Int :=
  let kept := lines.filter fun l =>
    partOk l.partKey && orderOk l.orderKey && orderYear l.orderKey == some yr
  ((kept.map fun l => if suppNation l.suppKey == targetNation then l.vol else 0).sum,
   (kept.map Line.vol).sum)

def SelectiveDimSemijoinFilterChain.Goal : Prop :=
  ∀ lines orderYear orderOk partOk suppNation targetNation yr,
    semijoinMarketShare lines orderYear orderOk partOk suppNation targetNation yr =
      refMarketShare lines orderYear orderOk partOk suppNation targetNation yr

/-- TPC-DS Q39. Fuse consecutive-month (`moy = m` and `moy = m+1`) `(cnt, sum, sumSq)`
CTE self-joins into a single pass over `(warehouse, item)` pairs. -/
structure Mom2 where
  cnt : Nat
  sum : Int
  sumSq : Int
  deriving BEq, DecidableEq

def mom2Add (a : Mom2) (v : Int) : Mom2 := ⟨a.cnt + 1, a.sum + v, a.sumSq + v * v⟩

def fusedTwoMonthMom (rows : List (Nat × Nat × Int)) (m : Nat) (key : Nat) : Mom2 × Mom2 :=
  let tbl : Std.HashMap Nat (Mom2 × Mom2) := rows.foldl (fun h (k, moy, v) =>
    if moy == m then
      let (a, b) := h.getD k (⟨0,0,0⟩, ⟨0,0,0⟩)
      h.insert k (mom2Add a v, b)
    else if moy == m + 1 then
      let (a, b) := h.getD k (⟨0,0,0⟩, ⟨0,0,0⟩)
      h.insert k (a, mom2Add b v)
    else h) {}
  tbl.getD key (⟨0,0,0⟩, ⟨0,0,0⟩)

def refTwoMonthMom (rows : List (Nat × Nat × Int)) (m : Nat) (key : Nat) : Mom2 × Mom2 :=
  let foldM := fun target =>
    (rows.filter fun (k, moy, _) => k == key && moy == target).foldl
      (fun acc (_, _, v) => mom2Add acc v) ⟨0,0,0⟩
  (foldM m, foldM (m + 1))

def CteVarianceCoeffSelfJoinPivot.Goal : Prop :=
  ∀ (rows : List (Nat × Nat × Int)) (m key : Nat),
    fusedTwoMonthMom rows m key = refTwoMonthMom rows m key

#guard pivotYears [(1, 2001, some 5), (1, 2002, some 8)] 2001 1 ==
  refYearSelfJoin [(1, 2001, some 5), (1, 2002, some 8)] 2001 1
#guard (singleScanStoreItems [(1, 10, 1), (1, 20, 100)] 1) == [(10, 1)]
#guard semijoinMarketShare [⟨1, 2, 3, 50⟩] (fun _ => some 1995) (fun _ => true)
  (fun _ => true) (fun _ => 9) 9 1995 == (50, 50)
#guard fusedTwoMonthMom [(1, 1, 3), (1, 2, 4)] 1 1 == (⟨1, 3, 9⟩, ⟨1, 4, 16⟩)

end TinylambOpt.ResearchGoals.Wave9.TpcdsMultiChannelAndTpch
