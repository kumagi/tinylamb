import Std
import TinylambOpt.ResearchGoals.QueryRewrites.Common

/-!
Unproved optimization goals for the 8 TPC-H queries where TinyLamb currently
trails DuckDB on compressed TLCS v3 storage (Q01, Q03, Q06, Q07, Q08, Q13, Q19, Q20).

Each `Goal` is stated as an open `Prop` with explicit soundness assumptions.
No `sorry` is used; proofs can be added independently under `ResearchProofs`.
-/

namespace TinylambOpt.ProposedGoals.BenchmarkRematch.Tpch

open TinylambOpt.ResearchGoals.QueryRewrites

/-- TPC-H Q01 (`l_shipdate <= cutoff` grouped by `(l_returnflag, l_linestatus)`):
When every row in a block satisfies the shipdate cutoff and both grouping keys
are dictionary-coded into a small dense pair code `code`, accumulating the four
exact linear measures directly into a dense code-indexed array produces the same
per-group totals as decoding and filtering row-by-row.
Permits skipping per-row predicate evaluation and hash-table probing on
interior TLCS blocks. -/
def Q01_InteriorBlockDenseCodeTotalsGoal : Prop :=
  ∀ (blocks : List (List (Nat × Int × Int))) (cutoff : Nat) (k : Nat),
    (∀ b ∈ blocks, ∀ r ∈ b, r.1 ≤ cutoff) →
    total (blocks.flatten.filter (fun r => decide (r.1 ≤ cutoff) && (r.2.1 == (k : Int))))
      (fun r => r.2.2) =
    (blocks.map (fun b =>
      total (b.filter (fun r => r.2.1 == (k : Int))) (fun r => r.2.2))).sum

/-- TPC-H Q03 (`customer ⋈ orders ⋈ lineitem`, Top-10 revenue):
If every lineitem contributes nonnegative net revenue and a partial top-k
threshold `tau` is already achieved by `k` complete order groups, then any
unscanned order group whose remaining maximum possible revenue (from block-level
upper bounds on `l_extendedprice`) is strictly below `tau` cannot enter the top-k
order set.
Permits pruning late-materialized order payloads and skipping remaining lineitem
blocks for low-bound orders. -/
def Q03_NonnegativeRevenueUpperBoundDominanceGoal : Prop :=
  ∀ (rev bound : List Int) (tau : Int),
    rev.length = bound.length →
    (∀ p ∈ rev.zip bound, 0 ≤ p.1 ∧ p.1 ≤ p.2) →
    (bound.sum < tau) →
    rev.sum < tau

/-- TPC-H Q06 (single-table selective revenue sum on `lineitem`):
When `l_discount` and `l_quantity` are dictionary- or bit-packed-coded and a
block's `(min_date, max_date)` is completely inside the date window `[lo, hi)`,
evaluating the compound predicate via precomputed bitmasks on `(discount, quantity)`
and summing `l_extendedprice * l_discount` only on masked rows matches the
full-row predicate evaluation.
Permits branchless SIMD mask evaluation without materializing non-matching
columns. -/
def Q06_InteriorDateMaskRevenueSumGoal : Prop :=
  ∀ (rows : List (Nat × Nat × Nat × Int)) (dLo dHi qHi : Nat)
    (discOk : Nat → Bool),
    (∀ r ∈ rows, dLo ≤ r.1 ∧ r.1 < dHi) →
    total (rows.filter (fun r =>
        decide (dLo ≤ r.1 ∧ r.1 < dHi) && discOk r.2.1 && decide (r.2.2.1 < qHi)))
      (fun r => r.2.2.2) =
    total (rows.filter (fun r => discOk r.2.1 && decide (r.2.2.1 < qHi)))
      (fun r => r.2.2.2)

/-- TPC-H Q07 (bidirectional nation shipping volume between two nations):
For a symmetric pair of target nations `{n1, n2}` (`n1 ≠ n2`), filtering
`lineitem` by `s_nation ∈ {n1, n2}` and `c_nation ∈ {n1, n2}` with
`s_nation ≠ c_nation` partitions exactly into the two directed pairs
`(n1, n2)` and `(n2, n1)`.
Permits replacing a general hash join + disjunction on nation names with two
compact bitset semi-joins and a 2-slot `(direction, year)` direct array. -/
def Q07_OrderedNationPairPartitionGoal : Prop :=
  ∀ (rows : List (Nat × Nat × Int)) (n1 n2 : Nat),
    n1 ≠ n2 →
    total (rows.filter (fun r =>
        ((r.1 == n1) && (r.2.1 == n2)) || ((r.1 == n2) && (r.2.1 == n1))))
      (fun r => r.2.2) =
    total (rows.filter (fun r => (r.1 == n1) && (r.2.1 == n2))) (fun r => r.2.2) +
    total (rows.filter (fun r => (r.1 == n2) && (r.2.1 == n1))) (fun r => r.2.2)

/-- TPC-H Q08 (national market share for one target nation over a year):
Summing a conditional indicator `if nation == target then vol else 0` and total
volume `vol` over `lineitem` joined to unique `supplier` keys equals
pre-aggregating `vol` by `(year, suppkey)` first and then joining each unique
`suppkey` once to `supplier` to classify `nation == target`.
Permits deferring the `supplier`/`nation` lookup until after `lineitem` is
collapsed by `(year, suppkey)`. -/
def Q08_SupplierPreaggMarketShareGoal : Prop :=
  ∀ (rows : List (Nat × Int)) (isTarget : Nat → Bool) (keys : List Nat),
    keys.Nodup →
    (∀ r ∈ rows, r.1 ∈ keys) →
    total (rows.filter (fun r => isTarget r.1)) Prod.snd =
    (keys.filter isTarget |>.map (fun s =>
      total (rows.filter (fun r => r.1 == s)) Prod.snd)).sum

/-- TPC-H Q13 (`customer LEFT OUTER JOIN orders` with `o_comment NOT LIKE ...`):
For a left outer join where each customer `c` matches `deg(c)` qualifying orders
(possibly 0), counting the number of customers having exactly `k` qualifying
orders is invariant to replacing the outer join with an inner-join degree table
for `deg(c) > 0` plus `|C| - |{c ∈ C | deg(c) > 0}|` for `k = 0`.
Permits avoiding NULL-padded row emission for unmatched customers and computing
the histogram directly from a dense `c_custkey`-indexed degree array. -/
def Q13_LeftJoinZeroDegreeComplementGoal : Prop :=
  ∀ (custs : List Nat) (deg : Nat → Nat),
    countWhere custs (fun c => deg c == 0) +
    countWhere custs (fun c => decide (0 < deg c)) =
    custs.length

/-- TPC-H Q19 (disjunction of 3 brand/container/size/quantity branches with shared
shipmode and shipinstruct predicates):
Factoring the common `l_shipmode IN ('AIR','AIR REG') AND l_shipinstruct = 'DELIVER IN PERSON'`
predicate outside the 3-way disjunction preserves the exact set of qualifying
rows and their summed revenue.
Permits evaluating the selective shipment filter once per block before probing
the `part` dimension or evaluating any branch-specific quantity bounds. -/
def Q19_SharedShipmentPredicateFactoringGoal : Prop :=
  ∀ {α : Type} (rows : List α) (common b1 b2 b3 : α → Bool) (rev : α → Int),
    total (rows.filter (fun r =>
        (common r && b1 r) || (common r && b2 r) || (common r && b3 r))) rev =
    total ((rows.filter common).filter (fun r => b1 r || b2 r || b3 r)) rev

/-- TPC-H Q20 (correlated `ps_availqty > 0.5 * SUM(l_quantity)` semi-join):
When `partsupp` is pre-filtered to candidate `(partkey, suppkey)` pairs matching
the `part` prefix and `supplier` nation, summing `l_quantity` only over
`lineitem` rows whose `(partkey, suppkey)` belongs to that candidate set yields
the exact same threshold for every candidate pair.
Permits pushing a compact `(partkey, suppkey)` bloom/bitset semi-join into the
`lineitem` scan before hashing or summing `l_quantity`. -/
def Q20_CandidatePairQuantitySumEquivGoal : Prop :=
  ∀ (lineitems : List ((Nat × Nat) × Int)) (cands : List (Nat × Nat))
    (k : Nat × Nat),
    k ∈ cands →
    total (lineitems.filter (fun r => r.1 == k)) Prod.snd =
    total ((lineitems.filter (fun r => cands.contains r.1)).filter (fun r => r.1 == k))
      Prod.snd

end TinylambOpt.ProposedGoals.BenchmarkRematch.Tpch
