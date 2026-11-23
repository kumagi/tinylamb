import Std.Tactic

/-!
# Checked INT64 SUM over an ordered chunk tree

C++ counterpart: `executor/detail/pipeline/ooc_group_agg_sink.cpp`.

The **reference** walks the rows of a group in input order.  After every
non-NULL value it checks that the mathematical running sum still fits in
INT64; the first row whose addition leaves the range determines the reported
failure ordinal (0-based, counting every row including NULL rows).  NULL rows
are skipped by the addition but still consume an ordinal.

The **optimized** computation first summarizes every ordered chunk by
`(sum, minPrefix, maxPrefix, nonNullCount)` over mathematical integers.
Summaries are combined bottom-up in the shape of an arbitrary binary tree that
preserves the input order (`node l r` means all rows of `l` precede `r`).
A subtree is *safe* for an incoming accumulator `acc` iff
`i64Min ≤ acc + minPrefix` and `acc + maxPrefix ≤ i64Max`; a safe subtree is
skipped wholesale (only its `sum` is added, at cost zero checked additions).
An unsafe subtree is descended into, in left-to-right order, to locate the
first failing row.

Sufficient hypotheses: the tree must preserve row order (captured by
`flatten`), the incoming accumulator must already be in range, and the
boundary summary arithmetic is over mathematical integers (no wrap-around).
Empty chunks, negative values and sparse NULL positions are all admitted.

`checked_aggregate_tree_sound` proves (a) the tree result equals the
sequential reference (value or first failure ordinal), (b) the number of
checked additions never exceeds the number of non-NULL rows, and (c) the
result is independent of the bracketing of the same ordered row sequence.
The counterexamples show that merging chunk totals in arbitrary order, or
checking only the final total, loses both the intermediate failure and its
row ordinal on the regression input `[MAX, 1, -1]`.
-/

namespace TinylambOpt.Research.CheckedAggregateTree

abbrev Val := Option Int

def i64Min : Int := -9223372036854775808
def i64Max : Int :=  9223372036854775807

def InRange (z : Int) : Prop := i64Min ≤ z ∧ z ≤ i64Max

instance (z : Int) : Decidable (InRange z) := by
  unfold InRange
  infer_instance

inductive AggResult where
  | ok : Int → AggResult
  | fail : Nat → AggResult
  deriving DecidableEq, Repr

open AggResult

def sumN : List Val → Int
  | [] => 0
  | none :: xs => sumN xs
  | some v :: xs => v + sumN xs

def countNN : List Val → Nat
  | [] => 0
  | none :: xs => countNN xs
  | some _ :: xs => countNN xs + 1

theorem countNN_append (xs ys : List Val) :
    countNN (xs ++ ys) = countNN xs + countNN ys := by
  induction xs with
  | nil => simp [countNN]
  | cons o xs ih =>
      cases o <;> simp [countNN, ih, Nat.add_assoc, Nat.add_comm, Nat.add_left_comm]

/-! ## Reference: sequential checked SUM (plus an explicit cost evaluator) -/

def seqChecked (acc : Int) (idx : Nat) : List Val → AggResult
  | [] => ok acc
  | none :: xs => seqChecked acc (idx + 1) xs
  | some v :: xs =>
      let acc' := acc + v
      if InRange acc' then seqChecked acc' (idx + 1) xs else fail idx

def seqCheckedCost (acc : Int) (idx : Nat) : List Val → AggResult × Nat
  | [] => (ok acc, 0)
  | none :: xs => seqCheckedCost acc (idx + 1) xs
  | some v :: xs =>
      let acc' := acc + v
      if InRange acc' then
        let r := seqCheckedCost acc' (idx + 1) xs
        (r.1, r.2 + 1)
      else (fail idx, 1)

theorem seqCheckedCost_fst (acc : Int) (idx : Nat) (xs : List Val) :
    (seqCheckedCost acc idx xs).1 = seqChecked acc idx xs := by
  induction xs generalizing acc idx with
  | nil => rfl
  | cons o xs ih =>
      cases o with
      | none => simpa [seqCheckedCost, seqChecked] using ih acc (idx + 1)
      | some v =>
          by_cases h : InRange (acc + v)
          · simp [seqCheckedCost, seqChecked, h, ih (acc + v) (idx + 1)]
          · simp [seqCheckedCost, seqChecked, h]

theorem seqCheckedCost_le (acc : Int) (idx : Nat) (xs : List Val) :
    (seqCheckedCost acc idx xs).2 ≤ countNN xs := by
  induction xs generalizing acc idx with
  | nil => simp [seqCheckedCost, countNN]
  | cons o xs ih =>
      cases o with
      | none => simpa [seqCheckedCost, countNN] using ih acc (idx + 1)
      | some v =>
          by_cases h : InRange (acc + v)
          · simp [seqCheckedCost, countNN, h]
            have := ih (acc + v) (idx + 1)
            omega
          · simp [seqCheckedCost, countNN, h]

def PrefixesSafe (acc : Int) : List Val → Prop
  | [] => InRange acc
  | none :: xs => PrefixesSafe acc xs
  | some v :: xs => InRange (acc + v) ∧ PrefixesSafe (acc + v) xs

theorem seqChecked_ok_of_prefixes (acc : Int) (idx : Nat) (xs : List Val)
    (h : PrefixesSafe acc xs) : seqChecked acc idx xs = ok (acc + sumN xs) := by
  induction xs generalizing acc idx with
  | nil => simp [seqChecked, sumN]
  | cons o xs ih =>
      cases o with
      | none => simpa [seqChecked, sumN] using ih acc (idx + 1) h
      | some v =>
          have hIn : InRange (acc + v) := h.1
          have ht := ih (acc + v) (idx + 1) h.2
          simpa [seqChecked, sumN, hIn, Int.add_assoc] using ht

theorem seqCheckedCost_ok_of_prefixes (acc : Int) (idx : Nat) (xs : List Val)
    (h : PrefixesSafe acc xs) :
    seqCheckedCost acc idx xs = (ok (acc + sumN xs), countNN xs) := by
  induction xs generalizing acc idx with
  | nil => simp [seqCheckedCost, sumN, countNN]
  | cons o xs ih =>
      cases o with
      | none => simpa [seqCheckedCost, sumN, countNN] using ih acc (idx + 1) h
      | some v =>
          have hIn : InRange (acc + v) := h.1
          have ht := ih (acc + v) (idx + 1) h.2
          rw [show seqCheckedCost acc idx (some v :: xs) =
              (let r := seqCheckedCost (acc + v) (idx + 1) xs
               (r.1, r.2 + 1)) by simp [seqCheckedCost, hIn]]
          rw [ht]
          simp [sumN, countNN, Int.add_assoc]

theorem finalInRange_of_prefixes (acc : Int) (xs : List Val)
    (h : PrefixesSafe acc xs) : InRange (acc + sumN xs) := by
  induction xs generalizing acc with
  | nil => simpa [sumN, PrefixesSafe] using h
  | cons o xs ih =>
      cases o with
      | none => simpa [sumN, PrefixesSafe] using ih acc h
      | some v =>
          have ht := ih (acc + v) h.2
          simpa [sumN, Int.add_assoc] using ht

theorem seqChecked_ok_inRange (acc : Int) (idx : Nat) (xs : List Val)
    (hacc : InRange acc) {a : Int}
    (h : seqChecked acc idx xs = ok a) : InRange a := by
  induction xs generalizing acc idx with
  | nil =>
      simp [seqChecked] at h
      subst a
      exact hacc
  | cons o xs ih =>
      cases o with
      | none => exact ih acc (idx + 1) hacc h
      | some v =>
          by_cases hi : InRange (acc + v)
          · have ht : seqChecked (acc + v) (idx + 1) xs = ok a := by simpa [seqChecked, hi] using h
            exact ih (acc + v) (idx + 1) hi ht
          · simp [seqChecked, hi] at h

theorem seqChecked_exists_fail_of_not_prefixes (acc : Int) (idx : Nat) (xs : List Val)
    (hacc : InRange acc) (h : ¬ PrefixesSafe acc xs) :
    ∃ o, seqChecked acc idx xs = fail o := by
  induction xs generalizing acc idx with
  | nil =>
      have hh : ¬ InRange acc := by simpa [PrefixesSafe] using h
      exact False.elim (hh hacc)
  | cons o xs ih =>
      cases o with
      | none =>
          obtain ⟨o', ho'⟩ := ih acc (idx + 1) hacc h
          exact ⟨o', by simpa [seqChecked] using ho'⟩
      | some v =>
          by_cases hIn : InRange (acc + v)
          · have htail : ¬ PrefixesSafe (acc + v) xs := by
              intro ht
              exact h ⟨hIn, ht⟩
            obtain ⟨o', ho'⟩ := ih (acc + v) (idx + 1) hIn htail
            exact ⟨o', by simp [seqChecked, hIn, ho']⟩
          · exact ⟨idx, by simp [seqChecked, hIn]⟩

theorem seqChecked_append (acc : Int) (idx : Nat) (xs ys : List Val) :
    seqChecked acc idx (xs ++ ys) =
      match seqChecked acc idx xs with
      | ok a => seqChecked a (idx + xs.length) ys
      | fail o => fail o := by
  induction xs generalizing acc idx with
  | nil => rfl
  | cons o xs ih =>
      cases o with
      | none =>
          have ht := ih acc (idx + 1)
          simpa [seqChecked, Nat.add_assoc, Nat.add_comm, Nat.add_left_comm] using ht
      | some v =>
          by_cases h : InRange (acc + v)
          · have ht := ih (acc + v) (idx + 1)
            simpa [seqChecked, h, Nat.add_assoc, Nat.add_comm, Nat.add_left_comm] using ht
          · simp [seqChecked, h]

/-! ## Summary monoid -/

structure Summary where
  sum : Int
  minPref : Int
  maxPref : Int
  nonNull : Nat
  deriving DecidableEq, Repr

def summary : List Val → Summary
  | [] => ⟨0, 0, 0, 0⟩
  | none :: xs => summary xs
  | some v :: xs =>
      let s := summary xs
      ⟨v + s.sum, min 0 (v + s.minPref), max 0 (v + s.maxPref), s.nonNull + 1⟩

def combine (a b : Summary) : Summary :=
  ⟨a.sum + b.sum,
   min a.minPref (a.sum + b.minPref),
   max a.maxPref (a.sum + b.maxPref),
   a.nonNull + b.nonNull⟩

theorem summary_sum (xs : List Val) : (summary xs).sum = sumN xs := by
  induction xs with
  | nil => rfl
  | cons o xs ih =>
      cases o <;> simp [summary, sumN, ih]

theorem summary_nonNull (xs : List Val) : (summary xs).nonNull = countNN xs := by
  induction xs with
  | nil => rfl
  | cons o xs ih =>
      cases o <;> simp [summary, countNN, ih, Nat.add_assoc]

theorem summary_minPref_nonpos (xs : List Val) : (summary xs).minPref ≤ 0 := by
  induction xs with
  | nil => simp [summary]
  | cons o xs ih =>
      cases o <;> simp [summary, ih] <;> omega

theorem summary_maxPref_nonneg (xs : List Val) : 0 ≤ (summary xs).maxPref := by
  induction xs with
  | nil => simp [summary]
  | cons o xs ih =>
      cases o <;> simp [summary, ih] <;> omega

theorem summary_append (xs ys : List Val) :
    summary (xs ++ ys) = combine (summary xs) (summary ys) := by
  induction xs with
  | nil =>
      have hmn := summary_minPref_nonpos ys
      have hmx := summary_maxPref_nonneg ys
      have hmin : min 0 (summary ys).minPref = (summary ys).minPref := by omega
      have hmax : max 0 (summary ys).maxPref = (summary ys).maxPref := by omega
      simp [summary, combine, hmin, hmax]
  | cons o xs ih =>
      cases o with
      | none =>
          change summary (xs ++ ys) = combine (summary xs) (summary ys)
          exact ih
      | some v =>
          simp only [List.cons_append]
          simp [summary, ih, combine] <;> omega

theorem prefixesSafe_of_summary (acc : Int) (xs : List Val) (hacc : InRange acc)
    (hlo : i64Min ≤ acc + (summary xs).minPref)
    (hhi : acc + (summary xs).maxPref ≤ i64Max) : PrefixesSafe acc xs := by
  induction xs generalizing acc with
  | nil => simpa [summary, PrefixesSafe, InRange] using And.intro hlo hhi
  | cons o xs ih =>
      cases o with
      | none => exact ih acc hacc hlo hhi
      | some v =>
          have hlo' : i64Min ≤ (acc + v) + (summary xs).minPref := by
            have hm : min 0 (v + (summary xs).minPref) ≤ v + (summary xs).minPref := by omega
            simp [summary] at hlo
            omega
          have hhi' : (acc + v) + (summary xs).maxPref ≤ i64Max := by
            have hm : v + (summary xs).maxPref ≤ max 0 (v + (summary xs).maxPref) := by omega
            simp [summary] at hhi
            omega
          have hIn : InRange (acc + v) := by
            constructor
            · have hm := summary_minPref_nonpos xs
              have hle : (acc + v) + (summary xs).minPref ≤ acc + v := by omega
              exact Int.le_trans hlo' hle
            · have hm := summary_maxPref_nonneg xs
              have hle : acc + v ≤ (acc + v) + (summary xs).maxPref := by omega
              exact Int.le_trans hle hhi'
          exact ⟨hIn, ih (acc + v) hIn hlo' hhi'⟩

theorem summary_safe_of_prefixes (acc : Int) (xs : List Val) (hacc : InRange acc)
    (h : PrefixesSafe acc xs) :
    i64Min ≤ acc + (summary xs).minPref ∧ acc + (summary xs).maxPref ≤ i64Max := by
  induction xs generalizing acc with
  | nil => simpa [summary, InRange] using hacc
  | cons o xs ih =>
      cases o with
      | none => exact ih acc hacc h
      | some v =>
          have hIn : InRange (acc + v) := h.1
          have ht := ih (acc + v) hIn h.2
          have hInLo : i64Min ≤ acc + v := hIn.1
          have hInHi : acc + v ≤ i64Max := hIn.2
          have htLo : i64Min ≤ (acc + v) + (summary xs).minPref := ht.1
          have htHi : (acc + v) + (summary xs).maxPref ≤ i64Max := ht.2
          have haLo : i64Min ≤ acc := hacc.1
          have haHi : acc ≤ i64Max := hacc.2
          simp [summary]
          constructor <;> omega

def safe (acc : Int) (s : Summary) : Bool :=
  decide (i64Min ≤ acc + s.minPref ∧ acc + s.maxPref ≤ i64Max)

theorem safe_eq (acc : Int) (s : Summary) :
    safe acc s = true ↔ i64Min ≤ acc + s.minPref ∧ acc + s.maxPref ≤ i64Max := by
  simp [safe]

/-! ## Ordered chunk tree and its optimized evaluator -/

inductive Tree where
  | leaf : List Val → Tree
  | node : Tree → Tree → Tree

def flatten : Tree → List Val
  | Tree.leaf vs => vs
  | Tree.node l r => flatten l ++ flatten r

def treeSummary : Tree → Summary
  | Tree.leaf vs => summary vs
  | Tree.node l r => combine (treeSummary l) (treeSummary r)

theorem treeSummary_flatten (t : Tree) : treeSummary t = summary (flatten t) := by
  induction t with
  | leaf vs => simp [flatten, treeSummary]
  | node l r ihl ihr => simp [flatten, treeSummary, ihl, ihr, summary_append]

def treeEval (acc : Int) (base : Nat) : Tree → AggResult × Nat
  | Tree.leaf vs => seqCheckedCost acc base vs
  | Tree.node l r =>
      let sl := treeSummary l
      if safe acc sl then
        let rr := treeEval (acc + sl.sum) (base + (flatten l).length) r
        (rr.1, rr.2)
      else
        let lr := treeEval acc base l
        match lr.1 with
        | fail o => (fail o, lr.2)
        | ok a =>
            let rr := treeEval a (base + (flatten l).length) r
            (rr.1, lr.2 + rr.2)

theorem treeEval_result (acc : Int) (base : Nat) (hacc : InRange acc) (t : Tree) :
    (treeEval acc base t).1 = seqChecked acc base (flatten t) := by
  induction t generalizing acc base with
  | leaf vs => exact seqCheckedCost_fst acc base vs
  | node l r ihl ihr =>
      let sl := treeSummary l
      have hsl : summary (flatten l) = sl := by
        simpa [sl] using (treeSummary_flatten l).symm
      by_cases hs : safe acc sl = true
      · have hb : i64Min ≤ acc + sl.minPref ∧ acc + sl.maxPref ≤ i64Max := (safe_eq acc sl).mp hs
        have hpre : PrefixesSafe acc (flatten l) :=
          prefixesSafe_of_summary acc (flatten l) hacc
            (by simpa [hsl] using hb.1) (by simpa [hsl] using hb.2)
        have hseqL : seqChecked acc base (flatten l) = ok (acc + sumN (flatten l)) :=
          seqChecked_ok_of_prefixes acc base (flatten l) hpre
        have hsum : sl.sum = sumN (flatten l) := by
          have heq : sl = summary (flatten l) := by simpa [sl] using treeSummary_flatten l
          rw [heq, summary_sum]
        have haccR : InRange (acc + sl.sum) := by
          rw [hsum]
          exact finalInRange_of_prefixes acc (flatten l) hpre
        rw [treeEval, if_pos hs]
        rw [ihr (acc + sl.sum) (base + (flatten l).length) haccR]
        rw [flatten, seqChecked_append, hseqL]
        simp [hsum]
      · have hnb : ¬ (i64Min ≤ acc + sl.minPref ∧ acc + sl.maxPref ≤ i64Max) := by
          intro hb
          exact hs ((safe_eq acc sl).mpr hb)
        have hnpre : ¬ PrefixesSafe acc (flatten l) := by
          intro hp
          exact hnb (by simpa [hsl] using summary_safe_of_prefixes acc (flatten l) hacc hp)
        obtain ⟨o, hfo⟩ := seqChecked_exists_fail_of_not_prefixes acc base (flatten l) hacc hnpre
        have htl : (treeEval acc base l).1 = fail o := by
          rw [ihl acc base hacc, hfo]
        have hs' : ¬ safe acc (treeSummary l) = true := by simpa [sl] using hs
        simp [treeEval, hs', htl]
        rw [flatten, seqChecked_append, hfo]

theorem treeEval_cost_le (acc : Int) (base : Nat) (hacc : InRange acc) (t : Tree) :
    (treeEval acc base t).2 ≤ countNN (flatten t) := by
  induction t generalizing acc base with
  | leaf vs => simpa [treeEval, flatten] using seqCheckedCost_le acc base vs
  | node l r ihl ihr =>
      let sl := treeSummary l
      have hcount : countNN (flatten (Tree.node l r)) = countNN (flatten l) + countNN (flatten r) := by
        simp [flatten, countNN_append]
      by_cases hs : safe acc sl = true
      · have hb : i64Min ≤ acc + sl.minPref ∧ acc + sl.maxPref ≤ i64Max := (safe_eq acc sl).mp hs
        have hsl : summary (flatten l) = sl := by
          simpa [sl] using (treeSummary_flatten l).symm
        have hpre : PrefixesSafe acc (flatten l) :=
          prefixesSafe_of_summary acc (flatten l) hacc
            (by simpa [hsl] using hb.1) (by simpa [hsl] using hb.2)
        have hsum : sl.sum = sumN (flatten l) := by
          have heq : sl = summary (flatten l) := by simpa [sl] using treeSummary_flatten l
          rw [heq, summary_sum]
        have haccR : InRange (acc + sl.sum) := by
          rw [hsum]
          exact finalInRange_of_prefixes acc (flatten l) hpre
        rw [treeEval, if_pos hs]
        change (treeEval (acc + (treeSummary l).sum) (base + (flatten l).length) r).2 ≤
          countNN (flatten (Tree.node l r))
        have haccR' : InRange (acc + (treeSummary l).sum) := by simpa [sl] using haccR
        have hr := ihr (acc + (treeSummary l).sum) (base + (flatten l).length) haccR'
        rw [hcount]
        omega
      · rw [treeEval, if_neg hs]
        have hcl := ihl acc base hacc
        cases hres : (treeEval acc base l).1 with
        | fail o =>
            simp [hres]
            omega
        | ok a =>
            have hseqL : seqChecked acc base (flatten l) = ok a := by
              rw [← treeEval_result acc base hacc l, hres]
            have haIn : InRange a := seqChecked_ok_inRange acc base (flatten l) hacc hseqL
            have hr := ihr a (base + (flatten l).length) haIn
            rw [hcount]
            simp [hres]
            omega

theorem checked_aggregate_tree_sound (acc : Int) (hacc : InRange acc) (t : Tree) :
    (treeEval acc 0 t).1 = seqChecked acc 0 (flatten t) ∧
    (treeEval acc 0 t).2 ≤ countNN (flatten t) ∧
    (∀ t' : Tree, flatten t' = flatten t → (treeEval acc 0 t').1 = (treeEval acc 0 t).1) := by
  refine ⟨treeEval_result acc 0 hacc t, treeEval_cost_le acc 0 hacc t, ?_⟩
  intro t' hft
  rw [treeEval_result acc 0 hacc t', treeEval_result acc 0 hacc t, hft]

/-- A safe left subtree is skipped at zero checked-addition cost: the node's
    evaluator is literally the evaluator of the right subtree with the
    shifted accumulator. -/
theorem safe_subtree_skipped (acc : Int) (base : Nat) (l r : Tree)
    (hs : safe acc (treeSummary l) = true) :
    (treeEval acc base (Tree.node l r)).1 =
        (treeEval (acc + (treeSummary l).sum) (base + (flatten l).length) r).1 ∧
    (treeEval acc base (Tree.node l r)).2 =
        (treeEval (acc + (treeSummary l).sum) (base + (flatten l).length) r).2 := by
  constructor <;> rw [treeEval, if_pos hs]

/-! ## Counterexamples: reordering partial sums loses the first failure -/

def chunkSum (c : List Val) : Int := sumN c

def naiveTotal (cs : List (List Val)) : Option Int :=
  let s := cs.foldl (fun a c => a + chunkSum c) 0
  if InRange s then some s else none

def naiveChunkCheck (cs : List (List Val)) : AggResult :=
  cs.foldl (fun r c =>
    match r with
    | fail o => fail o
    | ok a =>
        let a' := a + chunkSum c
        if InRange a' then ok a' else fail 0) (ok 0)

theorem reordered_partial_merge_counterexample :
    let cs : List (List Val) := [[some i64Max], [some 1], [some (-1)]]
    naiveTotal cs = some i64Max ∧
    naiveTotal cs.reverse = some i64Max ∧
    seqChecked 0 0 cs.flatten = fail 1 := by
  decide

theorem arbitrary_order_merge_loses_failure_ordinal :
    let cs : List (List Val) := [[some i64Max], [some 1], [some (-1)]]
    naiveChunkCheck cs = fail 0 ∧
    naiveChunkCheck cs.reverse = ok i64Max ∧
    seqChecked 0 0 cs.flatten = fail 1 := by
  decide

theorem final_total_only_unsound :
    let cs : List (List Val) := [[some i64Max], [some 1], [some (-1)]]
    naiveTotal cs = some i64Max ∧
    (seqChecked 0 0 cs.flatten) ≠ ok i64Max := by
  decide

end TinylambOpt.Research.CheckedAggregateTree
