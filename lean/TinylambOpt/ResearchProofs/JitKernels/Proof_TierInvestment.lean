import TinylambOpt.ResearchGoals.JitKernels.TierInvestment

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

/-!
This module audits the work-unit model for JIT tier investment. It proves the
exact suffix-return threshold, the adjacent-cut difference, the optimal-cut
criterion, same-shape batch fusion, and eventual amortization by repetition.
It also gives concrete counterexamples to five stronger admission or fusion
claims. The results identify when SQL execution may move the compilation cut
or fuse dispatches without changing the modeled ordering of total work.
-/

namespace TinylambOpt.ResearchProofs.JitKernels.Proof_TierInvestment

open TinylambOpt.ResearchGoals.JitKernels.TierInvestment

/-- A list's net gain is its scalar work minus its compiled work. -/
private theorem net_sum_eq (t : Tariff) (xs : List Batch) :
    (xs.map (net t)).sum =
      ((xs.map (scalar t)).sum : Int) - ((xs.map (compiled t)).sum : Int) := by
  induction xs with
  | nil => simp [net]
  | cons b xs ih =>
      simp only [List.map_cons, List.sum_cons, net]
      push_cast
      omega

/-- Before the end of a trace, a bill is scalar total work plus compilation
minus the net gain of the compiled suffix. -/
private theorem bill_eq_of_lt (t : Tariff) (xs : List Batch) (cut : Nat)
    (hcut : cut < xs.length) :
    (bill t xs cut : Int) = ((xs.map (scalar t)).sum : Int) + t.compile -
      ((xs.drop cut).map (net t)).sum := by
  have hsplit := List.take_append_drop cut xs
  have hmap :
      (xs.take cut).map (scalar t) ++ (xs.drop cut).map (scalar t) =
        xs.map (scalar t) := by
    rw [← List.map_append, hsplit]
  have hsum :
      ((xs.take cut).map (scalar t)).sum + ((xs.drop cut).map (scalar t)).sum =
        (xs.map (scalar t)).sum := by
    rw [← List.sum_append, hmap]
  have hsumInt :
      (((xs.take cut).map (scalar t)).sum : Int) +
          (((xs.drop cut).map (scalar t)).sum : Int) =
        ((xs.map (scalar t)).sum : Int) := by
    exact_mod_cast hsum
  have hnet := net_sum_eq t (xs.drop cut)
  unfold bill
  rw [ite_eq_left hcut]
  push_cast
  rw [hnet]
  omega

/-- A schedule that never compiles pays exactly the scalar work. -/
private theorem bill_end (t : Tariff) (xs : List Batch) :
    bill t xs xs.length = (xs.map (scalar t)).sum := by
  simp [bill]

/-- Integer-valued form of the non-compiling endpoint bill. -/
private theorem bill_end_int (t : Tariff) (xs : List Batch) :
    (bill t xs xs.length : Int) = ((xs.map (scalar t)).sum : Int) := by
  rw [bill_end]

/-- Compiling at a cut is no worse than never compiling exactly when the
suffix's realized net gain covers the compilation charge. -/
private theorem bill_cut_le_end_iff (t : Tariff) (xs : List Batch) (cut : Nat)
    (hcut : cut < xs.length) :
    bill t xs cut ≤ bill t xs xs.length ↔
      (t.compile : Int) ≤ ((xs.drop cut).map (net t)).sum := by
  rw [← Int.ofNat_le, bill_eq_of_lt t xs cut hcut, bill_end_int t xs]
  omega

/-- Strict improvement over never compiling is equivalent to the suffix's net
gain strictly exceeding the compilation charge. -/
private theorem bill_cut_lt_end_iff (t : Tariff) (xs : List Batch) (cut : Nat)
    (hcut : cut < xs.length) :
    bill t xs cut < bill t xs xs.length ↔
      (t.compile : Int) < ((xs.drop cut).map (net t)).sum := by
  rw [← Int.ofNat_lt, bill_eq_of_lt t xs cut hcut, bill_end_int t xs]
  omega

/-- Advancing a suffix cut past an observed batch removes exactly that batch's
net gain from the remaining suffix. -/
private theorem map_sum_drop_succ (f : Batch → Int) (xs : List Batch) (i : Nat)
    (b : Batch) (hget : xs[i]? = some b) :
    ((xs.drop i).map f).sum = f b + ((xs.drop (i + 1)).map f).sum := by
  induction xs generalizing i b with
  | nil => simp at hget
  | cons x xs ih =>
      cases i with
      | zero =>
          simp only [List.getElem?_cons_zero, Option.some.injEq] at hget
          subst b
          simp
      | succ i =>
          simp only [List.getElem?_cons_succ] at hget
          simpa only [List.drop_succ_cons] using ih i b hget

/-- Repeating a trace multiplies its net gain by the repetition count. -/
private theorem sum_flatten_replicate (n : Nat) (xs : List Int) :
    (List.replicate n xs).flatten.sum = (n : Int) * xs.sum := by
  induction n with
  | zero => simp
  | succ n ih =>
      simp only [List.replicate_succ, List.flatten_cons, List.sum_append, ih]
      rw [Int.natCast_add, Int.add_mul]
      omega

/-- A nonempty compiled suffix repays one compilation exactly when its realized
savings cover the charge. This lets SQL execution select any such tier-up cut
without assuming stationarity or independence. -/
theorem suffixReturnTierUp : SuffixReturnTierUp.Goal := by
  intro t xs cut hcut
  exact bill_cut_le_end_iff t xs cut hcut

/-- Total rows and the number of successful batches do not determine passing
rows. The two traces below have equal length, row total, and success count but
different bills, so aggregate-only admission cannot preserve modeled work. -/
theorem guardRateOnlyAdmission_refuted : ¬ GuardRateOnlyAdmission.Goal := by
  intro h
  let t : Tariff := {compile := 0, guardFee := 0, slow := 10, fast := 1, replay := 0}
  let a : List Batch := [{rows := 10, passes := true}, {rows := 0, passes := false}]
  let b : List Batch := [{rows := 0, passes := true}, {rows := 10, passes := false}]
  have hbad := h t a b (by decide) (by decide) (by decide)
  have hne : bill t a 0 ≠ bill t b 0 := by decide
  exact hne hbad

/-- A profitable prefix does not lock in compilation: one later guard miss
with a replay charge can erase the prefix's savings. -/
theorem profitablePrefixLocksTier_refuted : ¬ ProfitablePrefixLocksTier.Goal := by
  intro h
  let t : Tariff :=
    {compile := 1, guardFee := 1, slow := 10, fast := 0, replay := 100}
  let a : List Batch := [{rows := 1, passes := true}]
  let b : List Batch := [{rows := 1, passes := false}]
  have hpref : bill t a 0 < bill t a a.length := by decide
  have hbad := h t a b hpref
  have hne : ¬ bill t (a ++ b) 0 ≤ bill t (a ++ b) (a ++ b).length := by decide
  exact hne hbad

/-- Moving compilation past one observed batch changes the bill by exactly
that batch's scalar-minus-compiled net gain; all other suffix terms cancel. -/
theorem adjacentTierCutDifference : AdjacentTierCutDifference.Goal := by
  intro t xs i b hbound hget
  have hi : i < xs.length := by omega
  have hbillI := bill_eq_of_lt t xs i hi
  have hbillNext := bill_eq_of_lt t xs (i + 1) hbound
  have hdrop := map_sum_drop_succ (net t) xs i b hget
  change (bill t xs (i + 1) : Int) - (bill t xs i : Int) = net t b
  rw [hbillNext, hbillI, hdrop]
  omega

/-- Among cuts that compile once, minimizing the bill is equivalent to
maximizing the net gain of the selected suffix. This permits SQL execution to
move the compilation point without changing the work-ordering criterion. -/
theorem specializeAtMaximumSuffixGain : SpecializeAtMaximumSuffixGain.Goal := by
  intro t xs i j hi hj
  rw [← Int.ofNat_le, bill_eq_of_lt t xs i hi, bill_eq_of_lt t xs j hj]
  omega

/-- A majority of successful rows can still coexist with costly failed guards.
Here the row and compilation conditions hold, but replay charges make the
compiled schedule worse, so row majority alone is not a safe admission rule. -/
theorem compileIfHotRowsDominate_refuted : ¬ CompileIfHotRowsDominate.Goal := by
  intro h
  let t : Tariff := {compile := 1, guardFee := 0, slow := 2, fast := 1, replay := 100}
  let xs : List Batch :=
    [{rows := 3, passes := true}, {rows := 1, passes := false},
      {rows := 1, passes := false}]
  have hbad := h t xs (by decide) (by decide) (by decide)
  have hne : ¬ bill t xs 0 ≤ bill t xs xs.length := by decide
  exact hne hbad

/-- Fusing batches with the same guard outcome preserves the outcome and adds
rows linearly. One dispatch replaces two, so SQL execution can perform this
fusion without increasing modeled compiled work. -/
theorem sameShapeBatchFusion : SameShapeBatchFusion.Goal := by
  intro t a b hab
  cases ha : a.passes <;> cases hb : b.passes <;>
    simp_all [compiled, Nat.mul_add] <;> omega

/-- With no guard fee, fusing a failing batch with a successful batch forces
the successful rows onto the slow replay path. The saved dispatch is then
insufficient, so arbitrary mixed-shape fusion need not preserve total work. -/
theorem mixedShapeBatchFusion_refuted : ¬ MixedShapeBatchFusion.Goal := by
  intro h
  let t : Tariff := {compile := 0, guardFee := 0, slow := 10, fast := 0, replay := 0}
  let a : Batch := {rows := 0, passes := false}
  let b : Batch := {rows := 1, passes := true}
  have hbad := h t a b (by decide)
  have hne :
      ¬ compiled t ⟨a.rows + b.rows, a.passes && b.passes⟩ ≤
        compiled t a + compiled t b := by decide
  exact hne hbad

/-- Positive net savings on one copy of a trace eventually dominate any finite
compilation charge. Repeating that trace therefore lets SQL execution defer
tier-up to a sufficiently large finite prefix while preserving a strict win. -/
theorem compiledSuffixDuplication : CompiledSuffixDuplication.Goal := by
  intro t xs hpos
  let gain := (xs.map (net t)).sum
  let n := (((t.compile : Int) / gain) + 1).toNat
  let ys := (List.replicate n xs).flatten
  have hgain : 0 < gain := hpos
  have hquot : 0 ≤ (t.compile : Int) / gain :=
    Int.ediv_nonneg (by omega) (by omega)
  have hncast : (n : Int) = (t.compile : Int) / gain + 1 := by
    dsimp [n]
    exact Int.toNat_of_nonneg (by omega)
  have hnpositive : 0 < (n : Int) := by
    rw [hncast]
    omega
  have hn : 0 < n := by exact_mod_cast hnpositive
  have hcompile : (t.compile : Int) < (n : Int) * gain := by
    rw [hncast]
    simpa [Int.mul_comm] using Int.lt_ediv_add_one_mul_self (t.compile : Int) hgain
  have hxslen : 0 < xs.length := by
    cases xs with
    | nil => simp [gain] at hpos
    | cons b bs => simp
  have hnetrep : (ys.map (net t)).sum = (n : Int) * gain := by
    dsimp [ys]
    simpa [List.map_flatten, List.map_replicate] using
      sum_flatten_replicate n (xs.map (net t))
  have hylen : 0 < ys.length := by
    dsimp [ys]
    rw [List.length_flatten, List.map_replicate, List.sum_replicate_nat]
    exact Nat.mul_pos hn hxslen
  refine ⟨n, ?_⟩
  change bill t ys 0 < bill t ys ys.length
  apply (bill_cut_lt_end_iff t ys 0 hylen).2
  change (t.compile : Int) < (ys.map (net t)).sum
  rw [hnetrep]
  exact hcompile

/-- Successful batches also incur guard fees. The two passing batches below
pay more compiled work than the failure-only bound allows, so deoptimization
counts alone cannot bound excess work. -/
theorem failuresOnlyBoundRecompileCost_refuted :
    ¬ FailuresOnlyBoundRecompileCost.Goal := by
  intro h
  let t : Tariff := {compile := 0, guardFee := 1, slow := 1, fast := 1, replay := 0}
  let xs : List Batch := [{rows := 1, passes := true}, {rows := 1, passes := true}]
  have hbad := h t xs (by decide)
  have hne :
      ¬ bill t xs 0 ≤ bill t xs xs.length + t.compile +
        (xs.countP (fun b => !b.passes)) * (t.replay + t.guardFee) := by decide
  exact hne hbad

end TinylambOpt.ResearchProofs.JitKernels.Proof_TierInvestment
