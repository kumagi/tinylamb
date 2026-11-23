import TinylambOpt.ResearchGoals.ExactIntTotalOverflowGuard

namespace TinylambOpt.ResearchProofs.ExactIntTotalOverflowGuard

open TinylambOpt.ResearchGoals.ExactIntTotalOverflowGuard

private theorem int64Max_nat : int64Max.toNat = 9223372036854775807 := by decide
private theorem int64Min_int : int64Min = -9223372036854775808 := by rfl
private theorem int64Max_int : int64Max = 9223372036854775807 := by rfl

private theorem int64_range_of_natAbs_le {s : Int} {N : Nat} (hN : N ≤ int64Max.toNat)
    (hs : s.natAbs ≤ N) : int64Min ≤ s ∧ s ≤ int64Max := by
  have h1 : s.natAbs ≤ int64Max.toNat := by omega
  rw [int64Max_nat] at h1
  rw [int64Min_int, int64Max_int]
  omega

private def sumOpt (rows : List (Option Int)) : Option Int :=
  rows.foldl combineOptSum none

private def ovAbs : Option Int → Nat
  | none => 0
  | some v => v.natAbs

private def rowsAbsSum : List (Option Int) → Nat
  | [] => 0
  | ov :: xs => ovAbs ov + rowsAbsSum xs

private theorem rowsAbsSum_append (xs ys : List (Option Int)) :
    rowsAbsSum (xs ++ ys) = rowsAbsSum xs + rowsAbsSum ys := by
  induction xs with
  | nil => simp [rowsAbsSum]
  | cons ov xs ih =>
      change ovAbs ov + rowsAbsSum (xs ++ ys) = (ovAbs ov + rowsAbsSum xs) + rowsAbsSum ys
      rw [ih, Nat.add_assoc]

private theorem combineOptSum_assoc (a b c : Option Int) :
    combineOptSum (combineOptSum a b) c = combineOptSum a (combineOptSum b c) := by
  cases a <;> cases b <;> cases c <;> simp [combineOptSum] <;> omega

private theorem foldl_combineOptSum_eq_combine (ys : List (Option Int)) (acc : Option Int) :
    ys.foldl combineOptSum acc = combineOptSum acc (ys.foldl combineOptSum none) := by
  induction ys generalizing acc with
  | nil => cases acc <;> rfl
  | cons x ys ih =>
      change ys.foldl combineOptSum (combineOptSum acc x) =
        combineOptSum acc (ys.foldl combineOptSum (combineOptSum none x))
      rw [ih (acc := combineOptSum acc x), ih (acc := combineOptSum none x)]
      rw [combineOptSum_assoc]
      cases x <;> rfl

private theorem foldl_combineOptSum_append (xs ys : List (Option Int)) (acc : Option Int) :
    (xs ++ ys).foldl combineOptSum acc = ys.foldl combineOptSum (xs.foldl combineOptSum acc) := by
  induction xs generalizing acc with
  | nil => rfl
  | cons x xs ih =>
      change List.foldl combineOptSum (combineOptSum acc x) (xs ++ ys) =
        List.foldl combineOptSum (List.foldl combineOptSum (combineOptSum acc x) xs) ys
      exact ih (acc := combineOptSum acc x)

private theorem sumOpt_append (xs ys : List (Option Int)) :
    sumOpt (xs ++ ys) = combineOptSum (sumOpt xs) (sumOpt ys) := by
  unfold sumOpt
  rw [foldl_combineOptSum_append]
  exact foldl_combineOptSum_eq_combine ys (List.foldl combineOptSum none xs)

private theorem foldl_flatMap_sumOpt (bs : List Block) (acc : Option Int) :
    bs.foldl (fun a b => combineOptSum a (sumOpt (blockToRows b))) acc =
      combineOptSum acc (sumOpt (bs.flatMap blockToRows)) := by
  induction bs generalizing acc with
  | nil => cases acc <;> rfl
  | cons b bs ih =>
      change List.foldl (fun a b => combineOptSum a (sumOpt (blockToRows b)))
          (combineOptSum acc (sumOpt (blockToRows b))) bs =
        combineOptSum acc (sumOpt (blockToRows b ++ (bs.flatMap blockToRows)))
      rw [sumOpt_append]
      rw [ih]
      exact combineOptSum_assoc acc (sumOpt (blockToRows b)) (sumOpt (bs.flatMap blockToRows))

private theorem sumOpt_flatMap (bs : List Block) :
    sumOpt (bs.flatMap blockToRows) =
      bs.foldl (fun a b => combineOptSum a (sumOpt (blockToRows b))) none := by
  have h := foldl_flatMap_sumOpt bs none
  cases hR : sumOpt (bs.flatMap blockToRows) with
  | none => simpa [hR, combineOptSum] using h.symm
  | some s => simpa [hR, combineOptSum] using h.symm

private def ckStep (res : CheckedResult) (ov : Option Int) : CheckedResult :=
  match res with
  | .overflow => .overflow
  | .ok acc =>
      match ov with
      | none => .ok acc
      | some v =>
          match acc with
          | none =>
              if decide (int64Min ≤ v ∧ v ≤ int64Max) then .ok (some v) else .overflow
          | some cur =>
              match checkedAddInt64 cur v with
              | .overflow => .overflow
              | .ok (some s) => .ok (some s)
              | .ok none => .overflow

private theorem refCheckedSum_foldl (rows : List (Option Int)) :
    refCheckedSum rows = rows.foldl ckStep (.ok none) := by
  rfl

private theorem ck_foldl_ok (rows : List (Option Int)) (acc : Option Int) (A C : Nat)
    (hAC : A + C ≤ int64Max.toNat)
    (hacc : match acc with | none => A = 0 | some a => a.natAbs ≤ A)
    (hrows : rowsAbsSum rows ≤ C) :
    rows.foldl ckStep (.ok acc) = .ok (rows.foldl combineOptSum acc) := by
  induction rows generalizing acc A C with
  | nil => rfl
  | cons ov rows ih =>
      have hOvAbsC : ovAbs ov ≤ C := by
        change ovAbs ov + rowsAbsSum rows ≤ C at hrows
        omega
      have hrows' : rowsAbsSum rows ≤ C - ovAbs ov := by
        change ovAbs ov + rowsAbsSum rows ≤ C at hrows
        omega
      let A' : Nat := A + ovAbs ov
      let C' : Nat := C - ovAbs ov
      have hA'C' : A' + C' ≤ int64Max.toNat := by
        dsimp [A', C']
        omega
      have hA'Bound : A' ≤ int64Max.toNat := by omega
      have hstep : ckStep (.ok acc) ov = .ok (combineOptSum acc ov) := by
        cases acc with
        | none =>
            cases ov with
            | none => rfl
            | some v =>
                have hvAbs : v.natAbs ≤ int64Max.toNat := by
                  dsimp [ovAbs] at hOvAbsC
                  omega
                have hvRange := int64_range_of_natAbs_le hvAbs
                  (show v.natAbs ≤ v.natAbs from Nat.le_refl _)
                simp [ckStep, hvRange, combineOptSum]
        | some a =>
            cases ov with
            | none => rfl
            | some v =>
                have hsrange : int64Min ≤ a + v ∧ a + v ≤ int64Max := by
                  apply int64_range_of_natAbs_le hA'Bound
                  have ha : a.natAbs ≤ A := by simpa using hacc
                  dsimp [A', ovAbs]
                  omega
                simp [ckStep, checkedAddInt64, hsrange, combineOptSum]
      have hacc' : match combineOptSum acc ov with
          | none => A' = 0
          | some s => s.natAbs ≤ A' := by
        cases acc with
        | none =>
            cases ov with
            | none =>
                simp [combineOptSum, A', ovAbs]
                exact hacc
            | some v => simp [combineOptSum, A', ovAbs]
        | some a =>
            cases ov with
            | none =>
                have ha : a.natAbs ≤ A := by simpa using hacc
                simpa [combineOptSum, A', ovAbs] using ha
            | some v =>
                have ha : a.natAbs ≤ A := by simpa using hacc
                dsimp [combineOptSum, A', ovAbs]
                omega
      calc
        (ov :: rows).foldl ckStep (.ok acc)
            = rows.foldl ckStep (ckStep (.ok acc) ov) := rfl
        _ = rows.foldl ckStep (.ok (combineOptSum acc ov)) := by rw [hstep]
        _ = .ok (rows.foldl combineOptSum (combineOptSum acc ov)) :=
            ih (acc := combineOptSum acc ov) (A := A') (C := C') hA'C' hacc' hrows'
        _ = .ok ((ov :: rows).foldl combineOptSum acc) := rfl

private def flatStep (acc : Option Int) (ov : Option Int) : Option Int :=
  match ov with
  | none => acc
  | some v => some (acc.getD 0 + v)

private theorem foldl_flatStep_eq (xs : List (Option Int)) (acc : Option Int) :
    xs.foldl flatStep acc = xs.foldl combineOptSum acc := by
  induction xs generalizing acc with
  | nil => rfl
  | cons ov xs ih =>
      change xs.foldl flatStep (flatStep acc ov) = xs.foldl combineOptSum (combineOptSum acc ov)
      cases ov with
      | none =>
          have h : combineOptSum acc none = acc := by cases acc <;> rfl
          rw [h]
          exact ih acc
      | some v =>
          cases acc with
          | none =>
              have h : flatStep none (some v) = some v := by simp [flatStep]
              rw [h]
              have h2 : combineOptSum none (some v) = some v := by rfl
              rw [h2]
              exact ih (some v)
          | some a =>
              have h : flatStep (some a) (some v) = some (a + v) := by rfl
              rw [h]
              have h2 : combineOptSum (some a) (some v) = some (a + v) := by rfl
              rw [h2]
              exact ih (some (a + v))

private theorem foldl_replicate_some_succ (n : Nat) (v : Int) (acc : Option Int) :
    (List.replicate (n + 1) (some v)).foldl combineOptSum acc =
      combineOptSum acc (some (Int.ofNat (n + 1) * v)) := by
  induction n generalizing acc with
  | zero => simp [List.replicate_succ]
  | succ n ih =>
      rw [List.replicate_succ]
      change (List.replicate (n + 1) (some v)).foldl combineOptSum (combineOptSum acc (some v)) =
        combineOptSum acc (some (Int.ofNat (n + 2) * v))
      rw [ih (acc := combineOptSum acc (some v))]
      cases acc with
      | none =>
          simp [combineOptSum]
          change v + ((n + 1 : Nat) : Int) * v = ((n + 2 : Nat) : Int) * v
          have h : ((n + 2 : Nat) : Int) * v = ((n + 1 : Nat) : Int) * v + v := by
            rw [show (n + 2 : Nat) = (n + 1) + 1 by omega, Int.natCast_add, Int.add_mul]
            simp
          omega
      | some a =>
          simp [combineOptSum]
          change a + v + ((n + 1 : Nat) : Int) * v = a + ((n + 2 : Nat) : Int) * v
          have h : ((n + 2 : Nat) : Int) * v = ((n + 1 : Nat) : Int) * v + v := by
            rw [show (n + 2 : Nat) = (n + 1) + 1 by omega, Int.natCast_add, Int.add_mul]
            simp
          omega

private theorem sumOpt_replicate_none (n : Nat) : sumOpt (List.replicate n none) = none := by
  induction n with
  | zero => rfl
  | succ n ih =>
      rw [List.replicate_succ]
      unfold sumOpt
      rw [List.foldl_cons]
      change List.foldl combineOptSum none (List.replicate n none) = none
      exact ih

private theorem evalBlockUnchecked_eq_sumOpt (b : Block) (hv : ValidBlock b) :
    evalBlockUnchecked b = sumOpt (blockToRows b) := by
  cases b with
  | constBlock cnt val =>
      cases val with
      | none =>
          cases cnt with
          | zero => rfl
          | succ n =>
              simp [evalBlockUnchecked]
              exact (sumOpt_replicate_none (n + 1)).symm
      | some v =>
          cases cnt with
          | zero => rfl
          | succ n =>
              change some (Int.ofNat (n + 1) * v) =
                sumOpt (List.replicate (n + 1) (some v))
              unfold sumOpt
              rw [foldl_replicate_some_succ]
              rfl
  | flatBlock xs maxAbs =>
      change xs.foldl flatStep none = sumOpt xs
      unfold sumOpt
      exact foldl_flatStep_eq xs none

private theorem foldl_eval_eq_foldl_sumOpt (bs : List Block) (acc : Option Int)
    (hvalid : ∀ b ∈ bs, ValidBlock b) :
    bs.foldl (fun a b => combineOptSum a (evalBlockUnchecked b)) acc =
      bs.foldl (fun a b => combineOptSum a (sumOpt (blockToRows b))) acc := by
  induction bs generalizing acc with
  | nil => rfl
  | cons b bs ih =>
      have hb := hvalid b (by simp)
      have hbs : ∀ x ∈ bs, ValidBlock x := by
        intro x hx
        exact hvalid x (by simp [hx])
      rw [List.foldl_cons, List.foldl_cons, evalBlockUnchecked_eq_sumOpt b hb]
      exact ih (acc := combineOptSum acc (sumOpt (blockToRows b))) hbs

private theorem rowsAbsSum_replicate_some (n : Nat) (v : Int) :
    rowsAbsSum (List.replicate n (some v)) = n * v.natAbs := by
  induction n with
  | zero => simp [rowsAbsSum]
  | succ n ih =>
      rw [List.replicate_succ]
      change ovAbs (some v) + rowsAbsSum (List.replicate n (some v)) = (n + 1) * v.natAbs
      rw [ih]
      dsimp [ovAbs]
      rw [Nat.add_comm]
      exact (Nat.succ_mul n v.natAbs).symm

private theorem rowsAbsSum_replicate_none (n : Nat) : rowsAbsSum (List.replicate n none) = 0 := by
  induction n with
  | zero => rfl
  | succ n ih =>
      rw [List.replicate_succ]
      change ovAbs none + rowsAbsSum (List.replicate n none) = 0
      dsimp [ovAbs]
      rw [ih]

private theorem rowsAbsSum_flat_le (xs : List (Option Int)) (maxAbs : Nat)
    (hv : ∀ v : Int, some v ∈ xs → v.natAbs ≤ maxAbs) :
    rowsAbsSum xs ≤ nonNullCount xs * maxAbs := by
  induction xs with
  | nil => simp [rowsAbsSum, nonNullCount]
  | cons ov xs ih =>
      have hvTail : ∀ v : Int, some v ∈ xs → v.natAbs ≤ maxAbs := by
        intro v hmem
        exact hv v (by simp [hmem])
      cases ov with
      | none =>
          change ovAbs none + rowsAbsSum xs ≤ nonNullCount (none :: xs) * maxAbs
          dsimp [ovAbs]
          simp [nonNullCount]
          exact ih hvTail
      | some v =>
          have hvabs : v.natAbs ≤ maxAbs := hv v (by simp)
          have ih' := ih hvTail
          change ovAbs (some v) + rowsAbsSum xs ≤ nonNullCount (some v :: xs) * maxAbs
          dsimp [ovAbs, nonNullCount] at ih' ⊢
          simp
          rw [Nat.succ_mul]
          omega

private theorem rowsAbsSum_block_le_bound (b : Block) (hv : ValidBlock b) :
    rowsAbsSum (blockToRows b) ≤ blockAbsBound b := by
  cases b with
  | constBlock cnt val =>
      cases val with
      | none =>
          simp [blockToRows, blockAbsBound]
          exact rowsAbsSum_replicate_none cnt
      | some v =>
          cases cnt with
          | zero => simp [blockToRows, blockAbsBound, rowsAbsSum]
          | succ n =>
              change rowsAbsSum (List.replicate (n + 1) (some v)) ≤ (n + 1) * v.natAbs
              exact Nat.le_of_eq (rowsAbsSum_replicate_some (n + 1) v)
  | flatBlock xs maxAbs =>
      dsimp [ValidBlock] at hv
      change rowsAbsSum xs ≤ nonNullCount xs * maxAbs
      exact rowsAbsSum_flat_le xs maxAbs hv

private theorem rowsAbsSum_flatMap_le (bs : List Block) (hvalid : ∀ b ∈ bs, ValidBlock b) :
    rowsAbsSum (bs.flatMap blockToRows) ≤ (bs.map blockAbsBound).sum := by
  induction bs with
  | nil => simp [rowsAbsSum]
  | cons b bs ih =>
      have hb := hvalid b (by simp)
      have hbs : ∀ x ∈ bs, ValidBlock x := by
        intro x hx
        exact hvalid x (by simp [hx])
      have hbnd := rowsAbsSum_block_le_bound b hb
      have ih' := ih hbs
      rw [List.flatMap_cons, rowsAbsSum_append]
      simp only [List.map_cons, List.sum_cons]
      omega

theorem sound : TinylambOpt.ResearchGoals.ExactIntTotalOverflowGuard.Goal := by
  intro bs hvalid hbound
  have hRows : rowsAbsSum (bs.flatMap blockToRows) ≤ int64Max.toNat := by
    have hle := rowsAbsSum_flatMap_le bs hvalid
    omega
  have href : refCheckedSum (bs.flatMap blockToRows) =
      .ok (sumOpt (bs.flatMap blockToRows)) := by
    rw [refCheckedSum_foldl]
    exact ck_foldl_ok (bs.flatMap blockToRows) none 0 int64Max.toNat
      (by omega) (by rfl) hRows
  have hpar : parExactSum bs = sumOpt (bs.flatMap blockToRows) := by
    unfold parExactSum
    rw [foldl_eval_eq_foldl_sumOpt bs none hvalid]
    exact (sumOpt_flatMap bs).symm
  rw [hpar, href]

end TinylambOpt.ResearchProofs.ExactIntTotalOverflowGuard
