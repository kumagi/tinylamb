import TinylambOpt.ResearchGoals.LeftJoinRightMultiplicityCollapse

/-!
Proved

- `Goal` — Right-side multiplicity collapse for `LEFT OUTER JOIN` without right-column
  references (STATS-CEB C4): replacing materialization of `R` matches with
  per-non-NULL-key multiplicity weights `max 1 mult(k)` (and `1` for NULL join keys)
  yields identical `(COUNT(*), COUNT(L.v), SUM(L.v))`.

The reference model is defined in
`TinylambOpt.ResearchGoals.LeftJoinRightMultiplicityCollapse`.
-/

namespace TinylambOpt.ResearchProofs.LeftJoinRightMultiplicityCollapse

open TinylambOpt.ResearchGoals.LeftJoinRightMultiplicityCollapse

/-- The summation used by `sumOptList`, factored out so that associativity can be proved. -/
private def optF : Option Int → Option Int → Option Int
  | none, y => y
  | x, none => x
  | some a, some b => some (a + b)

private theorem sumOptList_eq_foldl (xs : List (Option Int)) :
    sumOptList xs = xs.foldl optF none := by
  rfl

private theorem optF_none_left (a : Option Int) : optF none a = a := by
  cases a <;> rfl

private theorem optF_none_right (a : Option Int) : optF a none = a := by
  cases a <;> rfl

private theorem optF_assoc (a b c : Option Int) : optF (optF a b) c = optF a (optF b c) := by
  cases a <;> cases b <;> cases c <;> simp [optF] <;> omega

private theorem foldl_optF (vals : List (Option Int)) (acc : Option Int) :
    vals.foldl optF acc = optF acc (vals.foldl optF none) := by
  induction vals generalizing acc with
  | nil => simpa using (optF_none_right acc).symm
  | cons y ys ih =>
      rw [List.foldl_cons, ih (optF acc y)]
      rw [List.foldl_cons, ih (optF none y)]
      rw [optF_none_left y]
      rw [optF_assoc]

private theorem sumOptList_append (xs ys : List (Option Int)) :
    sumOptList (xs ++ ys) = optF (sumOptList xs) (sumOptList ys) := by
  simp only [sumOptList_eq_foldl]
  rw [List.foldl_append]
  exact foldl_optF ys (xs.foldl optF none)

private theorem sumOptList_replicate_none (n : Nat) :
    sumOptList (List.replicate n none) = none := by
  simp only [sumOptList_eq_foldl]
  induction n with
  | zero => rfl
  | succ n ih =>
      rw [List.replicate_succ, List.foldl_cons]
      simp [optF, ih]

private theorem foldl_replicate_some (n : Nat) (x : Int) :
    ∀ s : Int, (List.replicate n (some x)).foldl optF (some s) = some (s + Int.ofNat n * x) := by
  induction n with
  | zero => intro s; simp
  | succ n ih =>
      intro s
      rw [List.replicate_succ, List.foldl_cons]
      change (List.replicate n (some x)).foldl optF (some (s + x)) =
        some (s + Int.ofNat (n + 1) * x)
      rw [ih (s + x)]
      congr 1
      rw [Int.ofNat_eq_natCast, Int.ofNat_eq_natCast, Int.natCast_add, Int.add_mul]
      simp
      rw [Int.add_assoc]
      congr 1
      rw [Int.add_comm]

private theorem sumOptList_replicate_some (n : Nat) (x : Int) (hpos : 0 < n) :
    sumOptList (List.replicate n (some x)) = some (Int.ofNat n * x) := by
  simp only [sumOptList_eq_foldl]
  cases n with
  | zero => omega
  | succ n =>
      rw [List.replicate_succ, List.foldl_cons]
      rw [optF_none_left]
      rw [foldl_replicate_some]
      congr 1
      rw [Int.ofNat_eq_natCast, Int.ofNat_eq_natCast, Int.natCast_add, Int.add_mul]
      simp
      rw [Int.add_comm]

/-- The triple `(COUNT(*), COUNT(L.v), SUM(L.v))`, with addition lifted to the product. -/
private abbrev Acc := Nat × Nat × Option Int

private def accZero : Acc := (0, 0, none)

private def accAdd (a b : Acc) : Acc :=
  (a.1 + b.1, a.2.1 + b.2.1, optF a.2.2 b.2.2)

private theorem accAdd_assoc (a b c : Acc) : accAdd (accAdd a b) c = accAdd a (accAdd b c) := by
  unfold accAdd
  apply Prod.ext
  · simp
    omega
  · apply Prod.ext
    · simp
      omega
    · simpa using optF_assoc a.2.2 b.2.2 c.2.2

private theorem accAdd_zero (a : Acc) : accAdd accZero a = a := by
  unfold accAdd accZero
  simp [optF_none_left]

private theorem accAdd_zero_right (a : Acc) : accAdd a accZero = a := by
  unfold accAdd accZero
  simp [optF_none_right]

private theorem accAdd_foldl (a : Acc) (xs : List LRow) (f : LRow → Acc) (z : Acc) :
    accAdd a (xs.foldl (fun acc x => accAdd acc (f x)) z) =
      xs.foldl (fun acc x => accAdd acc (f x)) (accAdd a z) := by
  induction xs generalizing z with
  | nil => rfl
  | cons x xs ih =>
      rw [List.foldl_cons, List.foldl_cons]
      rw [ih (accAdd z (f x))]
      rw [accAdd_assoc]

private def accOfPairs (ps : List (LRow × Option RRow)) : Acc :=
  (ps.length, (ps.filter (fun lr => lr.1.v.isSome)).length,
    sumOptList (ps.map (fun lr => lr.1.v)))

private theorem accOfPairs_append (ps qs : List (LRow × Option RRow)) :
    accOfPairs (ps ++ qs) = accAdd (accOfPairs ps) (accOfPairs qs) := by
  unfold accOfPairs accAdd
  rw [List.length_append, List.filter_append, List.length_append, List.map_append,
    sumOptList_append]

private def pairsFor (l : LRow) (rs : List RRow) : List (LRow × Option RRow) :=
  match l.k with
  | none => [(l, none)]
  | some lk =>
      let matchesR := rs.filter (fun r => r.k == some lk)
      if matchesR.isEmpty then [(l, none)] else matchesR.map (fun r => (l, some r))

private def weight (l : LRow) (rs : List RRow) : Nat :=
  match l.k with
  | none => 1
  | some lk => max 1 (rightKeyMultiplicity rs lk)

private def rowAcc (l : LRow) (rs : List RRow) : Acc :=
  let w := weight l rs
  match l.v with
  | none => (w, 0, none)
  | some x => (w, w, some (Int.ofNat w * x))

private theorem accOfPairs_singleton_none (l : LRow) :
    accOfPairs [(l, none)] = (1, if l.v.isSome then 1 else 0, l.v) := by
  unfold accOfPairs
  cases l with
  | mk k v pred =>
      cases v <;> simp [sumOptList]

private theorem filter_pairs_const (l : LRow) (ms : List RRow) :
    (ms.map (fun r => (l, some r))).filter (fun lr => lr.1.v.isSome) =
      if l.v.isSome then ms.map (fun r => (l, some r)) else [] := by
  cases l with
  | mk k v pred =>
      cases v <;> induction ms with
      | nil => simp
      | cons r ms ih => simp [ih]

private theorem map_pairs_v (l : LRow) (ms : List RRow) :
    (ms.map (fun r => (l, some r))).map (fun lr => lr.1.v) = ms.map (fun _ => l.v) := by
  induction ms with
  | nil => rfl
  | cons r ms ih => simp [ih]

private theorem accOfPairs_map_right (l : LRow) (ms : List RRow) :
    accOfPairs (ms.map (fun r => (l, some r))) =
      (ms.length, if l.v.isSome then ms.length else 0,
        sumOptList (ms.map (fun _ => l.v))) := by
  unfold accOfPairs
  rw [filter_pairs_const, map_pairs_v]
  cases l with
  | mk k v pred =>
      cases v <;> simp

private theorem map_const_eq_replicate {α : Type} (xs : List RRow) (v : α) :
    xs.map (fun _ => v) = List.replicate xs.length v := by
  induction xs with
  | nil => rfl
  | cons x xs ih => simp [ih, List.replicate_succ]

private theorem sumOptList_map_none (xs : List RRow) :
    sumOptList (xs.map (fun _ => (none : Option Int))) = none := by
  rw [map_const_eq_replicate, sumOptList_replicate_none]

private theorem sumOptList_map_some (xs : List RRow) (x : Int) (hpos : 0 < xs.length) :
    sumOptList (xs.map (fun _ => some x)) = some (Int.ofNat xs.length * x) := by
  rw [map_const_eq_replicate]
  exact sumOptList_replicate_some xs.length x hpos

private theorem accOfPairs_pairsFor (l : LRow) (rs : List RRow) :
    accOfPairs (pairsFor l rs) = rowAcc l rs := by
  rcases l with ⟨k, v, pred⟩
  cases k with
  | none =>
      cases v <;> simp [pairsFor, weight, rowAcc, accOfPairs_singleton_none]
  | some lk =>
      let ms := rs.filter (fun r => r.k == some lk)
      have hlen : ms.length = rightKeyMultiplicity rs lk := rfl
      by_cases hEmpty : ms.isEmpty
      · have h0 : ms.length = 0 := by
          have hnil : ms = [] := (List.isEmpty_iff).mp hEmpty
          simp [hnil]
        have hw : max 1 (rightKeyMultiplicity rs lk) = 1 := by
          rw [← hlen, h0]
          rfl
        cases v <;> simp [pairsFor, ms, hEmpty, weight, rowAcc, accOfPairs_singleton_none, hw]
      · have hpos : 0 < ms.length := by
          apply (List.length_pos_iff).mpr
          intro hnil
          apply hEmpty
          simp [hnil]
        have hw : max 1 (rightKeyMultiplicity rs lk) = ms.length := by
          rw [← hlen]
          exact Nat.max_eq_right (by omega)
        cases v with
        | none =>
            simpa [pairsFor, ms, hEmpty, weight, rowAcc, accOfPairs_map_right, hw] using
              (sumOptList_map_none ms)
        | some x =>
            simpa [pairsFor, ms, hEmpty, weight, rowAcc, accOfPairs_map_right, hw] using
              (sumOptList_map_some ms x hpos)

private theorem accOfPairs_flatMap (fls : List LRow) (rs : List RRow) :
    accOfPairs (fls.flatMap (fun l => pairsFor l rs)) =
      fls.foldl (fun acc l => accAdd acc (rowAcc l rs)) accZero := by
  induction fls with
  | nil => rfl
  | cons l rest ih =>
      change accOfPairs (pairsFor l rs ++ rest.flatMap (fun l => pairsFor l rs)) = _
      rw [accOfPairs_append, accOfPairs_pairsFor, ih]
      rw [List.foldl_cons, accAdd_zero]
      simpa [accAdd_zero_right] using
        (accAdd_foldl (rowAcc l rs) rest (fun l => rowAcc l rs) accZero)

private theorem ref_eq_fold (ls : List LRow) (rs : List RRow) :
    refLeftJoinAgg ls rs =
      (ls.filter LRow.pred).foldl (fun acc l => accAdd acc (rowAcc l rs)) accZero := by
  unfold refLeftJoinAgg
  change accOfPairs (refLeftJoinPairs ls rs) = _
  unfold refLeftJoinPairs
  exact accOfPairs_flatMap (ls.filter LRow.pred) rs

/-- Accumulator used by the optimized scan: the optional sum is stored as `(seen, sum)`. -/
private abbrev Acc4 := Nat × Nat × Bool × Int

private def fourZero : Acc4 := (0, 0, false, 0)

private def fourStep (rs : List RRow) (acc : Acc4) (l : LRow) : Acc4 :=
  let (cStar, cVal, seen, sVal) := acc
  let w := weight l rs
  match l.v with
  | none => (cStar + w, cVal, seen, sVal)
  | some x => (cStar + w, cVal + w, true, sVal + Int.ofNat w * x)

private def fourFinal (acc : Acc4) : Acc :=
  (acc.1, acc.2.1, if acc.2.2.1 then some acc.2.2.2 else none)

private theorem fourStep_correct (rs : List RRow) (acc : Acc4) (l : LRow)
    (hInv : acc.2.2.1 = false → acc.2.2.2 = 0) :
    fourFinal (fourStep rs acc l) = accAdd (fourFinal acc) (rowAcc l rs) ∧
      ((fourStep rs acc l).2.2.1 = false → (fourStep rs acc l).2.2.2 = 0) := by
  rcases acc with ⟨cStar, cVal, seen, sVal⟩
  rcases l with ⟨k, v, pred⟩
  cases v with
  | none =>
      constructor
      · cases seen <;> simp [fourStep, fourFinal, rowAcc, weight, accAdd, optF]
      · intro h
        simpa [fourStep] using hInv h
  | some x =>
      constructor
      · cases seen with
        | false =>
            have hs0 : sVal = 0 := by simpa using hInv rfl
            simp [fourStep, fourFinal, rowAcc, weight, accAdd, optF, hs0]
        | true =>
            simp [fourStep, fourFinal, rowAcc, weight, accAdd, optF]
      · simp [fourStep]

private theorem four_correct_from (fls : List LRow) (rs : List RRow) (acc : Acc4)
    (hInv : acc.2.2.1 = false → acc.2.2.2 = 0) :
    let st := fls.foldl (fourStep rs) acc
    (st.2.2.1 = false → st.2.2.2 = 0) ∧
      fourFinal st =
        fls.foldl (fun a l => accAdd a (rowAcc l rs)) (fourFinal acc) := by
  induction fls generalizing acc with
  | nil => simpa using hInv
  | cons l rest ih =>
      simp only [List.foldl_cons]
      have hstep := fourStep_correct rs acc l hInv
      have hrest := ih (fourStep rs acc l) hstep.2
      simpa [hstep.1] using hrest

private theorem four_correct (fls : List LRow) (rs : List RRow) :
    fourFinal (fls.foldl (fourStep rs) fourZero) =
      fls.foldl (fun a l => accAdd a (rowAcc l rs)) accZero := by
  have h := four_correct_from fls rs fourZero (by simp [fourZero])
  simpa [accZero, fourFinal, fourZero] using h.2

private theorem opt_eq_fold (ls : List LRow) (rs : List RRow) :
    optMultiplicityAgg ls rs =
      (ls.filter LRow.pred).foldl (fun acc l => accAdd acc (rowAcc l rs)) accZero := by
  unfold optMultiplicityAgg
  change fourFinal ((ls.filter LRow.pred).foldl (fourStep rs) fourZero) = _
  exact four_correct (ls.filter LRow.pred) rs

theorem sound : TinylambOpt.ResearchGoals.LeftJoinRightMultiplicityCollapse.Goal := by
  intro ls rs
  exact (opt_eq_fold ls rs).trans (ref_eq_fold ls rs).symm

end TinylambOpt.ResearchProofs.LeftJoinRightMultiplicityCollapse

-- C++ references to this model or its proof.
-- executor/detail/analytics_factorized_join.hpp
