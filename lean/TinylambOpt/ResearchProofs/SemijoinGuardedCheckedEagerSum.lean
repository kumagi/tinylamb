import TinylambOpt.ResearchGoals.SemijoinGuardedCheckedEagerSum

/-!
Proved

- `Goal` — Semijoin-guarded checked eager `SUM` (H2O H5 / STATS-CEB C1 / LDBC L2 /
  BigBench B2): filtering the pre-aggregation side by semijoin existence against `R`
  and bounding post-fanout positive and negative contributions within `[i64Min,
  i64Max]` ensures exact agreement with sequential checked post-join `SUM`, while
  eliminating false overflows caused by unmatched dead keys (`[i64Max, 1]`).

The reference model is defined in
`TinylambOpt.ResearchGoals.SemijoinGuardedCheckedEagerSum`.
-/

namespace TinylambOpt.ResearchProofs.SemijoinGuardedCheckedEagerSum

open TinylambOpt.ResearchGoals.SemijoinGuardedCheckedEagerSum

private theorem inI64_bounds {z : Int} (h : inI64 z = true) : i64Min ≤ z ∧ z ≤ i64Max := by
  simpa [inI64] using h

private theorem inI64_mk {z : Int} (h1 : i64Min ≤ z) (h2 : z ≤ i64Max) : inI64 z = true := by
  simp [inI64, h1, h2]

private theorem i64Min_le_zero : i64Min ≤ (0 : Int) := by
  unfold i64Min
  omega

private theorem zero_le_i64Max : (0 : Int) ≤ i64Max := by
  unfold i64Max
  omega

private theorem inI64_of_nonneg_le {z : Int} (h0 : 0 ≤ z) (h1 : z ≤ i64Max) : inI64 z = true := by
  apply inI64_mk
  · unfold i64Min
    omega
  · exact h1

private theorem inI64_of_nonpos_ge {z : Int} (h0 : z ≤ 0) (h1 : i64Min ≤ z) : inI64 z = true := by
  apply inI64_mk
  · exact h1
  · unfold i64Max
    omega

private def posSum (xs : List Int) : Int := (xs.filter fun x => 0 ≤ x).sum
private def negSum (xs : List Int) : Int := (xs.filter fun x => x < 0).sum

private theorem sum_nonneg_of_forall : ∀ (xs : List Int), (∀ x ∈ xs, 0 ≤ x) → 0 ≤ xs.sum := by
  intro xs h
  induction xs with
  | nil => simp
  | cons a as ih =>
      have ha : 0 ≤ a := h a (by simp)
      have has : ∀ x ∈ as, 0 ≤ x := by intro x hx; exact h x (by simp [hx])
      have hasum := ih has
      simp only [List.sum_cons]
      omega

private theorem posSum_nonneg (xs : List Int) : 0 ≤ posSum xs := by
  unfold posSum
  apply sum_nonneg_of_forall
  intro x hx
  simpa using (List.mem_filter.mp hx).2

private theorem sum_nonpos_of_forall : ∀ (xs : List Int), (∀ x ∈ xs, x ≤ 0) → xs.sum ≤ 0 := by
  intro xs h
  induction xs with
  | nil => simp
  | cons a as ih =>
      have ha : a ≤ 0 := h a (by simp)
      have has : ∀ x ∈ as, x ≤ 0 := by intro x hx; exact h x (by simp [hx])
      have hasum := ih has
      simp only [List.sum_cons]
      omega

private theorem negSum_nonpos (xs : List Int) : negSum xs ≤ 0 := by
  unfold negSum
  apply sum_nonpos_of_forall
  intro x hx
  have hlt : x < 0 := by simpa using (List.mem_filter.mp hx).2
  omega

private theorem posSum_cons (x : Int) (xs : List Int) :
    posSum (x :: xs) = (if 0 ≤ x then x else 0) + posSum xs := by
  by_cases h : 0 ≤ x <;> simp [posSum, h]

private theorem negSum_cons (x : Int) (xs : List Int) :
    negSum (x :: xs) = (if x < 0 then x else 0) + negSum xs := by
  by_cases h : x < 0 <;> simp [negSum, h]

private def cAddStep (acc : Option Int) (x : Int) : Option Int :=
  match acc with
  | none => none
  | some s' => if inI64 x && inI64 (s' + x) then some (s' + x) else none

private def checkedAddFold (xs : List Int) (s : Int) : Option Int :=
  xs.foldl cAddStep (some s)

private theorem checkedAddFold_general : ∀ (xs : List Int) (p₀ n₀ s₀ : Int),
    0 ≤ p₀ → n₀ ≤ 0 → s₀ = p₀ + n₀ →
    inI64 (p₀ + posSum xs) = true → inI64 (n₀ + negSum xs) = true →
    checkedAddFold xs s₀ = some (s₀ + xs.sum) := by
  intro xs
  induction xs with
  | nil =>
      intro p₀ n₀ s₀ hp hn hs hpos hneg
      simp [checkedAddFold]
  | cons x xs ih =>
      intro p₀ n₀ s₀ hp hn hs hpos hneg
      have hps := posSum_nonneg xs
      have hns := negSum_nonpos xs
      have hpc := posSum_cons x xs
      have hnc := negSum_cons x xs
      have hx : inI64 x = true := by
        by_cases hx0 : 0 ≤ x
        · have hpc' : posSum (x :: xs) = x + posSum xs := by simpa [hx0] using hpc
          rw [hpc'] at hpos
          apply inI64_of_nonneg_le hx0
          have hb := inI64_bounds hpos
          omega
        · have hxlt : x < 0 := by omega
          have hnc' : negSum (x :: xs) = x + negSum xs := by simpa [hxlt] using hnc
          rw [hnc'] at hneg
          apply inI64_of_nonpos_ge (by omega)
          have hb := inI64_bounds hneg
          omega
      have hsx : inI64 (s₀ + x) = true := by
        by_cases hx0 : 0 ≤ x
        · have hxlt : ¬ x < 0 := by omega
          have hpc' : posSum (x :: xs) = x + posSum xs := by simpa [hx0] using hpc
          have hnc' : negSum (x :: xs) = negSum xs := by simpa [hxlt] using hnc
          rw [hpc'] at hpos
          rw [hnc'] at hneg
          apply inI64_mk
          · have hb := inI64_bounds hneg
            omega
          · have hb := inI64_bounds hpos
            omega
        · have hxlt : x < 0 := by omega
          have hpc' : posSum (x :: xs) = posSum xs := by simpa [hx0] using hpc
          have hnc' : negSum (x :: xs) = x + negSum xs := by simpa [hxlt] using hnc
          rw [hpc'] at hpos
          rw [hnc'] at hneg
          apply inI64_mk
          · have hb := inI64_bounds hneg
            omega
          · have hb := inI64_bounds hpos
            omega
      have hstep : cAddStep (some s₀) x = some (s₀ + x) := by
        simp [cAddStep, hx, hsx]
      rw [checkedAddFold, List.foldl_cons, hstep]
      simp only [List.sum_cons]
      by_cases hx0 : 0 ≤ x
      · have hxlt : ¬ x < 0 := by omega
        have hpos' : inI64 ((p₀ + x) + posSum xs) = true := by
          have hpc' : posSum (x :: xs) = x + posSum xs := by simpa [hx0] using hpc
          rw [hpc'] at hpos
          have heq : (p₀ + x) + posSum xs = p₀ + (x + posSum xs) := by omega
          rwa [heq]
        have hneg' : inI64 (n₀ + negSum xs) = true := by
          have hnc' : negSum (x :: xs) = negSum xs := by simpa [hxlt] using hnc
          rw [hnc'] at hneg
          exact hneg
        have hp' : 0 ≤ p₀ + x := by omega
        have hs' : s₀ + x = (p₀ + x) + n₀ := by omega
        have ihres := ih (p₀ + x) n₀ (s₀ + x) hp' hn hs' hpos' hneg'
        change checkedAddFold xs (s₀ + x) = some (s₀ + (x + xs.sum))
        rw [show s₀ + (x + xs.sum) = (s₀ + x) + xs.sum by omega]
        exact ihres
      · have hxlt : x < 0 := by omega
        have hpos' : inI64 (p₀ + posSum xs) = true := by
          have hpc' : posSum (x :: xs) = posSum xs := by simpa [hx0] using hpc
          rw [hpc'] at hpos
          exact hpos
        have hneg' : inI64 ((n₀ + x) + negSum xs) = true := by
          have hnc' : negSum (x :: xs) = x + negSum xs := by simpa [hxlt] using hnc
          rw [hnc'] at hneg
          have heq : (n₀ + x) + negSum xs = n₀ + (x + negSum xs) := by omega
          rwa [heq]
        have hn' : n₀ + x ≤ 0 := by omega
        have hs' : s₀ + x = p₀ + (n₀ + x) := by omega
        have ihres := ih p₀ (n₀ + x) (s₀ + x) hp hn' hs' hpos' hneg'
        change checkedAddFold xs (s₀ + x) = some (s₀ + (x + xs.sum))
        rw [show s₀ + (x + xs.sum) = (s₀ + x) + xs.sum by omega]
        exact ihres

private def csStep (acc : Option (Option Int)) (ov : Option Int) : Option (Option Int) :=
  match acc with
  | none => none
  | some cur =>
      match ov with
      | none => some cur
      | some x =>
          if !inI64 x then none
          else
            match cur with
            | none => some (some x)
            | some s =>
                let nxt := s + x
                if inI64 nxt then some (some nxt) else none

private theorem checkedSumList_fold : ∀ vals, checkedSumList vals = vals.foldl csStep (some none) := by
  intro vals
  rfl

private def nonNullVals (vals : List (Option Int)) : List Int := vals.filterMap id

private theorem csStep_none : ∀ vals : List (Option Int), vals.foldl csStep none = none := by
  intro vals
  induction vals with
  | nil => rfl
  | cons a as ih =>
      rw [List.foldl_cons]
      simp [csStep, ih]

private theorem cAddFold_none : ∀ xs : List Int, xs.foldl cAddStep none = none := by
  intro xs
  induction xs with
  | nil => rfl
  | cons a as ih =>
      rw [List.foldl_cons]
      simp [cAddStep, ih]

private theorem checkedSumAux_eq : ∀ (vals : List (Option Int)) (acc : Option (Option Int)),
    vals.foldl csStep acc =
      match acc with
      | none => none
      | some none =>
          match checkedAddFold (nonNullVals vals) 0 with
          | none => none
          | some s => if (nonNullVals vals).isEmpty then some none else some (some s)
      | some (some s) =>
          match checkedAddFold (nonNullVals vals) s with
          | none => none
          | some s' => some (some s') := by
  intro vals
  induction vals with
  | nil =>
      intro acc
      cases acc with
      | none => rfl
      | some cur =>
          cases cur <;> simp [csStep, checkedAddFold, nonNullVals]
  | cons ov rest ih =>
      intro acc
      rw [List.foldl_cons]
      cases acc with
      | none => simp [csStep, csStep_none]
      | some cur =>
          cases cur with
          | none =>
              cases ov with
              | none =>
                  simpa [csStep, nonNullVals] using ih (some none)
              | some x =>
                  by_cases hx : inI64 x = true
                  · have hz : cAddStep (some 0) x = some x := by
                      simp [cAddStep, hx]
                    simpa [csStep, hx, nonNullVals, checkedAddFold, hz] using ih (some (some x))
                  · have hbad : checkedAddFold (x :: List.filterMap id rest) 0 = none := by
                      simp [checkedAddFold, cAddStep, hx, cAddFold_none]
                    simp [csStep, hx, nonNullVals, csStep_none, hbad]
          | some s =>
              cases ov with
              | none =>
                  simpa [csStep, nonNullVals] using ih (some (some s))
              | some x =>
                  by_cases hx : inI64 x = true
                  · by_cases hsx : inI64 (s + x) = true
                    · have hstep : cAddStep (some s) x = some (s + x) := by
                        simp [cAddStep, hx, hsx]
                      simpa [csStep, hx, hsx, nonNullVals, checkedAddFold, hstep] using
                        ih (some (some (s + x)))
                    · have hbad : checkedAddFold (x :: List.filterMap id rest) s = none := by
                        simp [checkedAddFold, cAddStep, hx, hsx, cAddFold_none]
                      simp [csStep, hx, hsx, nonNullVals, csStep_none, hbad]
                  · have hbad : checkedAddFold (x :: List.filterMap id rest) s = none := by
                      simp [checkedAddFold, cAddStep, hx, cAddFold_none]
                    simp [csStep, hx, nonNullVals, csStep_none, hbad]

private theorem checkedSumList_eq (vals : List (Option Int)) :
    inI64 (posSum (nonNullVals vals)) = true →
    inI64 (negSum (nonNullVals vals)) = true →
    checkedSumList vals =
      if (nonNullVals vals).isEmpty then some none else some (some (nonNullVals vals).sum) := by
  intro hpos hneg
  change vals.foldl csStep (some none) = _
  rw [checkedSumAux_eq]
  simp only
  have hf := checkedAddFold_general (nonNullVals vals) 0 0 0 (by omega) (by omega) (by omega)
      (by simpa using hpos) (by simpa using hneg)
  simp at hf
  rw [hf]


private theorem nonNullVals_flatMap (ls : List LRow) (f : LRow → List (Option Int)) :
    nonNullVals (ls.flatMap f) = ls.flatMap (fun l => nonNullVals (f l)) := by
  simp [nonNullVals, List.filterMap_flatMap]

private theorem nonNullVals_replicate (n : Nat) (v : Option Int) :
    nonNullVals (List.replicate n v) =
      match v with
      | some x => List.replicate n x
      | none => [] := by
  induction n with
  | zero => cases v <;> simp [nonNullVals]
  | succ n ih => cases v <;> simp [nonNullVals, ih]

private def matchedStep (lk : Int) (v : Option Int) (r : RRow) : Option (Option Int) :=
  match r.k with
  | some rk => if lk == rk then some v else none
  | none => none

private def matchedVals (l : LRow) (rs : List RRow) : List (Option Int) :=
  match l.k with
  | some lk => rs.filterMap (matchedStep lk l.v)
  | none => []

private theorem matchedVals_none (v : Option Int) (rs : List RRow) :
    matchedVals { k := none, v := v } rs = [] := by
  simp [matchedVals]

private theorem filterMap_matchedStep (lk : Int) (v : Option Int) (rs : List RRow) :
    rs.filterMap (matchedStep lk v) = List.replicate (fanout rs lk) v := by
  induction rs with
  | nil => simp [matchedStep, fanout]
  | cons r rs ih =>
      cases r with
      | mk rk =>
          cases rk with
          | none =>
              rw [List.filterMap_cons]
              simp [matchedStep, fanout, ih]
          | some rk =>
              by_cases h : lk == rk
              · have heq : lk = rk := by simpa using h
                subst lk
                rw [List.filterMap_cons]
                simp [matchedStep, fanout, ih]
                rw [List.replicate_succ]
              · have hneq : lk ≠ rk := by simpa using h
                have hneq' : rk ≠ lk := by intro hh; exact hneq hh.symm
                have h' : ((some rk : Option Int) == some lk) = false := by simp [hneq']
                rw [List.filterMap_cons]
                simp [matchedStep, fanout, h, h', ih]

private theorem matchedVals_eq_replicate (lk : Int) (v : Option Int) (rs : List RRow) :
    matchedVals { k := some lk, v := v } rs = List.replicate (fanout rs lk) v := by
  simp [matchedVals]
  exact filterMap_matchedStep lk v rs

private theorem refVals_eq_matchedVals (l : LRow) (rs : List RRow) :
    (rs.filterMap fun r =>
      match l.k, r.k with
      | some lk, some rk => if lk == rk then some l.v else none
      | _, _ => none) = matchedVals l rs := by
  cases l with
  | mk k v =>
    cases k with
    | none =>
        have hf :
            (fun r : RRow =>
              match (none : Option Int), r.k with
              | some lk, some rk => if lk == rk then some v else none
              | _, _ => none) = (fun _ : RRow => (none : Option (Option Int))) := by
          funext r
          cases r with
          | mk rk => simp
        rw [hf]
        simp [matchedVals]
    | some lk =>
        have hf :
            (fun r : RRow =>
              match some lk, r.k with
              | some lk, some rk => if lk == rk then some v else none
              | _, _ => none) = matchedStep lk v := by
          funext r
          cases r with
          | mk rk =>
              cases rk <;> simp [matchedStep]
        rw [hf]
        simp [matchedVals]

private theorem flat_refVals (ls : List LRow) (rs : List RRow) :
    (ls.flatMap fun l =>
      rs.filterMap fun r =>
        match l.k, r.k with
        | some lk, some rk => if lk == rk then some l.v else none
        | _, _ => none) = ls.flatMap (fun l => matchedVals l rs) := by
  induction ls with
  | nil => rfl
  | cons l ls ih =>
      rw [List.flatMap_cons, List.flatMap_cons]
      rw [refVals_eq_matchedVals, ih]

private theorem matchedVals_nn (l : LRow) (rs : List RRow) :
    nonNullVals (matchedVals l rs) =
      match l.k, l.v with
      | some lk, some x => List.replicate (fanout rs lk) x
      | _, _ => [] := by
  cases l with
  | mk k v =>
    cases k with
    | none =>
        rw [matchedVals_none]
        simp [nonNullVals]
    | some lk =>
        rw [matchedVals_eq_replicate]
        cases v <;> simp [nonNullVals_replicate]

private def weightedVals (ls : List LRow) (rs : List RRow) : List Int :=
  ls.flatMap fun l =>
    match l.k, l.v with
    | some lk, some x => List.replicate (fanout rs lk) x
    | _, _ => []

private theorem flat_nonNullVals (ls : List LRow) (rs : List RRow) :
    nonNullVals (ls.flatMap fun l =>
      rs.filterMap fun r =>
        match l.k, r.k with
        | some lk, some rk => if lk == rk then some l.v else none
        | _, _ => none) = weightedVals ls rs := by
  rw [flat_refVals]
  simp [nonNullVals_flatMap, weightedVals, matchedVals_nn]

private theorem posSum_append (a b : List Int) : posSum (a ++ b) = posSum a + posSum b := by
  simp [posSum]

private theorem negSum_append (a b : List Int) : negSum (a ++ b) = negSum a + negSum b := by
  simp [negSum]

private theorem posSum_replicate (x : Int) (n : Nat) :
    posSum (List.replicate n x) = if 0 ≤ x then x * Int.ofNat n else 0 := by
  induction n with
  | zero => simp [posSum]
  | succ n ih =>
      rw [List.replicate_succ, posSum_cons, ih]
      by_cases h : 0 ≤ x
      · simp [h]
        rw [Int.mul_add, Int.mul_one]
        omega
      · simp [h]

private theorem negSum_replicate (x : Int) (n : Nat) :
    negSum (List.replicate n x) = if x < 0 then x * Int.ofNat n else 0 := by
  induction n with
  | zero => simp [negSum]
  | succ n ih =>
      rw [List.replicate_succ, negSum_cons, ih]
      by_cases h : x < 0
      · simp [h]
        rw [Int.mul_add, Int.mul_one]
        omega
      · simp [h]

private def wpStep (rs : List RRow) (acc : Int) (l : LRow) : Int :=
  match l.k, l.v with
  | some k, some x =>
      let f := fanout rs k
      if 0 < f && 0 ≤ x then acc + x * Int.ofNat f else acc
  | _, _ => acc

private theorem weightedPosTotal_eq_posSum : ∀ (ls : List LRow) (rs : List RRow) (acc : Int),
    ls.foldl (fun a l => wpStep rs a l) acc = acc + posSum (weightedVals ls rs) := by
  intro ls
  induction ls with
  | nil =>
      intro rs acc
      simp [weightedVals, posSum]
  | cons l rest ih =>
      intro rs acc
      rw [List.foldl_cons]
      cases l with
      | mk k v =>
        cases k with
        | none =>
            have hs : wpStep rs acc { k := none, v := v } = acc := by simp [wpStep]
            rw [hs, ih rs acc]
            simp [weightedVals, posSum]
        | some lk =>
            cases v with
            | none =>
                have hs : wpStep rs acc { k := some lk, v := none } = acc := by simp [wpStep]
                rw [hs, ih rs acc]
                simp [weightedVals, posSum]
            | some x =>
                by_cases hf : 0 < fanout rs lk
                · by_cases hx : 0 ≤ x
                  · have hs : wpStep rs acc { k := some lk, v := some x } =
                        acc + x * Int.ofNat (fanout rs lk) := by
                      simp [wpStep, hf, hx]
                    rw [hs, ih rs (acc + x * Int.ofNat (fanout rs lk))]
                    simp [weightedVals, posSum_append, posSum_replicate, hx]
                    omega
                  · have hs : wpStep rs acc { k := some lk, v := some x } = acc := by
                      simp [wpStep, hf, hx]
                    rw [hs, ih rs acc]
                    simp [weightedVals, posSum_append, posSum_replicate, hx]
                · have hf0 : fanout rs lk = 0 := by omega
                  have hs : wpStep rs acc { k := some lk, v := some x } = acc := by
                    simp [wpStep, hf]
                  rw [hs, ih rs acc]
                  simp [weightedVals, posSum_append, posSum_replicate, hf0]

private def wnStep (rs : List RRow) (acc : Int) (l : LRow) : Int :=
  match l.k, l.v with
  | some k, some x =>
      let f := fanout rs k
      if 0 < f && x < 0 then acc + x * Int.ofNat f else acc
  | _, _ => acc

private theorem weightedNegTotal_eq_negSum : ∀ (ls : List LRow) (rs : List RRow) (acc : Int),
    ls.foldl (fun a l => wnStep rs a l) acc = acc + negSum (weightedVals ls rs) := by
  intro ls
  induction ls with
  | nil =>
      intro rs acc
      simp [weightedVals, negSum]
  | cons l rest ih =>
      intro rs acc
      rw [List.foldl_cons]
      cases l with
      | mk k v =>
        cases k with
        | none =>
            have hs : wnStep rs acc { k := none, v := v } = acc := by simp [wnStep]
            rw [hs, ih rs acc]
            simp [weightedVals, negSum]
        | some lk =>
            cases v with
            | none =>
                have hs : wnStep rs acc { k := some lk, v := none } = acc := by simp [wnStep]
                rw [hs, ih rs acc]
                simp [weightedVals, negSum]
            | some x =>
                by_cases hf : 0 < fanout rs lk
                · by_cases hx : x < 0
                  · have hs : wnStep rs acc { k := some lk, v := some x } =
                        acc + x * Int.ofNat (fanout rs lk) := by
                      simp [wnStep, hf, hx]
                    rw [hs, ih rs (acc + x * Int.ofNat (fanout rs lk))]
                    simp [weightedVals, negSum_append, negSum_replicate, hx]
                    omega
                  · have hs : wnStep rs acc { k := some lk, v := some x } = acc := by
                      simp [wnStep, hf, hx]
                    rw [hs, ih rs acc]
                    simp [weightedVals, negSum_append, negSum_replicate, hx]
                · have hf0 : fanout rs lk = 0 := by omega
                  have hs : wnStep rs acc { k := some lk, v := some x } = acc := by
                    simp [wnStep, hf]
                  rw [hs, ih rs acc]
                  simp [weightedVals, negSum_append, negSum_replicate, hf0]


private theorem weightedPosTotal_eq_fold (ls : List LRow) (rs : List RRow) :
    weightedPosTotal ls rs = ls.foldl (fun a l => wpStep rs a l) 0 := by
  rfl

private theorem weightedNegTotal_eq_fold (ls : List LRow) (rs : List RRow) :
    weightedNegTotal ls rs = ls.foldl (fun a l => wnStep rs a l) 0 := by
  rfl

private theorem assumptions_to_bounds (ls : List LRow) (rs : List RRow)
    (hp : inI64 (weightedPosTotal ls rs) = true)
    (hn : inI64 (weightedNegTotal ls rs) = true) :
    inI64 (posSum (weightedVals ls rs)) = true ∧
    inI64 (negSum (weightedVals ls rs)) = true := by
  constructor
  · rw [weightedPosTotal_eq_fold, weightedPosTotal_eq_posSum] at hp
    simpa using hp
  · rw [weightedNegTotal_eq_fold, weightedNegTotal_eq_negSum] at hn
    simpa using hn

private theorem sum_eq_posSum_add_negSum (xs : List Int) :
    xs.sum = posSum xs + negSum xs := by
  induction xs with
  | nil => simp [posSum, negSum]
  | cons x xs ih =>
      by_cases h : 0 ≤ x
      · have hn : ¬ x < 0 := by omega
        simp [posSum_cons, negSum_cons, h, hn, ih]
        omega
      · have hlt : x < 0 := by omega
        simp [posSum_cons, negSum_cons, h, hlt, ih]
        omega

private theorem weightedVals_sum (ls : List LRow) (rs : List RRow) :
    (weightedVals ls rs).sum = weightedPosTotal ls rs + weightedNegTotal ls rs := by
  rw [sum_eq_posSum_add_negSum]
  have hp := weightedPosTotal_eq_posSum ls rs 0
  have hn := weightedNegTotal_eq_negSum ls rs 0
  have hp' : weightedPosTotal ls rs = posSum (weightedVals ls rs) := by
    rw [weightedPosTotal_eq_fold]
    simpa using hp
  have hn' : weightedNegTotal ls rs = negSum (weightedVals ls rs) := by
    rw [weightedNegTotal_eq_fold]
    simpa using hn
  omega

private theorem weightedPosTotal_eq_posSum' (ls : List LRow) (rs : List RRow) :
    weightedPosTotal ls rs = posSum (weightedVals ls rs) := by
  rw [weightedPosTotal_eq_fold]
  simpa using (weightedPosTotal_eq_posSum ls rs 0)

private theorem weightedNegTotal_eq_negSum' (ls : List LRow) (rs : List RRow) :
    weightedNegTotal ls rs = negSum (weightedVals ls rs) := by
  rw [weightedNegTotal_eq_fold]
  simpa using (weightedNegTotal_eq_negSum ls rs 0)

private theorem weightedVals_sum_inI64 (ls : List LRow) (rs : List RRow)
    (hp : inI64 (weightedPosTotal ls rs) = true)
    (hn : inI64 (weightedNegTotal ls rs) = true) :
    inI64 (weightedVals ls rs).sum = true := by
  rw [weightedVals_sum, weightedPosTotal_eq_posSum', weightedNegTotal_eq_negSum']
  apply inI64_mk
  · have pp := posSum_nonneg (weightedVals ls rs)
    have nb := inI64_bounds (assumptions_to_bounds ls rs hp hn).2
    have hsum := sum_eq_posSum_add_negSum (weightedVals ls rs)
    omega
  · have pb := inI64_bounds (assumptions_to_bounds ls rs hp hn).1
    have nn := negSum_nonpos (weightedVals ls rs)
    have hsum := sum_eq_posSum_add_negSum (weightedVals ls rs)
    omega

private theorem refFlatJoinSum_eq (ls : List LRow) (rs : List RRow)
    (hp : inI64 (weightedPosTotal ls rs) = true)
    (hn : inI64 (weightedNegTotal ls rs) = true) :
    refFlatJoinSum ls rs =
      if (weightedVals ls rs).isEmpty then some none
      else some (some (weightedPosTotal ls rs + weightedNegTotal ls rs)) := by
  change checkedSumList (ls.flatMap fun l =>
      rs.filterMap fun r =>
        match l.k, r.k with
        | some lk, some rk => if lk == rk then some l.v else none
        | _, _ => none) = _
  have hc := checkedSumList_eq (ls.flatMap fun l =>
      rs.filterMap fun r =>
        match l.k, r.k with
        | some lk, some rk => if lk == rk then some l.v else none
        | _, _ => none)
  rw [hc]
  · rw [flat_nonNullVals ls rs]
    rw [weightedVals_sum]
  · rw [flat_nonNullVals ls rs]
    exact (assumptions_to_bounds ls rs hp hn).1
  · rw [flat_nonNullVals ls rs]
    exact (assumptions_to_bounds ls rs hp hn).2


private def liveRows (ls : List LRow) (rs : List RRow) : List LRow :=
  ls.filter fun l =>
    match l.k with
    | some k => rightHasKey rs k
    | none => false

private def keyStep (acc : List Int) (l : LRow) : List Int :=
  match l.k with
  | some k => addUniqueInt acc k
  | none => acc

private def collectKeys (liveL : List LRow) : List Int :=
  liveL.foldl keyStep []

private def ksStep (k : Int) (st : Bool × Int) (l : LRow) : Bool × Int :=
  if l.k == some k then
    match l.v with
    | some x => (true, st.2 + x)
    | none => st
  else st

private def keySum (_rs : List RRow) (liveL : List LRow) (k : Int) : Bool × Int :=
  liveL.foldl (ksStep k) (false, 0)

private def sbStep (rs : List RRow) (liveL : List LRow) (st : Bool × Int) (k : Int) : Bool × Int :=
  let (seen, tot) := st
  let (kSeen, kSum) := keySum rs liveL k
  if kSeen then (true, tot + kSum * Int.ofNat (fanout rs k)) else (seen, tot)

private def sumByKeys (rs : List RRow) (liveL : List LRow) (ks : List Int) : Bool × Int :=
  ks.foldl (sbStep rs liveL) (false, 0)

private theorem contains_true {acc : List Int} {x : Int} : acc.contains x = true → x ∈ acc := by
  intro h
  change List.elem x acc = true at h
  rw [List.elem_eq_mem] at h
  exact of_decide_eq_true h

private theorem contains_of_mem {acc : List Int} {x : Int} : x ∈ acc → acc.contains x = true := by
  intro h
  change List.elem x acc = true
  rw [List.elem_eq_mem]
  simp [h]

private theorem addUniqueInt_mem_self (acc : List Int) (x : Int) : x ∈ addUniqueInt acc x := by
  unfold addUniqueInt
  by_cases hc : acc.contains x = true
  · rw [ite_eq_left hc]
    exact contains_true hc
  · rw [ite_eq_right hc]
    simp

private theorem addUniqueInt_mem_of_mem (acc : List Int) (x y : Int) (h : y ∈ acc) :
    y ∈ addUniqueInt acc x := by
  unfold addUniqueInt
  by_cases hc : acc.contains x = true
  · rw [ite_eq_left hc]
    exact h
  · rw [ite_eq_right hc]
    simp [h]

private theorem addUniqueInt_nodup (acc : List Int) (x : Int) :
    acc.Nodup → (addUniqueInt acc x).Nodup := by
  intro h
  unfold addUniqueInt
  by_cases hc : acc.contains x = true
  · rw [ite_eq_left hc]
    exact h
  · rw [ite_eq_right hc]
    have hnot : x ∉ acc := by
      intro hh
      exact hc (contains_of_mem hh)
    exact List.nodup_cons.mpr ⟨hnot, h⟩

private theorem collectKeysFold_nodup : ∀ (liveL : List LRow) (acc : List Int),
    acc.Nodup → (liveL.foldl keyStep acc).Nodup := by
  intro liveL
  induction liveL with
  | nil =>
      intro acc h
      simpa using h
  | cons l rest ih =>
      intro acc h
      rw [List.foldl_cons]
      cases l with
      | mk k v =>
          cases k with
          | none => exact ih acc h
          | some k => exact ih (addUniqueInt acc k) (addUniqueInt_nodup acc k h)

private theorem collectKeysFold_mem_acc : ∀ (liveL : List LRow) (acc : List Int) (x : Int),
    x ∈ acc → x ∈ liveL.foldl keyStep acc := by
  intro liveL
  induction liveL with
  | nil =>
      intro acc x h
      simpa using h
  | cons l rest ih =>
      intro acc x h
      rw [List.foldl_cons]
      cases l with
      | mk k v =>
          cases k with
          | none => exact ih acc x h
          | some k => exact ih (addUniqueInt acc k) x (addUniqueInt_mem_of_mem acc k x h)

private theorem collectKeysFold_cover : ∀ (liveL : List LRow) (acc : List Int) (l : LRow) (k : Int),
    l ∈ liveL → l.k = some k → k ∈ liveL.foldl keyStep acc := by
  intro liveL
  induction liveL with
  | nil =>
      intro acc l k h
      simpa using h
  | cons hd rest ih =>
      intro acc l k hmem hk
      rw [List.foldl_cons]
      simp only [List.mem_cons] at hmem
      rcases hmem with rfl | hmem
      · cases l with
        | mk lk v =>
            cases lk with
            | none => simp at hk
            | some lk' =>
                simp at hk
                subst k
                exact collectKeysFold_mem_acc rest (addUniqueInt acc lk') lk' (addUniqueInt_mem_self acc lk')
      · cases hd with
        | mk lk v =>
            cases lk with
            | none => exact ih acc l k hmem hk
            | some lk => exact ih (addUniqueInt acc lk) l k hmem hk

private theorem collectKeys_nodup (liveL : List LRow) : (collectKeys liveL).Nodup := by
  simpa [collectKeys] using (collectKeysFold_nodup liveL [] List.nodup_nil)

private theorem collectKeys_cover (liveL : List LRow) (l : LRow) (k : Int) :
    l ∈ liveL → l.k = some k → k ∈ collectKeys liveL := by
  simpa [collectKeys] using (collectKeysFold_cover liveL [] l k)

private theorem foldl_or_add : ∀ {α : Type} (xs : List α) (g : α → Bool) (h : α → Int) (b : Bool) (t : Int),
    xs.foldl (fun st a => (st.1 || g a, st.2 + h a)) (b, t) =
      (b || xs.any g, t + (xs.map h).sum) := by
  intro α xs
  induction xs with
  | nil =>
      intro g h b t
      simp
  | cons a as ih =>
      intro g h b t
      rw [List.foldl_cons]
      rw [ih g h (b || g a) (t + h a)]
      simp [Bool.or_assoc, Int.add_assoc]

private def kBool (k : Int) (l : LRow) : Bool := l.k == some k && l.v.isSome

private def kVal (k : Int) (l : LRow) : Int :=
  if l.k == some k then
    match l.v with
    | some x => x
    | none => 0
  else 0

private theorem ksStep_eq (k : Int) (st : Bool × Int) (l : LRow) :
    ksStep k st l = (st.1 || kBool k l, st.2 + kVal k l) := by
  cases l with
  | mk lk v =>
      cases v <;> by_cases h : lk == some k <;> simp [ksStep, kBool, kVal, h]

private theorem keySumAux_eq : ∀ (liveL : List LRow) (k : Int) (b : Bool) (t : Int),
    liveL.foldl (ksStep k) (b, t) = (b || liveL.any (kBool k), t + (liveL.map (kVal k)).sum) := by
  intro liveL
  induction liveL with
  | nil =>
      intro k b t
      simp
  | cons l rest ih =>
      intro k b t
      rw [List.foldl_cons, ksStep_eq]
      rw [ih k (b || kBool k l) (t + kVal k l)]
      simp [Bool.or_assoc, Int.add_assoc]

private theorem keySum_eq (rs : List RRow) (liveL : List LRow) (k : Int) :
    keySum rs liveL k = (liveL.any (kBool k), (liveL.map (kVal k)).sum) := by
  unfold keySum
  simpa using (keySumAux_eq liveL k false 0)

private theorem kSeen_of_mem (rs : List RRow) (liveL : List LRow) (k : Int) :
    (keySum rs liveL k).1 = true ↔ ∃ l ∈ liveL, l.k = some k ∧ l.v.isSome := by
  rw [keySum_eq]
  simp only [List.any_eq_true]
  constructor
  · rintro ⟨l, hl, hk⟩
    exact ⟨l, hl, by
      by_cases hkv : l.v.isSome
      · simpa [kBool, hkv] using hk
      · simp [kBool, hkv] at hk⟩
  · rintro ⟨l, hl, hk, hv⟩
    exact ⟨l, hl, by simp [kBool, hk, hv]⟩

private theorem kSeenAny_eq (rs : List RRow) (liveL : List LRow) (ks : List Int) :
    (∀ l ∈ liveL, l.v.isSome → ∃ k ∈ ks, l.k = some k) →
    ks.any (fun k => (keySum rs liveL k).1) = liveL.any (fun l => l.v.isSome) := by
  intro hcov
  by_cases h : ks.any (fun k => (keySum rs liveL k).1) = true
  · have hl : liveL.any (fun l => l.v.isSome) = true := by
      rw [List.any_eq_true] at h ⊢
      rcases h with ⟨k, hk, hseen⟩
      rcases (kSeen_of_mem rs liveL k).mp hseen with ⟨l, hl, _, hv⟩
      exact ⟨l, hl, hv⟩
    simp [h, hl]
  · have hl : liveL.any (fun l => l.v.isSome) ≠ true := by
      intro hl
      rw [List.any_eq_true] at hl
      rcases hl with ⟨l, hlmem, hv⟩
      rcases hcov l hlmem hv with ⟨k, hk, hkl⟩
      have hseen : (keySum rs liveL k).1 = true :=
        (kSeen_of_mem rs liveL k).mpr ⟨l, hlmem, hkl, hv⟩
      apply h
      rw [List.any_eq_true]
      exact ⟨k, hk, hseen⟩
    have hleft : ks.any (fun k => (keySum rs liveL k).1) = false := by
      cases x : ks.any (fun k => (keySum rs liveL k).1) <;> simp_all
    have hright : liveL.any (fun l => l.v.isSome) = false := by
      cases x : liveL.any (fun l => l.v.isSome) <;> simp_all
    rw [hleft, hright]

private theorem sum_map_eq_zero_of_not_mem {ks : List Int} (k0 : Int) (a : Int)
    (h : ∀ x ∈ ks, x ≠ k0) :
    (ks.map fun x => if x == k0 then a else 0).sum = 0 := by
  induction ks with
  | nil => rfl
  | cons y ys ih =>
      have hy : y ≠ k0 := h y (by simp)
      have hys : ∀ x ∈ ys, x ≠ k0 := by intro x hx; exact h x (by simp [hx])
      have hyb : ¬ ((y == k0) = true) := by
        intro hh
        exact hy (by simpa using hh)
      rw [List.map_cons, List.sum_cons, ite_eq_right hyb]
      rw [ih hys]
      simp

private theorem sum_single_of_nodup {ks : List Int} (hnd : ks.Nodup) (k0 : Int) (hk : k0 ∈ ks) (a : Int) :
    (ks.map fun k => if k == k0 then a else 0).sum = a := by
  induction ks with
  | nil => cases hk
  | cons k ks ih =>
      simp only [List.mem_cons] at hk
      have hnd' : ks.Nodup := (List.nodup_cons.mp hnd).2
      rcases hk with hk | hk
      · subst k
        have hnot : k0 ∉ ks := (List.nodup_cons.mp hnd).1
        have hzero : (ks.map fun x => if x == k0 then a else 0).sum = 0 :=
          sum_map_eq_zero_of_not_mem k0 a (by
            intro x hx heq
            exact hnot (by simpa [heq] using hx))
        rw [List.map_cons, List.sum_cons]
        have hkt : (k0 == k0) = true := by simp
        rw [ite_eq_left hkt, hzero]
        simp
      · have hne : k ≠ k0 := by
          intro heq
          exact (List.nodup_cons.mp hnd).1 (by simpa [heq] using hk)
        have hkb : ¬ ((k == k0) = true) := by
          intro hh
          exact hne (by simpa using hh)
        rw [List.map_cons, List.sum_cons, ite_eq_right hkb]
        rw [ih hnd' hk]
        simp


private def sbVal (rs : List RRow) (liveL : List LRow) (k : Int) : Int :=
  let s := keySum rs liveL k
  if s.1 then s.2 * Int.ofNat (fanout rs k) else 0

private def rowVal (rs : List RRow) (l : LRow) : Int :=
  match l.k, l.v with
  | some lk, some x => x * Int.ofNat (fanout rs lk)
  | _, _ => 0

private theorem beq_comm_int (a b : Int) : (a == b) = (b == a) := by
  by_cases h : a = b
  · subst b
    simp
  · have h' : b ≠ a := by intro hh; exact h hh.symm
    have hab : (a == b) = false := by simp [h]
    have hba : (b == a) = false := by simp [h']
    rw [hab, hba]

private theorem kBool_some_some (k lk x : Int) :
    kBool k ({ k := some lk, v := some x } : LRow) = (k == lk) := by
  unfold kBool
  by_cases h : k == lk
  · have hlk : (lk == k) = true := by rw [beq_comm_int]; exact h
    simp [hlk, h]
  · have hlk : ¬ ((lk == k) = true) := by
      intro hh
      have : (k == lk) = true := by rw [beq_comm_int]; exact hh
      exact h this
    simp [hlk, h]

private theorem kVal_some_some (k lk x : Int) :
    kVal k ({ k := some lk, v := some x } : LRow) = if k == lk then x else 0 := by
  unfold kVal
  by_cases h : k == lk
  · have hlk : (lk == k) = true := by rw [beq_comm_int]; exact h
    simp [hlk, h]
  · have hlk : ¬ ((lk == k) = true) := by
      intro hh
      have : (k == lk) = true := by rw [beq_comm_int]; exact hh
      exact h this
    simp [hlk, h]

private theorem kVal_zero_of_kBool_false (k : Int) (l : LRow) (h : kBool k l = false) :
    kVal k l = 0 := by
  cases l with
  | mk lk v =>
      cases v with
      | none => simp [kVal]
      | some x =>
          by_cases hk : lk = some k
          · simp [kBool, hk] at h
          · simp [kVal, hk]

private theorem sum_kVal_zero (k : Int) (rest : List LRow) :
    rest.any (kBool k) = false → (rest.map (kVal k)).sum = 0 := by
  intro h
  induction rest with
  | nil => rfl
  | cons l rest ih =>
      have hbl : kBool k l = false := by
        cases hbl' : kBool k l <;> simp [List.any_cons, hbl'] at h ⊢
      have htail : rest.any (kBool k) = false := by
        cases ht : rest.any (kBool k) <;> simp [List.any_cons, hbl, ht] at h ⊢
      have hvl : kVal k l = 0 := kVal_zero_of_kBool_false k l hbl
      rw [List.map_cons, List.sum_cons, hvl]
      rw [ih htail]
      simp

private theorem kSum_zero_of_not_kSeen (rs : List RRow) (rest : List LRow) (k : Int) :
    (keySum rs rest k).1 = false → (keySum rs rest k).2 = 0 := by
  intro h
  rw [keySum_eq] at h ⊢
  exact sum_kVal_zero k rest h

private theorem keySum_cons (rs : List RRow) (rest : List LRow) (k : Int) (l : LRow) :
    keySum rs (l :: rest) k =
      ((keySum rs rest k).1 || kBool k l, (keySum rs rest k).2 + kVal k l) := by
  unfold keySum
  rw [List.foldl_cons, ksStep_eq]
  simp only [Bool.false_or, Int.zero_add]
  rw [keySumAux_eq]
  rw [show (rest.foldl (ksStep k) (false, 0)) =
      (rest.any (kBool k), (rest.map (kVal k)).sum) by
        simpa using (keySumAux_eq rest k false 0)]
  apply Prod.ext
  · simp [Bool.or_comm]
  · omega

private theorem sbVal_cons_some (rs : List RRow) (rest : List LRow) (lk x k : Int) :
    sbVal rs ({ k := some lk, v := some x } :: rest) k =
      sbVal rs rest k + (if k == lk then x * Int.ofNat (fanout rs k) else 0) := by
  unfold sbVal
  rw [keySum_cons]
  rw [kBool_some_some, kVal_some_some]
  by_cases hkl : k == lk
  · simp [hkl]
    cases hs : (keySum rs rest k).1
    · have hz := kSum_zero_of_not_kSeen rs rest k hs
      simp [hs, hz, hkl]
    · rw [Int.add_mul]
      simp
  · simp [hkl]

private theorem sbVal_cons_none (rs : List RRow) (rest : List LRow) (lk k : Int) :
    sbVal rs ({ k := some lk, v := none } :: rest) k = sbVal rs rest k := by
  unfold sbVal
  rw [keySum_cons]
  have hkb : kBool k ({ k := some lk, v := none } : LRow) = false := by
    simp [kBool]
  have hkv : kVal k ({ k := some lk, v := none } : LRow) = 0 := by
    simp [kVal]
  rw [hkb, hkv]
  simp

private theorem sbVal_cons_keynone (rs : List RRow) (rest : List LRow) (v k : Int) :
    sbVal rs ({ k := none, v := some v } :: rest) k = sbVal rs rest k := by
  unfold sbVal
  rw [keySum_cons]
  have hkb : kBool k ({ k := none, v := some v } : LRow) = false := by
    simp [kBool]
  have hkv : kVal k ({ k := none, v := some v } : LRow) = 0 := by
    simp [kVal]
  rw [hkb, hkv]
  simp

private theorem sum_map_add {α : Type} (xs : List α) (f g : α → Int) :
    (xs.map fun a => f a + g a).sum = (xs.map f).sum + (xs.map g).sum := by
  induction xs with
  | nil => simp
  | cons a as ih =>
      simp only [List.map_cons, List.sum_cons]
      rw [ih]
      omega

private theorem sum_map_zero {α : Type} (xs : List α) : (xs.map fun _ => (0 : Int)).sum = 0 := by
  induction xs with
  | nil => rfl
  | cons a as ih =>
      simp only [List.map_cons, List.sum_cons, ih]
      omega

private theorem group_sum : ∀ (rs : List RRow) (liveL : List LRow) (ks : List Int),
    ks.Nodup →
    (∀ l ∈ liveL, l.v.isSome → ∃ k ∈ ks, l.k = some k) →
    (ks.map fun k => sbVal rs liveL k).sum =
      (liveL.map fun l => rowVal rs l).sum := by
  intro rs liveL
  induction liveL with
  | nil =>
      intro ks hnd hcov
      simp [sbVal, keySum]
      exact sum_map_zero ks
  | cons l rest ih =>
      intro ks hnd hcov
      cases l with
      | mk lk v =>
          cases lk with
          | none =>
              cases v with
              | none =>
                  have hf : (fun k => sbVal rs ({ k := none, v := none } :: rest) k) =
                      (fun k => sbVal rs rest k) := by
                    funext k
                    unfold sbVal
                    rw [keySum_cons]
                    simp [kBool, kVal]
                  rw [hf]
                  have hcov' : ∀ l ∈ rest, l.v.isSome → ∃ k ∈ ks, l.k = some k := by
                    intro l hl hv
                    exact hcov l (by simp [hl]) hv
                  simpa [rowVal] using (ih ks hnd hcov')
              | some v =>
                  have hf : (fun k => sbVal rs ({ k := none, v := some v } :: rest) k) =
                      (fun k => sbVal rs rest k) := by
                    funext k
                    exact sbVal_cons_keynone rs rest v k
                  rw [hf]
                  have hcov' : ∀ l ∈ rest, l.v.isSome → ∃ k ∈ ks, l.k = some k := by
                    intro l hl hv
                    exact hcov l (by simp [hl]) hv
                  simpa [rowVal] using (ih ks hnd hcov')
          | some lk =>
              cases v with
              | none =>
                  have hf : (fun k => sbVal rs ({ k := some lk, v := none } :: rest) k) =
                      (fun k => sbVal rs rest k) := by
                    funext k
                    exact sbVal_cons_none rs rest lk k
                  rw [hf]
                  have hcov' : ∀ l ∈ rest, l.v.isSome → ∃ k ∈ ks, l.k = some k := by
                    intro l hl hv
                    exact hcov l (by simp [hl]) hv
                  simpa [rowVal] using (ih ks hnd hcov')
              | some x =>
                  have hvsome : ({ k := some lk, v := some x } : LRow).v.isSome := by simp
                  rcases hcov { k := some lk, v := some x } (by simp) hvsome with ⟨k0, hk0, heq⟩
                  simp at heq
                  subst k0
                  have hf : (fun k => sbVal rs ({ k := some lk, v := some x } :: rest) k) =
                      (fun k => sbVal rs rest k + (if k == lk then x * Int.ofNat (fanout rs k) else 0)) := by
                    funext k
                    exact sbVal_cons_some rs rest lk x k
                  rw [hf, sum_map_add]
                  have hconst : (fun k => if k == lk then x * Int.ofNat (fanout rs k) else 0) =
                      (fun k => if k == lk then x * Int.ofNat (fanout rs lk) else 0) := by
                    funext k
                    by_cases h : k == lk
                    · have heq' : k = lk := by simpa using h
                      simp [h, heq']
                    · simp [h]
                  rw [hconst]
                  have hsingle := sum_single_of_nodup hnd lk hk0 (x * Int.ofNat (fanout rs lk))
                  rw [hsingle]
                  have hcov' : ∀ l ∈ rest, l.v.isSome → ∃ k ∈ ks, l.k = some k := by
                    intro l hl hv
                    exact hcov l (by simp [hl]) hv
                  have hih := ih ks hnd hcov'
                  have hih' : (ks.map fun k => sbVal rs rest k).sum =
                      (rest.map fun l =>
                        match l.k, l.v with
                        | some lk, some x => x * Int.ofNat (fanout rs lk)
                        | _, _ => 0).sum := by
                    simpa [rowVal] using hih
                  simp only [List.map_cons, List.sum_cons, rowVal]
                  rw [hih']
                  omega

private theorem sbContrib_eq (rs : List RRow) (liveL : List LRow) (st : Bool × Int) (k : Int) :
    sbStep rs liveL st k =
      (st.1 || (keySum rs liveL k).1, st.2 + sbVal rs liveL k) := by
  unfold sbStep sbVal
  cases h : keySum rs liveL k with
  | mk b s =>
      cases b <;> simp [h]

private theorem sumByKeys_eq (rs : List RRow) (liveL : List LRow) (ks : List Int) :
    sumByKeys rs liveL ks =
      (ks.any (fun k => (keySum rs liveL k).1), (ks.map fun k => sbVal rs liveL k).sum) := by
  unfold sumByKeys
  rw [show (sbStep rs liveL) =
      (fun st k => (st.1 || (keySum rs liveL k).1, st.2 + sbVal rs liveL k)) by
        funext st k
        exact sbContrib_eq rs liveL st k]
  simpa using (foldl_or_add ks (fun k => (keySum rs liveL k).1)
      (fun k => sbVal rs liveL k) false 0)

private theorem rightHasKey_iff_fanout_pos (rs : List RRow) (k : Int) :
    rightHasKey rs k = true ↔ 0 < fanout rs k := by
  induction rs with
  | nil => simp [rightHasKey, fanout]
  | cons r rs ih =>
      cases r with
      | mk rk =>
          cases rk with
          | none => simp [rightHasKey, fanout, ih]
          | some rk =>
              by_cases h : rk == k
              · have h' : ((some rk : Option Int) == some k) = true := by simp [h]
                simp [rightHasKey, fanout, h, h', ih]
              · have h' : ((some rk : Option Int) == some k) = false := by simp [h]
                simp [rightHasKey, fanout, h, h', ih]

private theorem liveRows_properties (ls : List LRow) (rs : List RRow) :
    (∀ l ∈ liveRows ls rs, ∃ k, l.k = some k ∧ rightHasKey rs k = true) ∧
    (∀ l ∈ ls, (∃ k, l.k = some k ∧ rightHasKey rs k = true) → l ∈ liveRows ls rs) := by
  constructor
  · intro l hl
    simp [liveRows] at hl
    rcases hl with ⟨hlmem, hk⟩
    cases l with
    | mk k v =>
        cases k with
        | none => simp at hk
        | some k =>
            have hr : rightHasKey rs k = true := by simpa using hk
            exact ⟨k, rfl, hr⟩
  · intro l hl h
    simp [liveRows]
    rcases h with ⟨k, hk, hr⟩
    constructor
    · exact hl
    · cases l with
      | mk lk v =>
          simp at hk
          subst lk
          simpa using hr

private theorem optSemijoinEagerSum_eq (ls : List LRow) (rs : List RRow) :
    optSemijoinEagerSum ls rs =
      let liveL := liveRows ls rs
      let (hasVal, total) := sumByKeys rs liveL (collectKeys liveL)
      if !hasVal then some none else if inI64 total then some (some total) else none := by
  rfl


private theorem sum_replicate (x : Int) (n : Nat) :
    (List.replicate n x).sum = x * Int.ofNat n := by
  induction n with
  | zero => simp
  | succ n ih =>
      rw [List.replicate_succ, List.sum_cons, ih]
      simp
      rw [Int.mul_add, Int.mul_one]
      omega

private theorem liveRows_rowVal_sum_eq_weightedVals (ls : List LRow) (rs : List RRow) :
    ((liveRows ls rs).map (rowVal rs)).sum = (weightedVals ls rs).sum := by
  induction ls with
  | nil =>
      simp [liveRows, weightedVals]
  | cons l rest ih =>
      cases l with
      | mk k v =>
        cases k with
        | none =>
            simpa [liveRows, weightedVals] using ih
        | some lk =>
            cases v with
            | none =>
                by_cases hr : rightHasKey rs lk
                · simp [liveRows, weightedVals, rowVal, hr]
                  exact ih
                · simp [liveRows, weightedVals, rowVal, hr]
                  exact ih
            | some x =>
                by_cases hr : rightHasKey rs lk
                · have hfpos : 0 < fanout rs lk := (rightHasKey_iff_fanout_pos rs lk).mp hr
                  have hfne : fanout rs lk ≠ 0 := by omega
                  simp [liveRows, weightedVals, rowVal, hr, List.sum_append, sum_replicate]
                  have ih' :
                      (List.map (rowVal rs)
                        (List.filter (fun l =>
                          match l.k with
                          | some k => rightHasKey rs k
                          | none => false) rest)).sum =
                      (List.flatMap (fun l =>
                        match l.k, l.v with
                        | some lk, some x => List.replicate (fanout rs lk) x
                        | _, _ => []) rest).sum := by
                    simpa [liveRows, weightedVals] using ih
                  rw [ih']
                  have hmul : x * (fanout rs lk : Int) = (fanout rs lk : Int) * x := by
                    exact Int.mul_comm x (Int.ofNat (fanout rs lk))
                  rw [hmul]
                · have hf0 : fanout rs lk = 0 := by
                    have : ¬ 0 < fanout rs lk := by
                      intro hpos
                      exact hr ((rightHasKey_iff_fanout_pos rs lk).mpr hpos)
                    omega
                  simpa [liveRows, weightedVals, hr, hf0] using ih

private theorem liveAny_eq_weightedVals_nonempty : ∀ (ls : List LRow) (rs : List RRow),
    (liveRows ls rs).any (fun l => l.v.isSome) = !(weightedVals ls rs).isEmpty := by
  intro ls
  induction ls with
  | nil =>
      intro rs
      simp [liveRows, weightedVals]
  | cons l rest ih =>
      intro rs
      cases l with
      | mk k v =>
        cases k with
        | none =>
            simpa [liveRows, weightedVals] using ih rs
        | some lk =>
            cases v with
            | none =>
                simpa [liveRows, weightedVals] using ih rs
            | some x =>
                by_cases hr : rightHasKey rs lk
                · have hfpos : 0 < fanout rs lk := (rightHasKey_iff_fanout_pos rs lk).mp hr
                  have hfne : fanout rs lk ≠ 0 := by omega
                  simp [liveRows, weightedVals, hr, hfne]
                · have hf0 : fanout rs lk = 0 := by
                    have : ¬ 0 < fanout rs lk := by
                      intro hpos
                      exact hr ((rightHasKey_iff_fanout_pos rs lk).mpr hpos)
                    omega
                  simpa [liveRows, weightedVals, hr, hf0] using ih rs

private theorem opt_result_eq (ls : List LRow) (rs : List RRow)
    (hp : inI64 (weightedPosTotal ls rs) = true)
    (hn : inI64 (weightedNegTotal ls rs) = true) :
    optSemijoinEagerSum ls rs =
      if (weightedVals ls rs).isEmpty then some none
      else some (some (weightedPosTotal ls rs + weightedNegTotal ls rs)) := by
  let liveL := liveRows ls rs
  let ks := collectKeys liveL
  have hcov : ∀ l ∈ liveL, l.v.isSome → ∃ k ∈ ks, l.k = some k := by
    intro l hl hv
    rcases (liveRows_properties ls rs).1 l hl with ⟨k, hk, hr⟩
    exact ⟨k, collectKeys_cover liveL l k hl hk, hk⟩
  have hsum := sumByKeys_eq rs liveL ks
  have hfst : (sumByKeys rs liveL ks).1 = ks.any (fun k => (keySum rs liveL k).1) :=
    congrArg Prod.fst hsum
  have hsnd : (sumByKeys rs liveL ks).2 = (ks.map fun k => sbVal rs liveL k).sum :=
    congrArg Prod.snd hsum
  have hseenany := kSeenAny_eq rs liveL ks hcov
  have hhas : (sumByKeys rs liveL ks).1 = !(weightedVals ls rs).isEmpty := by
    rw [hfst, hseenany]
    exact liveAny_eq_weightedVals_nonempty ls rs
  have hgrp := group_sum rs liveL ks (collectKeys_nodup liveL) hcov
  have htotal : (sumByKeys rs liveL ks).2 =
      weightedPosTotal ls rs + weightedNegTotal ls rs := by
    rw [hsnd, hgrp, liveRows_rowVal_sum_eq_weightedVals, weightedVals_sum]
  rw [optSemijoinEagerSum_eq]
  simp only
  rw [hhas, htotal]
  by_cases he : (weightedVals ls rs).isEmpty
  · simp [he]
  · have hin : inI64 (weightedPosTotal ls rs + weightedNegTotal ls rs) = true := by
      rw [← weightedVals_sum]
      exact weightedVals_sum_inI64 ls rs hp hn
    simp [he, hin]

theorem sound : TinylambOpt.ResearchGoals.SemijoinGuardedCheckedEagerSum.Goal := by
  constructor
  · intro ls rs hp hn
    constructor
    · rw [refFlatJoinSum_eq ls rs hp hn, opt_result_eq ls rs hp hn]
    · rw [opt_result_eq ls rs hp hn]
      by_cases he : (weightedVals ls rs).isEmpty
      · simp [he]
      · have hin : inI64 (weightedPosTotal ls rs + weightedNegTotal ls rs) = true := by
          rw [← weightedVals_sum]
          exact weightedVals_sum_inI64 ls rs hp hn
        simp [he, hin]
  · decide

end TinylambOpt.ResearchProofs.SemijoinGuardedCheckedEagerSum

-- C++ references to this model or its proof.
-- executor/detail/analytics_factorized_join.cpp
-- executor/detail/analytics_factorized_join.hpp
