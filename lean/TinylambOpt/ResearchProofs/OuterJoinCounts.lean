import TinylambOpt.ResearchGoals.OuterJoinCounts

namespace TinylambOpt.ResearchProofs.OuterJoinCounts

open TinylambOpt.ResearchGoals.OuterJoinCounts

private theorem histogram_fold_getD (m : Std.HashMap Key Nat) (xs : Rows) (key : Key) :
    (xs.foldl (fun m x => m.insert x (m.getD x 0 + 1)) m).getD key 0 =
      m.getD key 0 + xs.count key := by
  induction xs generalizing m with
  | nil => simp
  | cons x xs ih =>
      rw [List.foldl_cons, ih, Std.HashMap.getD_insert]
      by_cases hx : (x == key) = true
      · have hx' : x = key := by simpa using hx
        subst key
        simp
        omega
      · simp [hx, List.count_cons]

private theorem histogram_getD (xs : Rows) (key : Key) :
    (histogram xs).getD key 0 = xs.count key := by
  simpa [histogram] using histogram_fold_getD {} xs key

private def emit (x : Key) (r : Rows) : Rows :=
  let hits := r.filter (keyEq x)
  if hits.isEmpty then [x] else hits.map fun _ => x

private def leftPart (l r : Rows) : Rows := l.flatMap fun x => emit x r

private def rightPart (l r : Rows) : Rows :=
  r.filter fun y => !(l.any fun x => keyEq x y)

private theorem projected_eq (l r : Rows) : projected l r = leftPart l r ++ rightPart l r := by
  rfl

private theorem filter_keyEq_none (r : Rows) : r.filter (keyEq none) = [] := by
  induction r with
  | nil => rfl
  | cons y ys ih => simpa [keyEq] using ih

private theorem filter_keyEq_some_length (r : Rows) (k : Int) :
    (r.filter (keyEq (some k))).length = r.count (some k) := by
  induction r with
  | nil => rfl
  | cons y ys ih =>
      cases y with
      | none => simp [keyEq, ih]
      | some j =>
          by_cases h : k == j
          · have hkj : k = j := by simpa using h
            subst j
            simp [keyEq, ih]
          · have hkj : k ≠ j := by
              intro hkj'
              exact h (by simpa [hkj'])
            have hjk : j ≠ k := by
              intro hjk'
              exact hkj hjk'.symm
            have hbe : (some k == some j) = false := by simp [hkj]
            have hbe' : (some j == some k) = false := by simp [hjk]
            simp [keyEq, ih, hbe, hbe', List.count_cons]

private theorem emit_none (r : Rows) : emit none r = [none] := by
  simp [emit, filter_keyEq_none]

private theorem count_none_map_some (xs : List Key) (k : Int) :
    (xs.map fun _ => (some k : Key)).count none = 0 := by
  induction xs with
  | nil => rfl
  | cons x xs ih => simp [List.count_cons, ih]

private theorem count_some_map_some (xs : List Key) (k : Int) :
    (xs.map fun _ => (some k : Key)).count (some k) = xs.length := by
  induction xs with
  | nil => rfl
  | cons x xs ih => simp [List.count_cons, ih]

private theorem count_some_map_ne (xs : List Key) (j k : Int) (hjk : j ≠ k) :
    (xs.map fun _ => (some j : Key)).count (some k) = 0 := by
  induction xs with
  | nil => rfl
  | cons x xs ih =>
      have hbe : ((some j : Key) == some k) = false := by simp [hjk]
      simp [List.count_cons, ih, hbe]

private theorem emit_some_count_none (r : Rows) (k : Int) : (emit (some k) r).count none = 0 := by
  unfold emit
  by_cases h : (r.filter (keyEq (some k))).isEmpty
  · simp [h]
  · simp [h, count_none_map_some]

private theorem emit_some_count_some (r : Rows) (k : Int) :
    (emit (some k) r).count (some k) = if r.count (some k) = 0 then 1 else r.count (some k) := by
  cases hf : r.filter (keyEq (some k)) with
  | nil =>
      have hz : r.count (some k) = 0 := by
        rw [← filter_keyEq_some_length r k]
        simp [hf]
      simp [emit, hf, hz]
  | cons a as =>
      have hn : r.count (some k) ≠ 0 := by
        rw [← filter_keyEq_some_length r k]
        simp [hf]
      have hlen : (a :: as).length = r.count (some k) := by
        rw [← filter_keyEq_some_length r k]
        simp [hf]
      simp [emit, hf, hn, count_some_map_some]
      omega

private theorem emit_ne_count_some (r : Rows) (k : Int) (x : Key) (hx : x ≠ some k) :
    (emit x r).count (some k) = 0 := by
  cases x with
  | none => simp [emit_none]
  | some j =>
      by_cases hjk : j = k
      · subst j
        exact (hx rfl).elim
      · unfold emit
        by_cases h : (r.filter (keyEq (some j))).isEmpty
        · have hbe : ((some j : Key) == some k) = false := by simp [hjk]
          simp [h, hbe, List.count_cons]
        · simp [h, count_some_map_ne, hjk]

private theorem leftPart_count_none (l r : Rows) : (leftPart l r).count none = l.count none := by
  induction l with
  | nil => rfl
  | cons x xs ih =>
      change (emit x r ++ leftPart xs r).count none = (x :: xs).count none
      rw [List.count_append, ih]
      cases x with
      | none => simp [emit_none, List.count_cons]; omega
      | some k => simp [emit_some_count_none, List.count_cons]

private theorem leftPart_count_some (l r : Rows) (k : Int) :
    (leftPart l r).count (some k) =
      (l.count (some k)) * (if r.count (some k) = 0 then 1 else r.count (some k)) := by
  induction l with
  | nil => simp [leftPart]
  | cons x xs ih =>
      rw [show leftPart (x :: xs) r = emit x r ++ leftPart xs r by rfl, List.count_append, ih]
      by_cases hx : x = some k
      · subst x
        simp [emit_some_count_some, List.count_cons]
        rw [Nat.succ_mul]
        omega
      · rw [emit_ne_count_some r k x hx]
        simp [List.count_cons, hx]

private theorem any_keyEq_some (l : Rows) (k : Int) :
    (l.any (fun x => keyEq x (some k)) = true) ↔ some k ∈ l := by
  rw [List.any_eq_true]
  constructor
  · rintro ⟨x, hx, hkey⟩
    cases x with
    | none => simp [keyEq] at hkey
    | some j =>
        simp [keyEq] at hkey
        subst j
        exact hx
  · intro h
    exact ⟨some k, h, by simp [keyEq]⟩

private theorem any_keyEq_none_false (l : Rows) : (l.any fun x => keyEq x (none : Key)) = false := by
  rw [List.any_eq_false]
  intro x hx
  cases x <;> simp [keyEq]

private theorem filter_count_cons (p : Key → Bool) (a key : Key) (xs : List Key) :
    (List.filter p (a :: xs)).count key =
      (if p a = true then if (a == key) = true then 1 else 0 else 0) +
        (List.filter p xs).count key := by
  by_cases h : p a = true <;> by_cases heq : (a == key) = true <;>
    simp [List.filter_cons, h, heq, List.count_cons] <;> omega

private theorem rightPart_count_none (l r : Rows) : (rightPart l r).count none = r.count none := by
  induction r with
  | nil => rfl
  | cons y ys ih =>
      simp only [rightPart] at ih ⊢
      rw [filter_count_cons, ih]
      cases y with
      | none =>
          have hp : (!(l.any fun x => keyEq x (none : Key))) = true := by simp [any_keyEq_none_false]
          rw [if_pos hp]
          simp [List.count_cons]
          omega
      | some j =>
          have hbe : ¬ ((some j : Key) == none) = true := by simp
          by_cases hp : (!(l.any fun x => keyEq x (some j))) = true
          · rw [if_pos hp, if_neg hbe]
            simp [List.count_cons]
          · rw [if_neg hp]
            simp [List.count_cons]

private theorem rightPart_count_some (l r : Rows) (k : Int) :
    (rightPart l r).count (some k) =
      if l.count (some k) = 0 then r.count (some k) else 0 := by
  by_cases ha : l.count (some k) = 0
  · have hnot : ¬ some k ∈ l := List.count_eq_zero.mp ha
    have hany : (l.any (fun x => keyEq x (some k)) = false) := by
      cases hb : l.any (fun x => keyEq x (some k)) with
      | false => rfl
      | true => exact False.elim (hnot ((any_keyEq_some l k).mp hb))
    induction r with
    | nil => simp [rightPart, ha]
    | cons y ys ih =>
        simp only [rightPart] at ih ⊢
        rw [filter_count_cons, ih]
        simp only [ha, if_true, if_false]
        cases y with
        | none =>
            have hp : (!(l.any fun x => keyEq x (none : Key))) = true := by simp [any_keyEq_none_false]
            have hbe : ¬ ((none : Key) == some k) = true := by simp
            rw [if_pos hp, if_neg hbe]
            simp [List.count_cons]
        | some j =>
            by_cases hj : j = k
            · subst j
              have hp : (!(l.any fun x => keyEq x (some k))) = true := by simp [hany]
              have hbe : ((some k : Key) == some k) = true := by simp
              rw [if_pos hp, if_pos hbe]
              simp [List.count_cons]
              omega
            · have hbe : ¬ ((some j : Key) == some k) = true := by simp [hj]
              by_cases hp : (!(l.any fun x => keyEq x (some j))) = true
              · rw [if_pos hp, if_neg hbe]
                simp [List.count_cons, hj]
              · rw [if_neg hp]
                simp [List.count_cons, hj]
  · have hmem : some k ∈ l := by
      by_cases hmem : some k ∈ l
      · exact hmem
      · exact False.elim (ha (List.count_eq_zero.mpr hmem))
    have hany : (l.any (fun x => keyEq x (some k)) = true) := (any_keyEq_some l k).mpr hmem
    induction r with
    | nil => simp [rightPart, ha]
    | cons y ys ih =>
        simp only [rightPart] at ih ⊢
        rw [filter_count_cons, ih]
        simp only [ha, if_true, if_false]
        cases y with
        | none =>
            have hp : (!(l.any fun x => keyEq x (none : Key))) = true := by simp [any_keyEq_none_false]
            have hbe : ¬ ((none : Key) == some k) = true := by simp
            rw [if_pos hp, if_neg hbe]
        | some j =>
            by_cases hj : j = k
            · subst j
              have hp : ¬ ((!(l.any fun x => keyEq x (some k))) = true) := by simp [hany]
              rw [if_neg hp]
            · have hbe : ¬ ((some j : Key) == some k) = true := by simp [hj]
              by_cases hp : (!(l.any fun x => keyEq x (some j))) = true
              · rw [if_pos hp, if_neg hbe]
              · rw [if_neg hp]

theorem sound : TinylambOpt.ResearchGoals.OuterJoinCounts.Goal := by
  intro l r key
  unfold groupCount
  rw [histogram_getD, histogram_getD]
  cases key with
  | none =>
      simp [groupCount, projected_eq, leftPart_count_none, rightPart_count_none]
  | some k =>
      rw [projected_eq, List.count_append, leftPart_count_some, rightPart_count_some]
      by_cases ha : l.count (some k) = 0 <;> by_cases hb : r.count (some k) = 0 <;>
        simp [groupCount, ha, hb] <;> omega

end TinylambOpt.ResearchProofs.OuterJoinCounts
