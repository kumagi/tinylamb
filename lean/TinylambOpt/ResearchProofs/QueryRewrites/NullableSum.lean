import TinylambOpt.ResearchProofs.QueryRewrites.KeyGrouping

/-! Algebra for the query rewrite model of SQL SUM over exact integers.
`none` is the subtotal of an input with no non-NULL values. -/


namespace TinylambOpt.ResearchProofs.QueryRewrites.NullableSum

open TinylambOpt.ResearchGoals.QueryRewrites (nsum forKey)


/-- Combine partial sums, preserving NULL when both inputs have no values. -/
def merge (a b : Option Int) : Option Int :=
  match a, b with
  | none, b => b
  | a, none => a
  | some x, some y => some (x + y)

theorem merge_none_left (a : Option Int) : merge none a = a := rfl

theorem merge_assoc (a b c : Option Int) : merge (merge a b) c = merge a (merge b c) := by
  cases a <;> cases b <;> cases c <;> simp [merge, Int.add_assoc]

theorem merge_comm (a b : Option Int) : merge a b = merge b a := by
  cases a <;> cases b <;> simp [merge, Int.add_comm]

private theorem merge_left_comm (a b c : Option Int) :
    merge a (merge b c) = merge b (merge a c) := by
  rw [← merge_assoc, merge_comm a b, merge_assoc]

theorem nsum_append (xs ys : List (Option Int)) :
    nsum (xs ++ ys) = merge (nsum xs) (nsum ys) := by
  cases hx : xs.filterMap id <;> cases hy : ys.filterMap id <;>
    simp [nsum, merge, List.filterMap_append, hx, hy, Int.add_assoc]

theorem nsum_singleton (x : Option Int) : nsum [x] = x := by
  cases x <;> simp [nsum]

theorem nsum_cons (x : Option Int) (xs : List (Option Int)) :
    nsum (x :: xs) = merge x (nsum xs) := by
  simpa only [List.singleton_append, nsum_singleton] using nsum_append [x] xs

/-- The exact integer subtotal is invariant under row permutation. -/
theorem nsum_perm {xs ys : List (Option Int)} (h : xs.Perm ys) : nsum xs = nsum ys := by
  induction h with
  | nil => rfl
  | cons x _ ih => rw [nsum_cons, nsum_cons, ih]
  | swap x y xs => simpa only [nsum_cons] using merge_left_comm y x (nsum xs)
  | trans _ _ ih₁ ih₂ => exact ih₁.trans ih₂

private theorem merge_getD (a b : Option Int) :
    (merge a b).getD 0 = a.getD 0 + b.getD 0 := by
  cases a <;> cases b <;> simp [merge]

theorem nsum_cons_getD (x : Option Int) (xs : List (Option Int)) :
    (nsum (x :: xs)).getD 0 = x.getD 0 + (nsum xs).getD 0 := by
  rw [nsum_cons, merge_getD]

/-- Summing partial sums agrees with summing all original rows. -/
theorem nsum_map_nsum (groups : List (List (Option Int))) :
    nsum (groups.map nsum) = nsum groups.flatten := by
  induction groups with
  | nil => rfl
  | cons group groups ih =>
      rw [List.map_cons, List.flatten_cons, nsum_cons, nsum_append, ih]

theorem nsum_replicate (n : Nat) (x : Option Int) :
    nsum (List.replicate n x) = if n = 0 then none else x.map (fun v => (n : Int) * v) := by
  cases n <;> cases x <;> simp [nsum, List.sum_replicate_int]

/-- Repeating each row uniformly scales the sum when the repeat count is positive. -/
theorem nsum_flatMap_replicate (n : Nat) (xs : List (Option Int)) :
    nsum (xs.flatMap (List.replicate n)) =
      if n = 0 then none else (nsum xs).map (fun v => (n : Int) * v) := by
  induction xs with
  | nil => simp [nsum]
  | cons x xs ih =>
      rw [List.flatMap_cons, nsum_append, nsum_replicate, ih, nsum_cons]
      by_cases h : n = 0
      · simp [h, merge]
      · cases x <;> cases nsum xs <;> simp [h, merge, Int.mul_add]

theorem nsum_flatMap_const {α : Type} (xs : List α) (ys : List (Option Int)) :
    nsum (xs.flatMap (fun _ => ys)) =
      if xs.isEmpty then none else (nsum ys).map (fun v => (xs.length : Int) * v) := by
  rw [List.flatMap_def, ← nsum_map_nsum, List.map_map, Function.comp_def, List.map_const',
    nsum_replicate]
  cases xs <;> simp

/-- Group by the distinct keys before summing values from the selected keys. -/
theorem nsum_group_by_key {α : Type} [BEq α] [LawfulBEq α] (p : α → Bool)
    (rows : List (α × Option Int)) :
    nsum (((rows.map Prod.fst).eraseDups.filter p).map (fun k => nsum (forKey rows k))) =
      nsum ((rows.filter (fun row => p row.1)).map Prod.snd) := by
  let keys := (rows.map Prod.fst).eraseDups.filter p
  change nsum (keys.map (nsum ∘ forKey rows)) = _
  rw [← List.map_map, nsum_map_nsum]
  exact (nsum_perm (KeyGrouping.forKey_filter_perm rows p)).symm

end TinylambOpt.ResearchProofs.QueryRewrites.NullableSum
