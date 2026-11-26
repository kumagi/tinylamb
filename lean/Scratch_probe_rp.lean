import Std
-- Perm constructors
example {α : Type} {as bs : List α} (h : as.Perm bs) : True := by
  induction h with
  | @nil l => trivial
  | @cons a _ _ ih => trivial
  | @append l₁ l₂ _ _ ih => trivial
-- all API
#check @List.all_nil
#check @List.all_cons
#check @List.getElem?_append_left
#check @List.getElem?_append_right
#check @List.getElem?_cons_zero
#check @List.get_cons_zero
#check @Nat.pow_succ'
#check @Nat.succ_mul
#check @Nat.add_mul
#check @Nat.one_mul
#check @Nat.mul_one
#check @List.length_append
#check @List.mem_takeWhile
#check @List.drop_nil
#check @Nat.sub_lt
#check @Nat.sub_lt_sub_left
#check @Nat.pow_le_pow_of_le_right
#check @Nat.of_le
#check @decide_eq_true_eq
#check @List.sum_le_sum
#check @List.filter_cons
#check @List.find?_cons
#check @Option.getD_some
#check @Nat.max_le
#check @List.Nodup
#check @List.takeWhile
example (p : Nat → Bool) (l : List Nat) (h : ∀ x ∈ l, p x = true) : l.takeWhile p = l := by
  induction l with
  | nil => rfl
  | cons x xs ih => simp [List.takeWhile, h x (by simp), ih (fun y hy => h y (by simp [hy]))]
