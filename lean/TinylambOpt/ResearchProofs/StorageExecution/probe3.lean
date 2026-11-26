import Std
#check @List.mem_cons_self
#check @List.mem_cons_of_mem
#check @List.filter_cons_of_pos
#check @List.countP_cons_of_pos
#check @List.take_cons
#check @List.contains_cons
#check @List.Perm.cons
#check @List.filter_append
#check @List.length_cons
#check @Nat.not_succ_le_zero
#check @List.sum_cons
#check @List.eq_nil_of_sum_eq_zero?
open List in
example {l₁ l₂ : List Nat} : l₁ ~ l₂ → True := fun _ => trivial
