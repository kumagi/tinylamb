import Std
set_option maxHeartbeats 0

-- omega on nonlinear atoms
example (q t s : Nat) (h : s < q*(t+1) + (t+1)) : s ≤ q*(t+1) + t := by omega
example (x n y : Nat) (h : x ≤ y) : x + (n*(y+1)) ≤ y + (n*(y+1)) := by omega
example (a b c : Nat) : (a ≥ b) ∨ (b ≥ a) := by omega

#check @Nat.le_div_iff_mul_le
#check @Nat.div_lt_iff_lt_mul
#check @Nat.div_mul_le_self
#check @Nat.div_le_div_right
#check @Nat.div_add_mod
#check @Nat.mod_lt
#check @Nat.pow_le_pow_of_le_left
#check @Nat.mul_le_mul
#check @Nat.pow_pos
#check @Nat.two_pow_pos

#check @Nat.xor_div_two_pow
#check @Nat.testBit_div_two_pow
#check @Nat.testBit_lt_two_pow
#check @Nat.testBit_xor
#check @Nat.eq_of_testBit_eq
#check @Nat.zero_testBit
#check @Nat.log2_eq_iff
#check @Nat.log2_two_pow
#check @Nat.testBit_two_pow_sub_one
#check @Nat.ge_two_pow_of_testBit

#check @List.countP
#check List.countP
#check @List.mergeSort
#check @List.pairwise_mergeSort
#check @List.mergeSort_of_pairwise
#check @List.pairwise_merge
#check @List.mem_merge
#check @List.mergeSort_perm
#check @List.Pairwise.forall
#check @List.Pairwise.tail
#check @List.Pairwise.filter
#check @List.Perm.filter
#check @List.Perm.length_eq
#check @List.filterMap_append
#check @List.length_mergeSort
#check @Nat.lt_of_not_ge
#check @Nat.max_le
#check @Nat.le_max_left
#check @Nat.le_of_lt_succ
#check @Nat.div_eq_of_lt_le
