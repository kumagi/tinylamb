import Std
#check Nat.add_sub_add_right
#check Nat.add_sub_cancel_right
#check Nat.add_sub_add_left
example (a b c : Nat) (h : b ≤ a) : (a+c)-(b+c) = a-b := by omega
