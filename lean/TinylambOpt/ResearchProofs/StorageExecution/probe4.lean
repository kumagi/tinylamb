import Std
#check @List.Perm.tail
#check @List.Perm.symm
#check @List.Perm.trans
#check @List.Perm.refl
example {l₁ l₂ : List Nat} (h : List.Perm l₁ l₂) : True := trivial
open List in
example {l₁ l₂ : List Nat} (h : l₁ ~ l₂) : True := trivial
#check @List.pairwise_mergeSort
#check @List.Pairwise.filter
#check @List.length_append
