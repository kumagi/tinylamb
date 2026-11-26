import TinylambOpt.ResearchGoals.JitKernels.RadixCursors

/-!
Permuting input lanes before histograms leaves the set of scatter destinations
unchanged. Adjacent swaps preserve the destination multiset because only the
two swapped lanes exchange ranks; all other lanes see the same preceding
histogram. This licenses SQL lane swizzling without rechecking destinations.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
set_option linter.unnecessarySimpa false

namespace TinylambOpt.ResearchProofs.JitKernels.Goal_RadixCursors_SwizzleBucketOrder

open TinylambOpt.ResearchGoals.JitKernels.RadixCursors

theorem countP_swap_heads (a b : Nat) (l : List Nat) (p : Nat → Bool) :
    (a :: b :: l).countP p = (b :: a :: l).countP p := by
  simp only [List.countP_cons]
  omega
theorem count_swap_heads (a b k : Nat) (l : List Nat) :
    (a :: b :: l).count k = (b :: a :: l).count k := by
  simp only [List.count_cons]
  omega
theorem countP_swap_pref (A : List Nat) (a b : Nat) (l : List Nat) (p : Nat → Bool) :
    (A ++ a :: b :: l).countP p = (A ++ b :: a :: l).countP p := by
  induction A with
  | nil => exact countP_swap_heads a b l p
  | cons z A ih => change (z :: (A ++ a :: b :: l)).countP p = (z :: (A ++ b :: a :: l)).countP p; simp [List.countP_cons, ih]
theorem count_swap_pref (A : List Nat) (a b k : Nat) (l : List Nat) :
    (A ++ a :: b :: l).count k = (A ++ b :: a :: l).count k := by
  induction A with
  | nil => exact count_swap_heads a b k l
  | cons z A ih => change (z :: (A ++ a :: b :: l)).count k = (z :: (A ++ b :: a :: l)).count k; simp [List.count_cons, ih]

theorem active_swap_base_pref (pre : List (Option Nat)) (x y : Option Nat)
    (rest : List (Option Nat)) (k : Nat) :
    base (pre ++ x :: y :: rest) k = base (pre ++ y :: x :: rest) k := by
  unfold base active
  simp only [List.filterMap_append]
  cases x with
  | none => cases y <;> simp
  | some a =>
      cases y with
      | none => simp
      | some b => exact countP_swap_pref _ a b _ _

theorem active_swap_rank_pref (pre : List (Option Nat)) (x y : Option Nat)
    (rest : List (Option Nat)) (p k : Nat) :
    rank (pre ++ x :: y :: rest) (pre.length + 2 + p) k =
      rank (pre ++ y :: x :: rest) (pre.length + 2 + p) k := by
  unfold rank
  have htake : ∀ z w : Option Nat,
      (pre ++ z :: w :: rest).take (pre.length + 2 + p) = pre ++ z :: w :: rest.take p := by
    intro z w
    rw [List.take_append]
    rw [List.take_of_length_le (by omega : pre.length ≤ pre.length + 2 + p)]
    have hs : pre.length + 2 + p - pre.length = 2 + p := by omega
    rw [hs]
    simpa [Nat.add_comm, Nat.add_assoc]
  rw [htake, htake]
  unfold active
  simp only [List.filterMap_append]
  cases x with
  | none => cases y <;> simp
  | some a =>
      cases y with
      | none => simp
      | some b => exact count_swap_pref _ a b k _

theorem dst_swap_pref_prefix (pre : List (Option Nat)) (x y : Option Nat)
    (rest : List (Option Nat)) (i : Nat) (hi : i < pre.length) :
    dst (pre ++ x :: y :: rest) i = dst (pre ++ y :: x :: rest) i := by
  have hget : (pre ++ x :: y :: rest)[i]? = pre[i]? := by
    rw [List.getElem?_append]; simp [hi]
  have hget' : (pre ++ y :: x :: rest)[i]? = pre[i]? := by
    rw [List.getElem?_append]; simp [hi]
  have htake : (pre ++ x :: y :: rest).take i = pre.take i := by
    rw [List.take_append]
    simp [show i - pre.length = 0 by omega]
  have htake' : (pre ++ y :: x :: rest).take i = pre.take i := by
    rw [List.take_append]
    simp [show i - pre.length = 0 by omega]
  cases h : pre[i]? with
  | none => simp [dst, hget, hget', h]
  | some v =>
      cases v with
      | none => simp [dst, hget, hget', h]
      | some k =>
          have h1 : (pre ++ x :: y :: rest)[i]? = some (some k) := by simpa [h] using hget
          have h2 : (pre ++ y :: x :: rest)[i]? = some (some k) := by simpa [h] using hget'
          have hrank : rank (pre ++ x :: y :: rest) i k = rank (pre ++ y :: x :: rest) i k := by
            unfold rank
            rw [htake, htake']
          simp [dst, h1, h2, hrank, active_swap_base_pref]

theorem dst_swap_pref_tail (pre : List (Option Nat)) (x y : Option Nat)
    (rest : List (Option Nat)) (p : Nat) :
    dst (pre ++ x :: y :: rest) (pre.length + 2 + p) =
      dst (pre ++ y :: x :: rest) (pre.length + 2 + p) := by
  have hget : (pre ++ x :: y :: rest)[pre.length + 2 + p]? = rest[p]? := by
    rw [List.getElem?_append]
    simp [show ¬ pre.length + 2 + p < pre.length by omega,
      show pre.length + 2 + p - pre.length = 2 + p by omega]
    have h1 : (x :: y :: rest)[2 + p]? = (y :: rest)[1 + p]? := by
      simpa [Nat.add_assoc, Nat.add_comm] using
        (List.getElem?_cons_succ (a := x) (l := y :: rest) (i := 1 + p))
    have h2 : (y :: rest)[1 + p]? = rest[p]? := by
      simpa [Nat.add_assoc, Nat.add_comm] using
        (List.getElem?_cons_succ (a := y) (l := rest) (i := p))
    exact h1.trans h2
  have hget' : (pre ++ y :: x :: rest)[pre.length + 2 + p]? = rest[p]? := by
    rw [List.getElem?_append]
    simp [show ¬ pre.length + 2 + p < pre.length by omega,
      show pre.length + 2 + p - pre.length = 2 + p by omega]
    have h1 : (y :: x :: rest)[2 + p]? = (x :: rest)[1 + p]? := by
      simpa [Nat.add_assoc, Nat.add_comm] using
        (List.getElem?_cons_succ (a := y) (l := x :: rest) (i := 1 + p))
    have h2 : (x :: rest)[1 + p]? = rest[p]? := by
      simpa [Nat.add_assoc, Nat.add_comm] using
        (List.getElem?_cons_succ (a := x) (l := rest) (i := p))
    exact h1.trans h2
  cases h : rest[p]? with
  | none => simp [dst, hget, hget', h]
  | some v =>
      cases v with
      | none => simp [dst, hget, hget', h]
      | some k =>
          have h1 : (pre ++ x :: y :: rest)[pre.length + 2 + p]? = some (some k) := by simpa [h] using hget
          have h2 : (pre ++ y :: x :: rest)[pre.length + 2 + p]? = some (some k) := by simpa [h] using hget'
          simp [dst, h1, h2, active_swap_base_pref, active_swap_rank_pref]

theorem range_add_two (n : Nat) :
    List.range (n + 2) = 0 :: 1 :: (List.range n).map (fun p => p + 2) := by
  rw [show n + 2 = (n + 1) + 1 by omega]
  rw [List.range_succ_eq_map]
  change 0 :: ((List.range (n + 1)).map Nat.succ) = 0 :: 1 :: (List.range n).map (fun p => p + 2)
  congr
  rw [List.range_succ_eq_map]
  simp [List.map_map, Function.comp_def, Nat.succ_eq_add_one]

theorem filterMap_eq_of_mem {α β : Type} (f g : α → Option β) (l : List α)
    (h : ∀ a ∈ l, f a = g a) : l.filterMap f = l.filterMap g := by
  induction l with
  | nil => simp
  | cons a l ih =>
      have ha := h a (by simp)
      have hl : ∀ b ∈ l, f b = g b := by
        intro b hb
        exact h b (by simp [hb])
      simp [List.filterMap_cons, ha, ih hl]

theorem range_append_two (p t : Nat) :
    List.range (p + (t + 2)) =
      List.range p ++ (p :: (p + 1) :: (List.range t).map (fun i => p + (i + 2))) := by
  rw [List.range_add]
  rw [range_add_two t]
  simp [List.map_append, List.map_cons, List.map_map, Function.comp_def, Nat.add_assoc]

theorem head_dst_none_some (pre rest : List (Option Nat)) (c : Nat) :
    dst (pre ++ none :: some c :: rest) (pre.length + 1) =
      dst (pre ++ some c :: none :: rest) pre.length := by
  have ht : (pre ++ none :: some c :: rest).take (pre.length + 1) = pre ++ [none] := by
    rw [List.take_append]
    rw [List.take_of_length_le (by omega : pre.length ≤ pre.length + 1)]
    simp
  simp [dst, base, rank, active, List.countP_append, ht]

theorem head_dst_some_none (pre rest : List (Option Nat)) (a : Nat) :
    dst (pre ++ some a :: none :: rest) pre.length =
      dst (pre ++ none :: some a :: rest) (pre.length + 1) := by
  have ht : (pre ++ none :: some a :: rest).take (pre.length + 1) = pre ++ [none] := by
    rw [List.take_append]
    rw [List.take_of_length_le (by omega : pre.length ≤ pre.length + 1)]
    simp
  simp [dst, base, rank, active, List.countP_append, ht]

theorem head_dst_some_some_1 (pre rest : List (Option Nat)) (a b : Nat) (hne : b ≠ a) :
    dst (pre ++ some a :: some b :: rest) pre.length =
      dst (pre ++ some b :: some a :: rest) (pre.length + 1) := by
  have ht1 : (pre ++ some a :: some b :: rest).take pre.length = pre := by
    rw [List.take_append]
    rw [List.take_of_length_le (by omega : pre.length ≤ pre.length)]
    simp
  have ht2 : (pre ++ some b :: some a :: rest).take (pre.length + 1) = pre ++ [some b] := by
    rw [List.take_append]
    rw [List.take_of_length_le (by omega : pre.length ≤ pre.length + 1)]
    simp
  simp [dst, base, rank, active, List.countP_append, ht1, ht2, hne]
  simp only [List.countP_cons]
  have hna : ¬ a < a := by omega
  simp [hna]

theorem head_dst_some_some_2 (pre rest : List (Option Nat)) (a b : Nat) (hne : a ≠ b) :
    dst (pre ++ some a :: some b :: rest) (pre.length + 1) =
      dst (pre ++ some b :: some a :: rest) pre.length := by
  have ht1 : (pre ++ some a :: some b :: rest).take (pre.length + 1) = pre ++ [some a] := by
    rw [List.take_append]
    rw [List.take_of_length_le (by omega : pre.length ≤ pre.length + 1)]
    simp
  have ht2 : (pre ++ some b :: some a :: rest).take pre.length = pre := by
    rw [List.take_append]
    rw [List.take_of_length_le (by omega : pre.length ≤ pre.length)]
    simp
  simp [dst, base, rank, active, List.countP_append, ht1, ht2, hne]
  simp only [List.countP_cons]
  have hnb : ¬ b < b := by omega
  simp [hnb]

theorem addresses_swap_pref (pre : List (Option Nat)) (x y : Option Nat)
    (rest : List (Option Nat)) :
    (addresses (pre ++ x :: y :: rest)).Perm (addresses (pre ++ y :: x :: rest)) := by
  unfold addresses
  rw [show (pre ++ x :: y :: rest).length = pre.length + (rest.length + 2) by simp,
      show (pre ++ y :: x :: rest).length = pre.length + (rest.length + 2) by simp]
  rw [range_append_two pre.length rest.length]
  simp only [List.filterMap_append, List.filterMap_cons, List.filterMap_map]
  have hprefix : List.filterMap (dst (pre ++ x :: y :: rest)) (List.range pre.length) =
      List.filterMap (dst (pre ++ y :: x :: rest)) (List.range pre.length) := by
    apply filterMap_eq_of_mem
    intro i hi
    have hi' : i < pre.length := by simpa using hi
    exact dst_swap_pref_prefix pre x y rest i hi'
  have htail : List.filterMap (dst (pre ++ x :: y :: rest) ∘ fun i => pre.length + (i + 2))
        (List.range rest.length) =
      List.filterMap (dst (pre ++ y :: x :: rest) ∘ fun i => pre.length + (i + 2))
        (List.range rest.length) := by
    apply filterMap_eq_of_mem
    intro i hi
    change dst (pre ++ x :: y :: rest) (pre.length + (i + 2)) =
      dst (pre ++ y :: x :: rest) (pre.length + (i + 2))
    simpa [Nat.add_assoc, Nat.add_comm, Nat.add_left_comm] using dst_swap_pref_tail pre x y rest i
  rw [hprefix, htail]
  cases x with
  | none =>
      cases y with
      | none => simp [dst]
      | some c =>
          have hz1 : dst (pre ++ none :: some c :: rest) pre.length = none := by simp [dst]
          have hz2 : dst (pre ++ some c :: none :: rest) (pre.length + 1) = none := by simp [dst]
          rw [hz1, hz2, head_dst_none_some]
  | some a =>
      cases y with
      | none =>
          have hz1 : dst (pre ++ some a :: none :: rest) (pre.length + 1) = none := by simp [dst]
          have hz2 : dst (pre ++ none :: some a :: rest) pre.length = none := by simp [dst]
          rw [hz1, hz2, head_dst_some_none]
      | some b =>
          by_cases hab : a = b
          · subst b
            simp [dst]
          · have hne1 : b ≠ a := by omega
            have hne2 : a ≠ b := by omega
            rw [head_dst_some_some_1 pre rest a b hne1,
                head_dst_some_some_2 pre rest a b hne2]
            simp [dst]
            exact List.Perm.append (List.Perm.refl _) (List.Perm.swap _ _ _)

theorem swizzle : SwizzleBucketOrder.Goal := by
  intro a b hp
  have hgen : ∀ (pre : List (Option Nat)) (l m : List (Option Nat)), l.Perm m →
      (addresses (pre ++ l)).Perm (addresses (pre ++ m)) := by
    intro pre l m h
    induction h generalizing pre with
    | nil => exact List.Perm.refl _
    | cons x h ih =>
        simpa [List.append_assoc] using ih (pre ++ [x])
    | swap x y l =>
        exact addresses_swap_pref pre y x l
    | trans h1 h2 ih1 ih2 =>
        exact (ih1 pre).trans (ih2 pre)
  exact hgen [] a b hp

end TinylambOpt.ResearchProofs.JitKernels.Goal_RadixCursors_SwizzleBucketOrder
