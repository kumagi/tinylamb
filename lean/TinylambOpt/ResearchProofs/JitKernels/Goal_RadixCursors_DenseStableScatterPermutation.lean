import TinylambOpt.ResearchGoals.JitKernels.RadixCursors

/-!
Scatter destinations form a permutation of the live output slots. Sorting the
input lanes by bucket is a lane permutation that preserves the destination
multiset, and in sorted order each live lane receives its own ordinal. This
justifies a dense vector scatter in a SQL radix JIT kernel.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
set_option linter.unnecessarySimpa false

namespace TinylambOpt.ResearchProofs.JitKernels.Goal_RadixCursors_DenseStableScatterPermutation

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

def key (a : Option Nat) : Nat := match a with | none => 0 | some b => b + 1

theorem filterMap_map_option {α β : Type} (f : α → Option β) (g : β → β) (l : List α) :
    l.filterMap (fun a => Option.map g (f a)) = (l.filterMap f).map g := by
  induction l with
  | nil => simp
  | cons a l ih =>
      cases h : f a <;> simp [h, ih]

theorem rank_none_cons (rest : List (Option Nat)) (p k : Nat) :
    rank (none :: rest) (p + 1) k = rank rest p k := by
  simp [rank, active]

theorem dst_none_cons_succ (rest : List (Option Nat)) (p : Nat) :
    dst (none :: rest) (p + 1) = dst rest p := by
  cases h : rest[p]? with
  | none => simp [dst, h]
  | some v =>
      cases v with
      | none => simp [dst, h]
      | some k =>
          have h1 : (none :: rest)[p+1]? = some (some k) := by simpa using h
          simp [dst, h, h1, rank_none_cons, active, base]

theorem addresses_none_cons (rest : List (Option Nat)) :
    addresses (none :: rest) = addresses rest := by
  unfold addresses
  rw [show (none :: rest).length = rest.length + 1 by simp]
  rw [List.range_succ_eq_map]
  simp only [List.filterMap_cons, List.filterMap_map]
  simp [dst]
  apply filterMap_eq_of_mem
  intro i hi
  change dst (none :: rest) (Nat.succ i) = dst rest i
  exact dst_none_cons_succ rest i

theorem dst_some_cons_succ (b : Nat) (rest : List (Option Nat)) (p c : Nat)
    (hp : rest[p]? = some (some c)) (hbc : b ≤ c) :
    dst (some b :: rest) (p + 1) = some (base rest c + rank rest p c + 1) := by
  have h1 : (some b :: rest)[p+1]? = some (some c) := by simpa using hp
  by_cases heq : b = c
  · subst c
    simp [dst, hp, h1, base, rank, active]
    omega
  · have hlt : b < c := by omega
    simp [dst, hp, h1, base, rank, active, heq, hlt]
    omega

theorem addresses_some_cons (b : Nat) (rest : List (Option Nat))
    (hall : ∀ c ∈ active rest, b ≤ c) :
    addresses (some b :: rest) = 0 :: (addresses rest).map (fun v => v + 1) := by
  unfold addresses
  rw [show (some b :: rest).length = rest.length + 1 by simp]
  rw [List.range_succ_eq_map]
  simp only [List.filterMap_cons, List.filterMap_map]
  have hb0 : base (some b :: rest) b = 0 := by
    unfold base
    have hz : (active rest).countP (fun x => x < b) = 0 := by
      apply (List.countP_eq_zero).mpr
      intro a ha
      have hba : b ≤ a := hall a ha
      intro hlt
      simp at hlt
      omega
    change (b :: active rest).countP (fun x => x < b) = 0
    simp [hz]
  have h0 : dst (some b :: rest) 0 = some 0 := by
    simp [dst, hb0, rank, active]
  rw [h0]
  congr 1
  simp only [Option.map_some]
  rw [← filterMap_map_option]
  congr 1
  apply filterMap_eq_of_mem
  intro i hi
  change dst (some b :: rest) (Nat.succ i) = Option.map (fun v => v + 1) (dst rest i)
  cases h : rest[i]? with
  | none => simp [dst, h]
  | some v =>
      cases v with
      | none => simp [dst, h]
      | some c =>
          have hle : b ≤ c := by
            have hi' : i < rest.length := by simpa using hi
            -- get c in active rest at position i
            have hmem : c ∈ active rest := by
              rcases List.getElem?_eq_some_iff.mp h with ⟨hidx, hci⟩
              unfold active
              exact List.mem_filterMap.mpr
                ⟨some c, by simpa [hci] using List.getElem_mem hidx, rfl⟩
            exact hall c hmem
          have hs := dst_some_cons_succ b rest i c h hle
          change dst (some b :: rest) (i + 1) = Option.map (fun v => v + 1) (dst rest i)
          rw [hs]
          simp [dst, h]

theorem hall_of_sorted (b : Nat) (rest : List (Option Nat))
    (h : (some b :: rest).Pairwise (fun a c => key a ≤ key c)) :
    ∀ c ∈ active rest, b ≤ c := by
  rw [List.pairwise_cons] at h
  intro c hc
  have hmem : some c ∈ rest := by
    unfold active at hc
    rcases List.mem_filterMap.mp hc with ⟨x, hxmem, hx⟩
    cases x with
    | none => simp at hx
    | some d =>
        simp at hx
        subst d
        exact hxmem
  have hkey := h.1 (some c) hmem
  dsimp [key] at hkey
  omega

theorem addresses_sorted : ∀ xs : List (Option Nat),
    xs.Pairwise (fun a c => key a ≤ key c) →
    addresses xs = List.range (active xs).length := by
  intro xs
  induction xs with
  | nil => intro; simp [addresses, active]
  | cons a rest ih =>
      intro h
      rw [List.pairwise_cons] at h
      cases a with
      | none =>
          rw [addresses_none_cons, ih h.2]
          simp [active]
      | some b =>
          have hall : ∀ c ∈ active rest, b ≤ c := hall_of_sorted b rest (List.pairwise_cons.mpr h)
          rw [addresses_some_cons b rest hall, ih h.2]
          simp [active, List.range_succ_eq_map]

theorem sound : DenseStableScatterPermutation.Goal := by
  intro xs
  let S := xs.mergeSort (fun a b : Option Nat => decide (key a ≤ key b))
  have hperm : S.Perm xs := by
    dsimp [S]
    exact List.mergeSort_perm xs _
  have hsorted : S.Pairwise (fun a c => key a ≤ key c) := by
    dsimp [S]
    simpa using (List.pairwise_mergeSort
      (α := Option Nat) (le := fun a b : Option Nat => decide (key a ≤ key b))
      (by intro a b c hab hbc; simp at hab hbc ⊢; exact Nat.le_trans hab hbc)
      (by intro a b; by_cases h : key a ≤ key b <;> simp [h] <;> omega) xs)
  have haddr : addresses S = List.range (active S).length := addresses_sorted S hsorted
  have hswap : (addresses S).Perm (addresses xs) := swizzle S xs hperm
  have hlen : (active S).length = (active xs).length := (hperm.filterMap id).length_eq
  rw [haddr] at hswap
  simpa [hlen] using hswap.symm

end TinylambOpt.ResearchProofs.JitKernels.Goal_RadixCursors_DenseStableScatterPermutation
