import TinylambOpt.ResearchGoals.StorageExecution.BagKernels

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_BagKernels_RepeatedSlices_OuterWeight

open TinylambOpt.ResearchGoals.StorageExecution.BagKernels
open TinylambOpt.ResearchGoals.StorageExecution.BagKernels.RepeatedSlices

private def lhs (a c : List Nat) : Nat :=
  weighted bag a (fun x => max 1 (bag.mass c x))

private def rhs (a c : List Nat) : Nat :=
  (pairs (bag.expand a) (bag.expand c) (· == ·)).length +
  ((bag.expand a).filter (fun x => !(bag.expand c).contains x)).length

#eval lhs [2,1,2] [1,3,1,1]
#eval rhs [2,1,2] [1,3,1,1]
#eval lhs [] [1]
#eval rhs [] [1]
#eval lhs [1] []
#eval rhs [1] []

example : lhs [2,1,2] [1,3,1,1] = rhs [2,1,2] [1,3,1,1] := by decide
example : lhs [] [1] = rhs [] [1] := by decide
example : lhs [1] [] = rhs [1] [] := by decide

private def smallSlices : List Nat := [0, 1, 2]

#eval (smallSlices.flatMap fun a => smallSlices.flatMap fun b =>
  smallSlices.flatMap fun c => smallSlices.flatMap fun d =>
    if lhs [a,b] [c,d] == rhs [a,b] [c,d] then [] else [([a,b],[c,d])]).length

/-! ## Generic list lemmas -/

private theorem split_count (l : List Nat) (x : Nat) (f : Nat → Nat) :
    (l.map f).sum = l.count x * f x + ((l.filter fun y => !(y == x)).map f).sum := by
  induction l with
  | nil => simp
  | cons y l ih =>
      by_cases hy : y = x
      · subst y
        simp [ih, Nat.add_mul, Nat.add_assoc, Nat.add_comm]
      · have hbeq : (y == x) = false := by simp [hy]
        simp [hbeq, hy, ih, Nat.add_left_comm]

private theorem sum_count_of_sub_aux : ∀ n (ks : List Nat), ks.length = n →
    ∀ (xs : List Nat) (f : Nat → Nat), (∀ x ∈ xs, x ∈ ks) →
    (ks.eraseDups.map (fun k => xs.count k * f k)).sum = (xs.map f).sum := by
  intro n
  induction n using Nat.strongRecOn with
  | ind n ih =>
      intro ks hlen xs f hsub
      cases ks with
      | nil =>
          have hxnil : xs = [] := List.eq_nil_iff_forall_not_mem.mpr (by
            intro x hx
            simpa using hsub x hx)
          subst xs
          simp
      | cons k ks =>
          rw [List.eraseDups_cons]
          simp only [List.map_cons, List.sum_cons]
          by_cases hk : k ∈ xs
          · let xs' := xs.filter (fun y => !(y == k))
            have hnotk : ∀ y ∈ xs', y ≠ k := by
              intro y hy
              have hp : (!(y == k)) = true := (List.mem_filter.mp hy).2
              intro hyk
              subst y
              simp at hp
            have hsub' : ∀ y ∈ xs', y ∈ ks.filter (fun b => !(b == k)) := by
              intro y hy
              have hyks : y ∈ ks := by
                have hyall := hsub y (List.mem_filter.mp hy).1
                rcases List.mem_cons.mp hyall with hEq | hTail
                · exact False.elim (hnotk y hy hEq)
                · exact hTail
              exact List.mem_filter.mpr ⟨hyks, by simp [hnotk y hy]⟩
            have hweights :
                ((ks.filter fun b => !(b == k)).eraseDups.map (fun y => xs.count y * f y)) =
                  ((ks.filter fun b => !(b == k)).eraseDups.map (fun y => xs'.count y * f y)) := by
              apply List.map_congr_left
              intro y hy
              have hyks : y ∈ ks.filter (fun b => !(b == k)) := List.mem_eraseDups.mp hy
              have hp : (!(y == k)) = true := (List.mem_filter.mp hyks).2
              have hcf : List.count y (List.filter (fun b : Nat => !(b == k)) xs) = xs.count y :=
                List.count_filter hp
              exact congrArg (fun t => t * f y) hcf.symm
            have hlt : (ks.filter fun b => !(b == k)).length < n := by
              rw [← hlen]
              simp only [List.length_cons]
              exact Nat.lt_of_le_of_lt (List.length_filter_le (fun b : Nat => !(b == k)) ks)
                (Nat.lt_succ_self _)
            have hih := ih ((ks.filter fun b => !(b == k)).length) hlt
              (ks.filter fun b => !(b == k)) rfl xs' f hsub'
            rw [hweights, hih]
            exact (split_count xs k f).symm
          · have hzero : xs.count k = 0 := List.count_eq_zero.mpr hk
            have hsub' : ∀ y ∈ xs, y ∈ ks.filter (fun b => !(b == k)) := by
              intro y hy
              have hyks : y ∈ ks := by
                have hyall := hsub y hy
                rcases List.mem_cons.mp hyall with hEq | hTail
                · exact False.elim (hk (by simpa [hEq] using hy))
                · exact hTail
              have hyne : y ≠ k := by
                intro hEq
                apply hk
                simpa [hEq] using hy
              exact List.mem_filter.mpr ⟨hyks, by simp [hyne]⟩
            have hlt : (ks.filter fun b => !(b == k)).length < n := by
              rw [← hlen]
              simp only [List.length_cons]
              exact Nat.lt_of_le_of_lt (List.length_filter_le (fun b : Nat => !(b == k)) ks)
                (Nat.lt_succ_self _)
            have hih := ih ((ks.filter fun b => !(b == k)).length) hlt
              (ks.filter fun b => !(b == k)) rfl xs f hsub'
            rw [hzero]
            simp
            exact hih

private theorem sum_count_of_sub (xs ks : List Nat) (f : Nat → Nat)
    (hsub : ∀ x ∈ xs, x ∈ ks) :
    (ks.eraseDups.map (fun k => xs.count k * f k)).sum = (xs.map f).sum := by
  exact sum_count_of_sub_aux ks.length ks rfl xs f hsub

private theorem length_filter_eq_sum (l : List Nat) (p : Nat → Bool) :
    (l.filter p).length = (l.map fun x => if p x then 1 else 0).sum := by
  induction l with
  | nil => simp
  | cons x l ih => by_cases hp : p x <;> simp [hp, ih, Nat.add_comm]

private theorem sum_map_add (l : List Nat) (g h : Nat → Nat) :
    (l.map fun x => g x + h x).sum = (l.map g).sum + (l.map h).sum := by
  induction l with
  | nil => simp
  | cons x l ih => simp [ih, Nat.add_assoc, Nat.add_left_comm]

private theorem max_one_eq (n : Nat) : max 1 n = n + if n = 0 then 1 else 0 := by
  by_cases h : n = 0
  · simp [h]
  · have hle : 1 ≤ n := by omega
    rw [Nat.max_eq_right hle]
    simp [h]

/-! ## RepeatedSlices bag facts -/

private theorem expand_cons (x : Nat) (xs : List Nat) :
    bag.expand (x :: xs) = (x :: xs) ++ bag.expand xs := by
  unfold bag
  simp only [List.length_cons]
  rw [List.range_succ_eq_map, List.flatMap_cons]
  congr 1
  rw [List.flatMap_map]
  simp [List.drop_succ_cons]

private theorem zipIdx_succ (xs : List Nat) (n : Nat) :
    xs.zipIdx (n + 1) = (xs.zipIdx n).map (fun e : Nat × Nat => (e.1, e.2 + 1)) := by
  induction xs generalizing n with
  | nil => simp
  | cons y ys ih => simp [List.zipIdx_cons, ih (n + 1), Nat.add_assoc]

private theorem filter_zipIdx_length (xs : List Nat) (n k : Nat) :
    ((xs.zipIdx n).filter (fun e => e.1 == k)).length = xs.count k := by
  induction xs generalizing n with
  | nil => simp
  | cons y ys ih =>
      rw [List.zipIdx_cons, List.count_cons]
      by_cases h : y == k
      · have hyk : y = k := by simpa using h
        simp [hyk, ih (n + 1)]
      · have hyk : y ≠ k := by intro hy; apply h; simp [hy]
        simp [h, ih (n + 1)]

private theorem sum_map_succ (z : List (Nat × Nat)) :
    (z.map (fun e => e.2 + 1 + 1)).sum = (z.map (fun e => e.2 + 1)).sum + z.length := by
  induction z with
  | nil => simp
  | cons e z ih => simp [ih]; omega

private theorem sum_succ_filter_zip (xs : List Nat) (n k : Nat) :
    (((xs.zipIdx (n + 1)).filter fun e => e.1 == k).map fun e => e.2 + 1).sum =
      (((xs.zipIdx n).filter fun e => e.1 == k).map fun e => e.2 + 1).sum +
        ((xs.zipIdx n).filter fun e => e.1 == k).length := by
  rw [zipIdx_succ]
  rw [List.filter_map]
  rw [List.map_map]
  apply sum_map_succ

private theorem mass_cons (x k : Nat) (xs : List Nat) :
    bag.mass (x :: xs) k = (if x == k then 1 else 0) + bag.mass xs k + xs.count k := by
  unfold bag
  simp only [List.zipIdx_cons]
  rw [show (0 + 1 : Nat) = 1 by rfl]
  have hs := sum_succ_filter_zip xs 0 k
  rw [show (0 + 1 : Nat) = 1 by rfl] at hs
  by_cases h : x == k
  · have hyk : x = k := by simpa using h
    simp [hyk, hs, filter_zipIdx_length xs 0 k]
    omega
  · have hyk : x ≠ k := by intro hx; apply h; simp [hx]
    simp [h, hs, filter_zipIdx_length xs 0 k]

private theorem count_expand_eq_mass (xs : List Nat) (k : Nat) :
    (bag.expand xs).count k = bag.mass xs k := by
  induction xs with
  | nil => simp [bag]
  | cons x xs ih =>
      rw [expand_cons, List.count_append, ih, mass_cons]
      simp [List.count_cons]
      omega

private theorem mem_keys_of_mem_expand (xs : List Nat) (k : Nat) :
    k ∈ bag.expand xs → k ∈ bag.keys xs := by
  intro h
  induction xs with
  | nil => simp [bag] at h
  | cons x xs ih =>
      rw [expand_cons] at h
      rw [List.mem_append] at h
      rcases h with hHead | hTail
      · simpa [bag] using hHead
      · have hk : k ∈ xs := by simpa [bag] using (ih hTail)
        simpa [bag] using (List.mem_cons_of_mem x hk)

private theorem weighted_eq_sum_expand (p : List Nat) (f : Nat → Nat) :
    weighted bag p f = ((bag.expand p).map f).sum := by
  unfold weighted
  have hmass :
      ((bag.keys p).eraseDups.map fun k => bag.mass p k * f k) =
        ((bag.keys p).eraseDups.map fun k => (bag.expand p).count k * f k) := by
    apply List.map_congr_left
    intro k hk
    rw [← count_expand_eq_mass p k]
  rw [hmass]
  exact sum_count_of_sub (bag.expand p) (bag.keys p) f (by
    intro x hx
    exact mem_keys_of_mem_expand p x hx)

/-! ## Goal -/

theorem sound_OuterWeight : OuterWeight.Goal := by
  intro a c
  let xs := bag.expand a
  let ys := bag.expand c
  have hleft : weighted bag a (fun x => max 1 (bag.mass c x)) =
      ((xs.map fun x => max 1 (ys.count x)).sum) := by
    rw [weighted_eq_sum_expand]
    change ((xs.map fun x => max 1 (bag.mass c x)).sum) =
      ((xs.map fun x => max 1 (ys.count x)).sum)
    apply congrArg List.sum
    apply List.map_congr_left
    intro x hx
    dsimp [ys]
    rw [count_expand_eq_mass]
  have hpairs : (pairs xs ys (· == ·)).length = (xs.map fun x => ys.count x).sum := by
    unfold pairs
    rw [List.length_flatMap]
    apply congrArg List.sum
    apply List.map_congr_left
    intro x hx
    rw [List.length_map]
    simpa [BEq.comm] using (List.count_eq_length_filter (a := x) (l := ys)).symm
  have hunmatched : (xs.filter fun x => !(ys.contains x)).length =
      (xs.map fun x => if ys.count x = 0 then 1 else 0).sum := by
    rw [length_filter_eq_sum]
    apply congrArg List.sum
    apply List.map_congr_left
    intro x hx
    by_cases h : ys.contains x = true
    · have hmem : x ∈ ys := List.contains_iff_mem.mp h
      have hnzero : ys.count x ≠ 0 := by
        intro hz
        exact (List.count_eq_zero.mp hz) hmem
      simp [hnzero, hmem]
    · have hnotmem : x ∉ ys := by
        intro hmem
        exact h (List.contains_iff_mem.mpr hmem)
      have hz : ys.count x = 0 := List.count_eq_zero.mpr hnotmem
      simp [hz, hnotmem]
  have hdecomp : (xs.map fun x => max 1 (ys.count x)).sum =
      (xs.map fun x => ys.count x + if ys.count x = 0 then 1 else 0).sum := by
    apply congrArg List.sum
    apply List.map_congr_left
    intro x hx
    exact max_one_eq (ys.count x)
  rw [hleft, hdecomp, sum_map_add, ← hpairs, ← hunmatched]

end TinylambOpt.ResearchProofs.StorageExecution.Goal_BagKernels_RepeatedSlices_OuterWeight
