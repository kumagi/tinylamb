import TinylambOpt.ResearchGoals.StorageExecution.BagKernels

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_BagKernels_CoGroup_OuterWeight

open TinylambOpt.ResearchGoals.StorageExecution.BagKernels
open TinylambOpt.ResearchGoals.StorageExecution.BagKernels.CoGroup

private def lhs (a c : List Nat × List Nat) : Nat :=
  weighted bag a (fun x => max 1 (bag.mass c x))

private def rhs (a c : List Nat × List Nat) : Nat :=
  (pairs (bag.expand a) (bag.expand c) (· == ·)).length +
  ((bag.expand a).filter (fun x => !(bag.expand c).contains x)).length

#eval lhs ([1, 2], [1, 1, 2]) ([1, 2], [1, 3])
#eval rhs ([1, 2], [1, 1, 2]) ([1, 2], [1, 3])
#eval lhs ([1, 1, 2], [1, 0, 2]) ([2], [1, 2])
#eval rhs ([1, 1, 2], [1, 0, 2]) ([2], [1, 2])
#eval lhs ([], [3]) ([1], [2])
#eval rhs ([], [3]) ([1], [2])
#eval lhs ([1], [2]) ([], [])
#eval rhs ([1], [2]) ([], [])

example : lhs ([1, 2], [1, 1, 2]) ([1, 2], [1, 3]) = rhs ([1, 2], [1, 1, 2]) ([1, 2], [1, 3]) := by decide
example : lhs ([1, 1, 2], [1, 0, 2]) ([2], [1, 2]) = rhs ([1, 1, 2], [1, 0, 2]) ([2], [1, 2]) := by decide
example : lhs ([], [3]) ([1], [2]) = rhs ([], [3]) ([1], [2]) := by decide
example : lhs ([1], [2]) ([], []) = rhs ([1], [2]) ([], []) := by decide

private def smallLists : List (List Nat) :=
  [[], [0], [1], [2], [0, 1], [1, 1], [0, 2], [0, 1, 2]]

#eval (smallLists.flatMap fun a => smallLists.flatMap fun b =>
  smallLists.flatMap fun c => smallLists.flatMap fun d =>
    if lhs (a, b) (c, d) == rhs (a, b) (c, d) then [] else [((a, b), (c, d))]).length

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

private theorem dedup_weight_sum (f : Nat → Nat) (keys : List Nat) :
    ((keys.eraseDups.map fun k => keys.count k * f k).sum) = (keys.map f).sum := by
  let motive : Nat → Prop := fun n => ∀ keys : List Nat, keys.length = n →
      ((keys.eraseDups.map fun k => keys.count k * f k).sum) = (keys.map f).sum
  have H : motive keys.length := by
    apply Nat.strongRecOn
    intro n ih keys hlen
    cases keys with
    | nil => simp
    | cons k ks =>
        let rest := ks.filter (fun b => !(b == k))
        have hrestlen : rest.length < n := by
          rw [← hlen]
          simp [rest]
          exact Nat.lt_of_le_of_lt (List.length_filter_le (fun b : Nat => !(b == k)) ks)
            (Nat.lt_succ_self _)
        have ihrest := ih rest.length hrestlen rest rfl
        rw [List.eraseDups_cons]
        simp only [List.map_cons, List.sum_cons]
        have hhead : (List.count k (k :: ks)) * f k = f k + ks.count k * f k := by
          rw [List.count_cons]
          simp [Nat.add_mul, Nat.add_comm]
        rw [hhead]
        have hrest_count : ∀ a ∈ rest.eraseDups, (List.count a (k :: ks)) = rest.count a := by
          intro a ha
          have hane : (a == k) = false := by
            apply Bool.eq_false_of_ne_true
            intro h
            have hamem : a ∈ rest := List.mem_eraseDups.mp ha
            have hamem' : a ∈ ks.filter (fun b : Nat => !(b == k)) := by simpa [rest] using hamem
            have hff := (List.mem_filter.mp hamem').2
            simp [h] at hff
          have hrestc : rest.count a = ks.count a := by
            simp [rest, hane]
          have hka : (k == a) = false := by
            apply Bool.eq_false_of_ne_true
            intro h
            have ht : (a == k) = true := beq_iff_eq.mpr (beq_iff_eq.mp h).symm
            rw [ht] at hane
            contradiction
          rw [List.count_cons, hka, hrestc]
          simp
        rw [show ((rest.eraseDups.map fun a => List.count a (k :: ks) * f a).sum) =
             ((rest.eraseDups.map fun a => rest.count a * f a).sum) by
             apply congrArg List.sum
             apply List.map_congr_left
             intro a ha
             rw [hrest_count a ha]]
        rw [ihrest]
        have hks : (ks.map f).sum = ks.count k * f k + (rest.map f).sum := by
          simpa [rest] using split_count ks k f
        rw [hks]
        omega
  exact H keys rfl

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

/-! ## CoGroup bag facts -/

private theorem beq_comm_nat (a b : Nat) : (a == b) = (b == a) := by
  by_cases h : a = b
  · subst b
    rfl
  · have h' : b ≠ a := by intro hba; exact h hba.symm
    rw [beq_eq_false_iff_ne.mpr h, beq_eq_false_iff_ne.mpr h']

private theorem count_filter_beq (ys : List Nat) (x k : Nat) :
    (ys.filter (fun y => y == x)).count k = (k == x).toNat * ys.count k := by
  induction ys with
  | nil => simp
  | cons y ys ih =>
      by_cases hyx : y = x
      · subst y
        rw [show List.filter (fun z : Nat => z == x) (x :: ys) = x :: List.filter (fun z : Nat => z == x) ys by simp]
        rw [List.count_cons, List.count_cons, ih]
        by_cases hkx : k = x
        · subst k
          simp
        · have hxk : x ≠ k := by intro h; exact hkx h.symm
          have hkxbeq : (k == x) = false := by simp [hkx]
          simp [hxk, hkxbeq]
      · have hyxnot : ¬ (y == x) = true := by simp [hyx]
        rw [List.filter_cons_of_neg (p := fun z : Nat => z == x) hyxnot, List.count_cons, ih]
        by_cases hkx : k = x
        · subst k
          simp [hyx]
        · have hkxbeq : (k == x) = false := by simp [hkx]
          simp [hkxbeq]

private theorem count_expand_eq_mass (xs ys : List Nat) (k : Nat) :
    (bag.expand (xs, ys)).count k = bag.mass (xs, ys) k := by
  induction xs with
  | nil => simp [bag]
  | cons x xs ih =>
      dsimp [bag] at ih ⊢
      rw [List.flatMap_cons, List.count_append, ih, List.count_cons]
      rw [count_filter_beq]
      rw [beq_comm_nat x k]
      cases h : (k == x) <;> simp [Nat.add_mul, Nat.add_comm]

private theorem mass_eq_count (p : List Nat × List Nat) (k : Nat) :
    bag.mass p k = (bag.expand p).count k := by
  rcases p with ⟨xs, ys⟩
  exact (count_expand_eq_mass xs ys k).symm

private theorem mem_keys_of_mem_expand (xs ys : List Nat) (k : Nat) :
    k ∈ bag.expand (xs, ys) → k ∈ bag.keys (xs, ys) := by
  intro h
  change k ∈ xs
  rw [show bag.expand (xs, ys) = xs.flatMap (fun x => ys.filter (fun y => y == x)) by rfl] at h
  rw [List.mem_flatMap] at h
  rcases h with ⟨x, hx, hmem⟩
  rw [List.mem_filter] at hmem
  have heq : k = x := eq_of_beq hmem.2
  simpa [heq] using hx

private theorem weighted_eq_sum_expand (p : List Nat × List Nat) (f : Nat → Nat) :
    weighted bag p f = ((bag.expand p).map f).sum := by
  rcases p with ⟨a, b⟩
  unfold weighted
  change (((bag.keys (a, b)).eraseDups.map fun k => bag.mass (a, b) k * f k).sum) =
    ((bag.expand (a, b)).map f).sum
  have hmass :
      ((bag.keys (a, b)).eraseDups.map fun k => bag.mass (a, b) k * f k) =
        ((bag.keys (a, b)).eraseDups.map fun k => (bag.expand (a, b)).count k * f k) := by
    apply List.map_congr_left
    intro k hk
    rw [mass_eq_count]
  rw [hmass]
  exact sum_count_of_sub (bag.expand (a, b)) (bag.keys (a, b)) f (by
    intro x hx
    exact mem_keys_of_mem_expand a b x hx)

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
    rw [mass_eq_count]
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

end TinylambOpt.ResearchProofs.StorageExecution.Goal_BagKernels_CoGroup_OuterWeight
