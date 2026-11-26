import TinylambOpt.ResearchGoals.StorageExecution.BagKernels

/-!
A grouped or join aggregate can operate on distinct keys and multiplicities instead of
expanded non-NULL rows when the weighted result agrees.

Proved

- `MultiplicityRuns.OuterWeight` — LEFT JOIN contributes one unmatched right
  placeholder or the complete right multiplicity.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_BagKernels_MultiplicityRuns_OuterWeight

open TinylambOpt.ResearchGoals.StorageExecution.BagKernels
open TinylambOpt.ResearchGoals.StorageExecution.BagKernels.MultiplicityRuns

private def lhs (a c : List (Nat × Nat)) : Nat :=
  weighted bag a (fun x => max 1 (bag.mass c x))

private def rhs (a c : List (Nat × Nat)) : Nat :=
  (pairs (bag.expand a) (bag.expand c) (· == ·)).length +
  ((bag.expand a).filter (fun x => !(bag.expand c).contains x)).length

#eval lhs [(3,2),(7,3)] [(7,1)]
#eval rhs [(3,2),(7,3)] [(7,1)]
#eval lhs [(3,2),(3,4),(7,0)] [(3,1),(5,2)]
#eval rhs [(3,2),(3,4),(7,0)] [(3,1),(5,2)]
#eval lhs [(3,0),(7,3)] []
#eval rhs [(3,0),(7,3)] []

example : lhs [(3,2),(7,3)] [(7,1)] = rhs [(3,2),(7,3)] [(7,1)] := by decide
example : lhs [(3,2),(3,4),(7,0)] [(3,1),(5,2)] = rhs [(3,2),(3,4),(7,0)] [(3,1),(5,2)] := by decide
example : lhs [(3,0),(7,3)] [] = rhs [(3,0),(7,3)] [] := by decide

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
            have hn : ks.length + 1 = n := by simpa using hlen
            have hlt : (ks.filter fun b => !(b == k)).length < n := by
              have hle := List.length_filter_le (fun b : Nat => !(b == k)) ks
              omega
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
            have hn : ks.length + 1 = n := by simpa using hlen
            have hlt : (ks.filter fun b => !(b == k)).length < n := by
              have hle := List.length_filter_le (fun b : Nat => !(b == k)) ks
              omega
            have hih := ih ((ks.filter fun b => !(b == k)).length) hlt
              (ks.filter fun b => !(b == k)) rfl xs f hsub'
            rw [hzero]
            simp
            exact hih

private theorem sum_count_of_sub (xs ks : List Nat) (f : Nat → Nat)
    (hsub : ∀ x ∈ xs, x ∈ ks) :
    (ks.eraseDups.map (fun k => xs.count k * f k)).sum = (xs.map f).sum := by
  exact sum_count_of_sub_aux ks.length ks rfl xs f hsub

private theorem mass_eq_count (rs : List (Nat × Nat)) (k : Nat) :
    bag.mass rs k = (bag.expand rs).count k := by
  induction rs with
  | nil => simp [bag]
  | cons r rs ih =>
      rcases r with ⟨x, n⟩
      by_cases h : (x == k) = true
      · have hm : bag.mass ((x, n) :: rs) k = n + bag.mass rs k := by simp [bag, h]
        have he : bag.expand ((x, n) :: rs) = List.replicate n x ++ bag.expand rs := rfl
        rw [hm, he, List.count_append, ih, List.count_replicate]
        simp [h]
      · have hm : bag.mass ((x, n) :: rs) k = bag.mass rs k := by simp [bag, h]
        have he : bag.expand ((x, n) :: rs) = List.replicate n x ++ bag.expand rs := rfl
        rw [hm, he, List.count_append, ih, List.count_replicate]
        simp [h]

private theorem mem_expand_mem_keys {rs : List (Nat × Nat)} {x : Nat} :
    x ∈ bag.expand rs → x ∈ rs.map Prod.fst := by
  intro h
  rcases List.mem_flatMap.mp h with ⟨⟨y, n⟩, hmem, hrep⟩
  have hyx : x = y := List.eq_of_mem_replicate hrep
  simpa [hyx] using (List.mem_map_of_mem (f := Prod.fst) hmem)

private theorem weighted_eq_sum_expand (rs : List (Nat × Nat)) (f : Nat → Nat) :
    weighted bag rs f = ((bag.expand rs).map f).sum := by
  unfold weighted
  change (((rs.map Prod.fst).eraseDups.map fun k => bag.mass rs k * f k).sum) =
    ((bag.expand rs).map f).sum
  have hmass :
      ((rs.map Prod.fst).eraseDups.map fun k => bag.mass rs k * f k) =
        ((rs.map Prod.fst).eraseDups.map fun k => (bag.expand rs).count k * f k) := by
    apply List.map_congr_left
    intro k hk
    rw [mass_eq_count]
  rw [hmass]
  exact sum_count_of_sub (bag.expand rs) (rs.map Prod.fst) f (by
    intro x hx
    exact mem_expand_mem_keys hx)

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
    by_cases hmem : x ∈ ys
    · have hnzero : ys.count x ≠ 0 := by
        intro hz
        exact (List.count_eq_zero.mp hz) hmem
      have hc : ys.contains x = true := List.contains_iff_mem.mpr hmem
      have hpred : (!(ys.contains x)) = false := by rw [hc]; rfl
      have hleft : (if (!(ys.contains x)) then 1 else 0) = 0 := by
        rw [hpred]
        rfl
      have hright : (if ys.count x = 0 then 1 else 0) = 0 := by simp [hnzero]
      exact hleft.trans hright.symm
    · have hz : ys.count x = 0 := List.count_eq_zero.mpr hmem
      have hc : ys.contains x = false := by
        cases hval : ys.contains x
        · rfl
        · exact False.elim (hmem (List.contains_iff_mem.mp hval))
      have hpred : (!(ys.contains x)) = true := by rw [hc]; rfl
      have hleft : (if (!(ys.contains x)) then 1 else 0) = 1 := by
        rw [hpred]
        rfl
      have hright : (if ys.count x = 0 then 1 else 0) = 1 := by simp [hz]
      exact hleft.trans hright.symm
  have hdecomp : (xs.map fun x => max 1 (ys.count x)).sum =
      (xs.map fun x => ys.count x + if ys.count x = 0 then 1 else 0).sum := by
    apply congrArg List.sum
    apply List.map_congr_left
    intro x hx
    exact max_one_eq (ys.count x)
  rw [hleft, hdecomp, sum_map_add, ← hpairs, ← hunmatched]

end TinylambOpt.ResearchProofs.StorageExecution.Goal_BagKernels_MultiplicityRuns_OuterWeight
