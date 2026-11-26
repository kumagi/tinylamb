import TinylambOpt.ResearchGoals.StorageExecution.BagKernels

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_BagKernels_EdgeDegrees_OuterWeight

open TinylambOpt.ResearchGoals.StorageExecution.BagKernels
open TinylambOpt.ResearchGoals.StorageExecution.BagKernels.EdgeDegrees

private def lhs (a c : List (Nat × Nat)) : Nat :=
  weighted bag a (fun x => max 1 (bag.mass c x))

private def rhs (a c : List (Nat × Nat)) : Nat :=
  (pairs (bag.expand a) (bag.expand c) (· == ·)).length +
  ((bag.expand a).filter (fun x => !(bag.expand c).contains x)).length

#eval lhs [(0,1),(1,2)] [(1,2),(2,3)]
#eval rhs [(0,1),(1,2)] [(1,2),(2,3)]
#eval lhs [] [(1,1)]
#eval rhs [] [(1,1)]
#eval lhs [(1,1)] []
#eval rhs [(1,1)] []
#eval lhs [(0,0),(0,1),(2,2)] [(0,0),(3,4)]
#eval rhs [(0,0),(0,1),(2,2)] [(0,0),(3,4)]

example : lhs [(0,1),(1,2)] [(1,2),(2,3)] = rhs [(0,1),(1,2)] [(1,2),(2,3)] := by decide
example : lhs [] [(1,1)] = rhs [] [(1,1)] := by decide
example : lhs [(1,1)] [] = rhs [(1,1)] [] := by decide
example : lhs [(0,0),(0,1),(2,2)] [(0,0),(3,4)] = rhs [(0,0),(0,1),(2,2)] [(0,0),(3,4)] := by decide

private def smallEdges : List (Nat × Nat) :=
  [(0,0), (0,1), (1,2), (2,2)]

#eval (smallEdges.flatMap fun a => smallEdges.flatMap fun b =>
  smallEdges.flatMap fun c => smallEdges.flatMap fun d =>
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

/-! ## EdgeDegrees bag facts -/

private theorem count_expand_eq_mass (es : List (Nat × Nat)) (k : Nat) :
    (bag.expand es).count k = bag.mass es k := by
  induction es with
  | nil => simp [bag]
  | cons e es ih =>
      rcases e with ⟨a, b⟩
      by_cases ha : a == k <;> by_cases hb : b == k <;>
        simp [bag, List.count_cons, ha, hb] at ih ⊢ <;> omega

private theorem mem_keys_of_mem_expand (es : List (Nat × Nat)) (k : Nat) :
    k ∈ bag.expand es → k ∈ bag.keys es := by
  intro h
  change k ∈ es.flatMap (fun e : Nat × Nat => [e.1, e.2]) at h
  rw [List.mem_flatMap] at h
  rcases h with ⟨e, he, hk⟩
  rcases e with ⟨a, b⟩
  change k ∈ [a, b] at hk
  simp at hk
  change k ∈ (es.map Prod.fst) ++ (es.map Prod.snd)
  rcases hk with hka | hkb
  · exact List.mem_append.mpr (Or.inl (List.mem_map.mpr ⟨(a, b), he, hka.symm⟩))
  · exact List.mem_append.mpr (Or.inr (List.mem_map.mpr ⟨(a, b), he, hkb.symm⟩))

private theorem weighted_eq_sum_expand (p : List (Nat × Nat)) (f : Nat → Nat) :
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

end TinylambOpt.ResearchProofs.StorageExecution.Goal_BagKernels_EdgeDegrees_OuterWeight
