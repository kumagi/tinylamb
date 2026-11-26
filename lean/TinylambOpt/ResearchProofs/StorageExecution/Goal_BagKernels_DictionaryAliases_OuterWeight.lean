import TinylambOpt.ResearchGoals.StorageExecution.BagKernels

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_BagKernels_DictionaryAliases_OuterWeight

open TinylambOpt.ResearchGoals.StorageExecution.BagKernels
open TinylambOpt.ResearchGoals.StorageExecution.BagKernels.DictionaryAliases

private def lhs (a c : List Nat × List Nat) : Nat :=
  weighted bag a (fun x => max 1 (bag.mass c x))

private def rhs (a c : List Nat × List Nat) : Nat :=
  (pairs (bag.expand a) (bag.expand c) (· == ·)).length +
  ((bag.expand a).filter (fun x => !(bag.expand c).contains x)).length

#eval lhs ([3, 5, 3], [0, 1, 2, 1, 7, 0]) ([5, 3], [1, 0, 1, 2])
#eval rhs ([3, 5, 3], [0, 1, 2, 1, 7, 0]) ([5, 3], [1, 0, 1, 2])
#eval lhs ([], [0]) ([1], [])
#eval rhs ([], [0]) ([1], [])

private def cands : List (List Nat) := [[], [0], [1], [0, 0], [2, 1], [0, 1, 2], [3, 0, 3], [1, 1, 0, 2]]
private def checkAll : Bool :=
  (cands.flatMap fun d => cands.map fun ids => (d, ids)).all fun a =>
    (cands.flatMap fun d => cands.map fun ids => (d, ids)).all fun c =>
      lhs a c == rhs a c

example : checkAll := by decide

private theorem countP_comp_succ (p : Nat × Nat → Bool) (l : List (Nat × Nat)) :
    List.countP (p ∘ fun e : Nat × Nat => (e.1, e.2 + 1)) l =
      List.countP (fun e => p (e.1, e.2 + 1)) l := by
  induction l with
  | nil => simp
  | cons e l ih => simp [List.countP_cons, ih, Function.comp_apply]

private theorem zipIdx_countP (d : List Nat) (k i : Nat) :
    d.zipIdx.countP (fun e => (e.1 == k) && (e.2 == i)) =
      (decide (d[i]? == some k)).toNat := by
  induction d generalizing i with
  | nil => simp
  | cons x d ih =>
      cases i with
      | zero =>
          rw [List.zipIdx_cons, List.zipIdx_succ, List.countP_cons, List.countP_map]
          rw [countP_comp_succ]
          have hpred : (fun e : Nat × Nat => (e.1 == k) && (e.2 + 1 == 0)) =
              (fun _ : Nat × Nat => false) := by
            funext e
            simp
          rw [hpred]
          by_cases hx : x = k <;> simp [hx]
      | succ i =>
          have hpred : (fun e : Nat × Nat => (e.1 == k) && (e.2 + 1 == i + 1)) =
              (fun e : Nat × Nat => (e.1 == k) && (e.2 == i)) := by
            funext e
            by_cases h : e.2 = i
            · subst h
              simp
            · simp
          rw [List.zipIdx_cons, List.zipIdx_succ, List.countP_cons, List.countP_map]
          rw [countP_comp_succ, hpred, ih]
          simp [List.getElem?_cons_succ]

private theorem slots_count (d : List Nat) (k i : Nat) :
    (((d.zipIdx.filter (fun e => e.1 == k)).map Prod.snd).count i) =
      (decide (d[i]? == some k)).toNat := by
  rw [List.count_eq_countP, List.countP_map, List.countP_filter]
  have hp : (fun a : Nat × Nat => ((fun x => x == i) ∘ Prod.snd) a && a.1 == k) =
      (fun a : Nat × Nat => (a.1 == k) && (a.2 == i)) := by
    funext a
    cases a with
    | mk x y => simp [Bool.and_comm]
  rw [hp]
  exact zipIdx_countP d k i

private theorem sum_count_cons (s : List Nat) (ids : List Nat) (i : Nat) :
    (s.map fun j => (i :: ids).count j).sum =
      (s.map fun j => ids.count j).sum + s.count i := by
  induction s with
  | nil => simp
  | cons j s ih =>
      have h1 : (i :: ids).count j = ids.count j + if i == j then 1 else 0 := by
        rw [List.count_cons]
      have h2 : (j :: s).count i = s.count i + if j == i then 1 else 0 := by
        rw [List.count_cons]
      simp only [List.map_cons, List.sum_cons]
      rw [h1, h2]
      have hbeq : (i == j) = (j == i) := BEq.comm
      rw [hbeq]
      omega

private theorem sum_map_zero_pair (l : List (Nat × Nat)) :
    (l.map ((fun _ : Nat => 0) ∘ Prod.snd)).sum = 0 := by
  induction l with
  | nil => simp
  | cons e l ih => simp [ih]

private theorem mass_eq_count (d ids : List Nat) (k : Nat) :
    bag.mass (d, ids) k = (bag.expand (d, ids)).count k := by
  change (((d.zipIdx.filter (fun e => e.1 == k)).map (fun e => ids.count e.2)).sum) =
    (ids.filterMap (fun i => d[i]?)).count k
  rw [List.count_filterMap]
  let s := (d.zipIdx.filter (fun e => e.1 == k)).map Prod.snd
  rw [show List.map (fun e : Nat × Nat => ids.count e.2) (d.zipIdx.filter (fun e => e.1 == k)) =
      List.map (fun j => ids.count j) s by
    dsimp [s]
    rw [List.map_map]
    rfl]
  change (s.map fun j => ids.count j).sum = ids.countP (fun i => d[i]? == some k)
  induction ids with
  | nil =>
      simpa [s, List.map_map] using sum_map_zero_pair (d.zipIdx.filter (fun e => e.1 == k))
  | cons i ids ih =>
      rw [List.countP_cons]
      rw [sum_count_cons, ih, slots_count]
      by_cases h : d[i]? == some k <;> simp [h]

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


private theorem mem_keys_of_mem_expand (d ids : List Nat) (k : Nat) :
    k ∈ bag.expand (d, ids) → k ∈ bag.keys (d, ids) := by
  intro hk
  change k ∈ ids.filterMap (fun i => d[i]?) at hk
  rw [List.mem_filterMap] at hk
  rcases hk with ⟨i, hi, hdi⟩
  change k ∈ d
  rw [List.getElem?_eq_some_iff] at hdi
  rcases hdi with ⟨hn, hget⟩
  rw [← hget]
  exact List.getElem_mem hn

private theorem weighted_eq_sum_expand (p : List Nat × List Nat) (f : Nat → Nat) :
    weighted bag p f = ((bag.expand p).map f).sum := by
  rcases p with ⟨d, ids⟩
  unfold weighted
  change (((bag.keys (d, ids)).eraseDups.map fun k => bag.mass (d, ids) k * f k).sum) =
    ((bag.expand (d, ids)).map f).sum
  have hmass :
      ((bag.keys (d, ids)).eraseDups.map fun k => bag.mass (d, ids) k * f k) =
        ((bag.keys (d, ids)).eraseDups.map fun k => (bag.expand (d, ids)).count k * f k) := by
    apply List.map_congr_left
    intro k hk
    rw [mass_eq_count]
  rw [hmass]
  exact sum_count_of_sub (bag.expand (d, ids)) (bag.keys (d, ids)) f (by
    intro x hx
    exact mem_keys_of_mem_expand d ids x hx)

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

theorem sound_OuterWeight : DictionaryAliases.OuterWeight.Goal := by
  change outerWeight bag
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

end TinylambOpt.ResearchProofs.StorageExecution.Goal_BagKernels_DictionaryAliases_OuterWeight
