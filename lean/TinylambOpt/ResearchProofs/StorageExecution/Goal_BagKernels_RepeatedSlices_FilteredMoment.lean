import TinylambOpt.ResearchGoals.StorageExecution.BagKernels

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_BagKernels_RepeatedSlices_FilteredMoment

open TinylambOpt.ResearchGoals.StorageExecution.BagKernels
open TinylambOpt.ResearchGoals.StorageExecution.BagKernels.RepeatedSlices

private abbrev B := bag

/-! ## Sanity checks on small inputs (including empty and repeated keys) -/

#eval bag.expand ([1, 2, 1] : List Nat)
#eval weighted bag ([1, 2, 1] : List Nat) (fun x => if 1 ≤ x && x < 3 then x * x else 0)
#eval (((bag.expand ([1, 2, 1] : List Nat)).filter (fun x => 1 ≤ x && x < 3)).map (fun x => x * x)).sum

example : weighted bag ([] : List Nat) (fun x => if 1 ≤ x && x < 3 then x * x else 0) =
    (((bag.expand ([] : List Nat)).filter (fun x => 1 ≤ x && x < 3)).map (fun x => x * x)).sum := by decide
example : weighted bag ([7, 7, 7] : List Nat) (fun x => if 0 ≤ x && x < 8 then x * x else 0) =
    (((bag.expand ([7, 7, 7] : List Nat)).filter (fun x => 0 ≤ x && x < 8)).map (fun x => x * x)).sum := by decide
example : weighted bag ([3, 1, 4, 1, 5, 9, 2, 6] : List Nat) (fun x => if 2 ≤ x && x < 7 then x * x else 0) =
    (((bag.expand ([3, 1, 4, 1, 5, 9, 2, 6] : List Nat)).filter (fun x => 2 ≤ x && x < 7)).map (fun x => x * x)).sum := by decide

/-! ## RepeatedSlices bag facts -/

private theorem sum_map_succ (l : List Nat) : (l.map (fun x => x + 1)).sum = l.sum + l.length := by
  induction l with
  | nil => simp
  | cons x xs ih => simp [ih, Nat.add_assoc, Nat.add_comm, Nat.add_left_comm]

private theorem zipIdx_shift (xs : List Nat) (i : Nat) :
    xs.zipIdx i = (xs.zipIdx).map (fun e => (e.1, e.2 + i)) := by
  induction i with
  | zero => simp
  | succ i ih =>
      rw [List.zipIdx_succ, ih, List.map_map]
      apply List.map_congr_left
      intro e he
      simp [Nat.add_assoc, Nat.add_comm, Nat.add_left_comm]

private theorem filter_zipIdx_shift (xs : List Nat) (k d i : Nat) :
    (((xs.zipIdx i).map (fun e => (e.1, e.2 + d))).filter (fun e => e.1 == k)) =
      (((xs.zipIdx i).filter (fun e => e.1 == k)).map (fun e => (e.1, e.2 + d))) := by
  induction xs generalizing i with
  | nil => simp
  | cons a xs ih =>
      rw [List.zipIdx_cons]
      simp only [List.map_cons, List.filter_cons]
      by_cases h : a == k <;> simp [h, ih]

private theorem zipIdx_filter_count (xs : List Nat) (k i : Nat) :
    ((xs.zipIdx i).filter (fun e => e.1 == k)).length = xs.count k := by
  induction xs generalizing i with
  | nil => simp
  | cons a xs ih =>
      rw [List.zipIdx_cons, List.filter_cons]
      by_cases h : a == k <;> simp [h, ih, List.count_cons]

private theorem zipIdx_one_sum (xs : List Nat) (k : Nat) :
    (((xs.zipIdx 1).filter (fun e => e.1 == k)).map (fun e => e.2 + 1)).sum =
      B.mass xs k + xs.count k := by
  rw [zipIdx_shift xs 1]
  rw [filter_zipIdx_shift xs k 1 0]
  rw [List.map_map]
  change ((((xs.zipIdx.filter (fun e : Nat × Nat => e.1 == k)).map
      (fun e => (e.2 + 1) + 1)).sum) = B.mass xs k + xs.count k)
  have hmap : ((xs.zipIdx.filter (fun e : Nat × Nat => e.1 == k)).map
        (fun e => (e.2 + 1) + 1)) =
      (((xs.zipIdx.filter (fun e : Nat × Nat => e.1 == k)).map (fun e => e.2 + 1)).map
        (fun x => x + 1)) := by
    rw [List.map_map]
    apply List.map_congr_left
    intro e he
    rfl
  rw [hmap, sum_map_succ, List.length_map, zipIdx_filter_count xs k 0]
  simp [B, bag]

private theorem mass_cons (a k : Nat) (xs : List Nat) :
    B.mass (a :: xs) k = (if a == k then 1 else 0) + B.mass xs k + xs.count k := by
  change (((((a :: xs).zipIdx).filter (fun e : Nat × Nat => e.1 == k)).map
      (fun e => e.2 + 1)).sum) =
    (if a == k then 1 else 0) + B.mass xs k + xs.count k
  rw [List.zipIdx_cons]
  by_cases h : a == k
  · simp [h, zipIdx_one_sum] <;> omega
  · simp [h, zipIdx_one_sum] <;> omega

private theorem range_succ_cons (n : Nat) :
    List.range (n + 1) = 0 :: (List.range n).map (fun i => i + 1) := by
  induction n with
  | zero => simp
  | succ n ih =>
      rw [List.range_succ, ih]
      have h := congrArg (List.map (fun i => i + 1)) ih
      simpa [List.range_succ, List.map_append] using h

private theorem expand_cons (a : Nat) (xs : List Nat) :
    B.expand (a :: xs) = (a :: xs) ++ B.expand xs := by
  unfold B bag
  simp only
  rw [show (a :: xs).length = xs.length + 1 by simp]
  rw [range_succ_cons]
  simp only [List.flatMap_cons, List.drop_zero, List.flatMap_map, List.drop_succ_cons]

private theorem count_expand_eq_mass (xs : List Nat) (k : Nat) :
    (B.expand xs).count k = B.mass xs k := by
  induction xs with
  | nil => simp [B, bag]
  | cons a xs ih =>
      rw [expand_cons, List.count_append, ih, List.count_cons]
      rw [mass_cons]
      omega

private theorem mem_expand_iff (xs : List Nat) (k : Nat) : k ∈ B.expand xs ↔ k ∈ xs := by
  induction xs with
  | nil => simp [B, bag]
  | cons a xs ih =>
      rw [expand_cons, List.mem_append, ih]
      simp

/-! ## Generic list lemmas -/

private theorem sum_map_eq_count_head {α : Type} [BEq α] [LawfulBEq α]
    (k : α) (ks : List α) (f : α → Nat) :
    (ks.map f).sum = ks.count k * f k + ((ks.filter (fun b => !(b == k))).map f).sum := by
  induction ks with
  | nil => simp
  | cons x xs ih =>
      by_cases hx : x == k
      · have hxeq : x = k := beq_iff_eq.mp hx
        subst x
        simp [ih, Nat.succ_mul, Nat.add_assoc, Nat.add_comm]
      · have hkx : (k == x) = false := Bool.eq_false_of_ne_true (by
          intro h
          exact hx (beq_iff_eq.mpr (beq_iff_eq.mp h).symm))
        simp [List.count_cons, hx, ih, Nat.add_left_comm]

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
          have hka : (k == a) = false := Bool.eq_false_of_ne_true (by
            intro h
            have hkaeq : k = a := beq_iff_eq.mp h
            have haeq : (a == k) = true := beq_iff_eq.mpr hkaeq.symm
            simp [haeq] at hane)
          simp [List.count_cons, hka, hrestc]
        rw [show ((rest.eraseDups.map fun a => List.count a (k :: ks) * f a).sum) =
             ((rest.eraseDups.map fun a => rest.count a * f a).sum) by
             apply congrArg List.sum
             apply List.map_congr_left
             intro a ha
             rw [hrest_count a ha]]
        rw [ihrest]
        have hks : (ks.map f).sum = ks.count k * f k + (rest.map f).sum := by
          simpa [rest] using sum_map_eq_count_head k ks f
        rw [hks]
        omega
  exact H keys rfl

private theorem nodup_eraseDups {α : Type} [BEq α] [LawfulBEq α] : ∀ l : List α, l.eraseDups.Nodup
  | [] => by simp
  | a :: as => by
      rw [List.eraseDups_cons, List.nodup_cons]
      constructor
      · intro h
        have hf : a ∈ List.filter (fun b => !(b == a)) as := List.mem_eraseDups.mp h
        have hp := (List.mem_filter.mp hf).2
        simp at hp
      · exact nodup_eraseDups (List.filter (fun b => !(b == a)) as)
termination_by l => l.length
decreasing_by
  exact Nat.lt_succ_of_le (List.length_filter_le _ _)

private theorem sum_eq_of_nodup_mem (l₁ l₂ : List Nat) (f : Nat → Nat)
    (hnd₁ : l₁.Nodup) (hnd₂ : l₂.Nodup) (hmem : ∀ d, d ∈ l₁ ↔ d ∈ l₂) :
    (l₁.map f).sum = (l₂.map f).sum := by
  have hperm : l₁.Perm l₂ := (List.perm_ext_iff_of_nodup hnd₁ hnd₂).mpr hmem
  exact List.Perm.sum_nat (hperm.map f)

private theorem sum_filter_eq_sum_ite {α : Type} (L : List α) (p : α → Bool) (g : α → Nat) :
    ((L.filter p).map g).sum = (L.map fun x => if p x then g x else 0).sum := by
  induction L with
  | nil => simp
  | cons x xs ih => by_cases h : p x <;> simp [h, ih]

/-! ## Goal -/

theorem sound_FilteredMoment : FilteredMoment.Goal := by
  intro xs lo hi
  let f : Nat → Nat := fun x => if lo ≤ x && x < hi then x * x else 0
  let hsq : Nat → Nat := fun x => x * x
  let p : Nat → Bool := fun x => lo ≤ x && x < hi
  change ((B.keys xs).eraseDups.map (fun k => B.mass xs k * f k)).sum =
    (((B.expand xs).filter p).map hsq).sum

  have hR1 : (((B.expand xs).filter p).map hsq).sum = ((B.expand xs).map f).sum := by
    simpa [f, hsq, p] using (sum_filter_eq_sum_ite (B.expand xs) p hsq)

  have hR2 : ((B.expand xs).map f).sum =
      (((B.expand xs).eraseDups.map fun k => (B.expand xs).count k * f k).sum) := by
    exact (dedup_weight_sum f (B.expand xs)).symm

  have hR3 : ((B.expand xs).eraseDups.map fun k => (B.expand xs).count k * f k).sum =
      ((B.expand xs).eraseDups.map fun k => B.mass xs k * f k).sum := by
    apply congrArg List.sum
    apply List.map_congr_left
    intro k hk
    rw [count_expand_eq_mass]

  have hL : ((B.keys xs).eraseDups.map (fun k => B.mass xs k * f k)).sum =
      ((B.expand xs).eraseDups.map (fun k => B.mass xs k * f k)).sum := by
    apply sum_eq_of_nodup_mem
    · simpa [B, bag] using (nodup_eraseDups xs)
    · exact nodup_eraseDups _
    · intro d
      rw [List.mem_eraseDups, List.mem_eraseDups]
      change d ∈ xs ↔ d ∈ B.expand xs
      exact (mem_expand_iff xs d).symm

  calc
    ((B.keys xs).eraseDups.map (fun k => B.mass xs k * f k)).sum
        = ((B.expand xs).eraseDups.map (fun k => B.mass xs k * f k)).sum := hL
    _ = ((B.expand xs).eraseDups.map fun k => (B.expand xs).count k * f k).sum := hR3.symm
    _ = ((B.expand xs).map f).sum := hR2.symm
    _ = (((B.expand xs).filter p).map hsq).sum := hR1.symm

end TinylambOpt.ResearchProofs.StorageExecution.Goal_BagKernels_RepeatedSlices_FilteredMoment
