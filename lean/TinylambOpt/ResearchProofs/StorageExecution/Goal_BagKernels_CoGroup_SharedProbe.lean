import TinylambOpt.ResearchGoals.StorageExecution.BagKernels

/-!
A grouped or join aggregate can operate on distinct keys and multiplicities instead of
expanded non-NULL rows when the weighted result agrees.

Proved

- `CoGroup.SharedProbe` — `∀ a b c, weighted bag (a,b) (fun k => c.count k) = weighted
  bag (b,c) (fun k => a.count k)`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_BagKernels_CoGroup_SharedProbe

open TinylambOpt.ResearchGoals.StorageExecution.BagKernels
open TinylambOpt.ResearchGoals.StorageExecution.BagKernels.CoGroup

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
          have hkane : k ≠ a := by
            intro h
            subst k
            simp at hane
          have hka : (k == a) = false := Bool.eq_false_of_ne_true (by
            intro h
            exact hkane (beq_iff_eq.mp h))
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
          simpa [rest] using sum_map_eq_count_head k ks f
        rw [hks]
        omega
  exact H keys rfl

/-! ## Triple-count sum identity -/

private theorem count_cons_map_sum (x : Nat) (xs b : List Nat) (f : Nat → Nat) :
    (b.map fun k => (x :: xs).count k * f k).sum =
      b.count x * f x + (b.map fun k => xs.count k * f k).sum := by
  induction b with
  | nil => simp
  | cons y ys ih =>
      simp only [List.map_cons, List.sum_cons]
      rw [ih]
      by_cases h : y == x
      · have hyx : y = x := beq_iff_eq.mp h
        subst y
        simp [List.count_cons, Nat.add_mul, Nat.add_assoc, Nat.add_comm, Nat.add_left_comm]
      · have hxy : (x == y) = false := Bool.eq_false_of_ne_true (by
          intro hh
          exact h (beq_iff_eq.mpr (beq_iff_eq.mp hh).symm))
        simp [List.count_cons, h, hxy, Nat.add_assoc, Nat.add_comm, Nat.add_left_comm]

private theorem sum_map_zero (l : List Nat) : (l.map fun _ => 0).sum = 0 := by
  induction l with
  | nil => rfl
  | cons _ _ ih => simp [ih]

private theorem triple_sum (a b c : List Nat) :
    (a.map fun k => b.count k * c.count k).sum =
      (b.map fun k => a.count k * c.count k).sum := by
  induction a with
  | nil => simp [sum_map_zero]
  | cons x xs ih =>
      simp only [List.map_cons, List.sum_cons]
      rw [ih]
      rw [count_cons_map_sum x xs b c.count]

/-! ## Goal -/

-- Sanity check: both sides agree on a small instance.
#eval (weighted bag ([1, 2, 2, 3], [2, 3, 3, 4]) (fun k => [3, 3, 1].count k),
      weighted bag ([2, 3, 3, 4], [3, 3, 1]) (fun k => [1, 2, 2, 3].count k))

theorem sound_SharedProbe : SharedProbe.Goal := by
  intro a b c
  unfold weighted bag
  simp only [Prod.fst]
  calc
    (a.eraseDups.map fun k => (a.count k * b.count k) * c.count k).sum =
        (a.eraseDups.map fun k => a.count k * (b.count k * c.count k)).sum := by
          apply congrArg List.sum
          apply List.map_congr_left
          intro k hk
          rw [Nat.mul_assoc]
    _ = (a.map fun k => b.count k * c.count k).sum :=
          dedup_weight_sum (fun k => b.count k * c.count k) a
    _ = (b.map fun k => a.count k * c.count k).sum :=
          triple_sum a b c
    _ = (b.map fun k => c.count k * a.count k).sum := by
          apply congrArg List.sum
          apply List.map_congr_left
          intro k hk
          rw [Nat.mul_comm (a.count k) (c.count k)]
    _ = (b.eraseDups.map fun k => b.count k * (c.count k * a.count k)).sum := by
          symm
          exact dedup_weight_sum (fun k => c.count k * a.count k) b
    _ = (b.eraseDups.map fun k => (b.count k * c.count k) * a.count k).sum := by
          apply congrArg List.sum
          apply List.map_congr_left
          intro k hk
          rw [Nat.mul_assoc]

end TinylambOpt.ResearchProofs.StorageExecution.Goal_BagKernels_CoGroup_SharedProbe
