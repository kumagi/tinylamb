import TinylambOpt.ResearchGoals.StorageExecution.BagKernels

/-!
A grouped or join aggregate can operate on distinct keys and multiplicities instead of
expanded non-NULL rows when the weighted result agrees.

Proved

- `CoGroup.IntersectAll` — Intersect two bags by the minimum multiplicity at each key.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_BagKernels_CoGroup_IntersectAll

open TinylambOpt.ResearchGoals.StorageExecution.BagKernels
open TinylambOpt.ResearchGoals.StorageExecution.BagKernels.CoGroup

/-! ## Generic list lemmas -/

private theorem eraseDups_nodup {α : Type} [BEq α] [LawfulBEq α] (l : List α) :
    l.eraseDups.Nodup := by
  let motive : Nat → Prop := fun n => ∀ l : List α, l.length = n → l.eraseDups.Nodup
  have h : motive l.length := Nat.strongRecOn l.length (fun n ih l hl => by
    cases l with
    | nil => simp [List.eraseDups_nil]
    | cons b bs =>
        rw [List.eraseDups_cons, List.nodup_cons]
        constructor
        · intro hb
          rw [List.mem_eraseDups] at hb
          have hb' := (List.mem_filter.mp hb).2
          simp at hb'
        · apply ih (bs.filter (fun x => !x == b)).length
          · have hle := List.length_filter_le (fun x : α => !x == b) bs
            simp only [List.length_cons] at hl
            omega
          · rfl)
  exact h l rfl

private theorem count_flatMap_replicate (xs : List Nat) (f : Nat → Nat) (k : Nat) :
    (xs.flatMap (fun x => List.replicate (f x) x)).count k =
      (xs.map (fun x => if x = k then f x else 0)).sum := by
  induction xs with
  | nil => simp
  | cons x xs ih =>
      simp [ih, List.count_replicate, beq_iff_eq]

private theorem sum_eq_single_of_nodup (xs : List Nat) (hn : xs.Nodup) (k : Nat)
    (f : Nat → Nat) :
    (xs.map (fun x => if x = k then f x else 0)).sum = if k ∈ xs then f k else 0 := by
  induction xs with
  | nil => simp
  | cons x xs ih =>
      rw [List.nodup_cons] at hn
      simp only [List.map_cons, List.sum_cons]
      by_cases hxk : x = k
      · subst x
        have hnot : k ∉ xs := hn.1
        simpa [hnot] using (ih hn.2)
      · have hi := ih hn.2
        have hkx : k ≠ x := fun h => hxk h.symm
        by_cases hk : k ∈ xs
        · simp [hxk, hkx, hk, hi]
        · simp [hxk, hkx, hk, hi]

private theorem beq_comm_nat (a b : Nat) : (a == b) = (b == a) := by
  by_cases h : a = b
  · subst b
    rfl
  · have h' : b ≠ a := by intro hba; exact h hba.symm
    rw [beq_eq_false_iff_ne.mpr h, beq_eq_false_iff_ne.mpr h']

private theorem count_filter_beq (ys : List Nat) (x k : Nat) :
    (ys.filter (fun y => y == x)).count k = (k == x).toNat * ys.count k := by
  cases hkx : (k == x)
  · have hzero : (ys.filter (fun y => y == x)).count k = 0 := by
      rw [List.count_eq_zero_of_not_mem]
      intro hm
      rw [List.mem_filter] at hm
      have hk_eq_x : k = x := eq_of_beq hm.2
      subst k
      simp at hkx
    rw [hzero]
    simp
  · have hcount : (ys.filter (fun y => y == x)).count k = ys.count k := by
      rw [List.count_filter (a := k) (p := fun y : Nat => y == x) hkx]
    rw [hcount]
    simp

/-! ## CoGroup bag facts -/

private theorem count_expand_eq_mass (xs ys : List Nat) (k : Nat) :
    (bag.expand (xs, ys)).count k = bag.mass (xs, ys) k := by
  induction xs with
  | nil => simp [bag]
  | cons x xs ih =>
      change List.count k ((x :: xs).flatMap (fun x => ys.filter (fun y => y == x))) =
        (x :: xs).count k * ys.count k
      rw [List.flatMap_cons, List.count_append]
      rw [show List.count k (List.flatMap (fun x => ys.filter (fun y => y == x)) xs) =
          xs.count k * ys.count k by simpa [bag] using ih]
      rw [List.count_cons, count_filter_beq]
      rw [beq_comm_nat x k]
      cases h : (k == x)
      · simp [Nat.add_comm]
      · simp [Nat.add_mul, Nat.add_comm]

private theorem mem_keys_of_mass_pos (xs ys : List Nat) (k : Nat)
    (h : 0 < bag.mass (xs, ys) k) : k ∈ bag.keys (xs, ys) := by
  change k ∈ xs
  dsimp [bag] at h
  have hxpos : 0 < xs.count k := Nat.pos_of_mul_pos_right h
  exact List.count_pos_iff.mp hxpos

private theorem nodup_live (p : List Nat × List Nat) : (live bag p).Nodup := by
  unfold live
  exact List.Sublist.nodup List.filter_sublist (eraseDups_nodup (bag.keys p))

private theorem mem_live_of_mass_pos (p : List Nat × List Nat) (k : Nat)
    (h : 0 < bag.mass p k) : k ∈ live bag p := by
  obtain ⟨xs, ys⟩ := p
  have hkeys : k ∈ bag.keys (xs, ys) := mem_keys_of_mass_pos xs ys k h
  unfold live
  rw [List.mem_filter]
  constructor
  · rw [List.mem_eraseDups]
    exact hkeys
  · simp [h]

/-! ## Goal -/

theorem sound_IntersectAll : IntersectAll.Goal := by
  intro a c k
  obtain ⟨xs, ys⟩ := a
  obtain ⟨us, vs⟩ := c
  rw [count_flatMap_replicate]
  rw [sum_eq_single_of_nodup (live bag (xs, ys)) (nodup_live (xs, ys))]
  rw [count_expand_eq_mass, count_expand_eq_mass]
  by_cases hk : k ∈ live bag (xs, ys)
  · simp [hk]
  · have hm0 : bag.mass (xs, ys) k = 0 := by
      by_cases hz : bag.mass (xs, ys) k = 0
      · exact hz
      · exact False.elim (hk (mem_live_of_mass_pos (xs, ys) k (Nat.pos_of_ne_zero hz)))
    simp [hk, hm0]

end TinylambOpt.ResearchProofs.StorageExecution.Goal_BagKernels_CoGroup_IntersectAll
