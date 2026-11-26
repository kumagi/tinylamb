import TinylambOpt.ResearchGoals.StorageExecution.BagKernels

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_BagKernels_IntervalCover_JoinFanout

open TinylambOpt.ResearchGoals.StorageExecution.BagKernels
open TinylambOpt.ResearchGoals.StorageExecution.BagKernels.IntervalCover
open TinylambOpt.ResearchGoals.Compression.Wire

private theorem count_eq_one_of_mem_nodup {α : Type} [DecidableEq α] {l : List α} {a : α}
    (hn : l.Nodup) (ha : a ∈ l) : l.count a = 1 := by
  induction l with
  | nil => cases ha
  | cons b bs ih =>
    rw [List.nodup_cons] at hn
    simp only [List.mem_cons] at ha
    rw [List.count_cons]
    by_cases hab : b = a
    · subst a
      have : bs.count b = 0 := List.count_eq_zero_of_not_mem hn.1
      simp [this]
    · have ha' : a ∈ bs := by
        cases ha with
        | inl heq => exact False.elim (hab heq.symm)
        | inr hmem => exact hmem
      have hba : (b == a) = false := by simpa [beq_eq_false_iff_ne] using hab
      simp [hba, ih hn.2 ha']

private theorem filter_ne_eq_self (a : Nat) (as : List Nat) (h : ∀ b ∈ as, b ≠ a) :
    List.filter (fun b : Nat => !b == a) as = as := by
  induction as with
  | nil => simp
  | cons b bs ih =>
    have hb : b ≠ a := h b (by simp)
    have ht : ∀ c ∈ bs, c ≠ a := by intro c hc; exact h c (by simp [hc])
    have hbne : (!b == a) = true := by simp [beq_eq_false_iff_ne, hb]
    simp [hbne, ih ht]

private theorem eraseDups_eq_self_of_nodup {l : List Nat} (h : l.Nodup) : l.eraseDups = l := by
  induction l with
  | nil => simp [List.eraseDups]
  | cons a as ih =>
    rw [List.nodup_cons] at h
    rw [List.eraseDups_cons]
    rw [filter_ne_eq_self a as (by intro b hb hba; subst b; exact h.1 hb)]
    rw [ih h.2]

private theorem sum_range_eq_single_aux (N a : Nat) (f : Nat → Nat) (ha : ¬ a < N) :
    ((List.range N).map (fun k => if a = k then f k else 0)).sum = 0 := by
  induction N with
  | zero => simp
  | succ N ih =>
    rw [List.range_succ, List.map_append, List.sum_append]
    have ha' : ¬ a < N := by omega
    have hne : a ≠ N := by omega
    simp [ih ha', hne]

private theorem sum_range_eq_single (N a : Nat) (f : Nat → Nat) (ha : a < N) :
    ((List.range N).map (fun k => if a = k then f k else 0)).sum = f a := by
  induction N with
  | zero => omega
  | succ N ih =>
    rw [List.range_succ, List.map_append, List.sum_append]
    by_cases h : a = N
    · subst a
      have hzero : ((List.range N).map (fun k => if N = k then f k else 0)).sum = 0 :=
        sum_range_eq_single_aux N N f (by omega)
      simp [hzero]
    · have ha' : a < N := by omega
      simp [h, ih ha']

private theorem sum_map_zero_range (N : Nat) :
    ((List.range N).map (fun _ : Nat => 0)).sum = 0 := by
  induction N with
  | zero => simp
  | succ N ih =>
    rw [List.range_succ, List.map_append, List.sum_append]
    simp [ih]

private theorem sum_range_count (xs : List Nat) (N : Nat) (f : Nat → Nat)
    (hN : ∀ x ∈ xs, x < N) :
    ((List.range N).map (fun k => xs.count k * f k)).sum = (xs.map f).sum := by
  induction xs with
  | nil => simp [sum_map_zero_range]
  | cons a as ih =>
    have ha : a < N := hN a (by simp)
    have has : ∀ x ∈ as, x < N := by intro x hx; exact hN x (by simp [hx])
    have ih' := ih has
    simp only [List.map_cons, List.sum_cons]
    have hcnt : (List.range N).map (fun k => (a::as).count k * f k) =
        (List.range N).map (fun k => as.count k * f k + (if a = k then f k else 0)) := by
      apply List.map_congr_left
      intro k hk
      rw [List.count_cons]
      by_cases h : a = k
      · simp [h, Nat.add_mul]
      · have hbe : (a == k) = false := by simpa [beq_eq_false_iff_ne] using h
        simp [hbe, h]
    rw [hcnt]
    have hsum_add :
        ((List.range N).map (fun k => as.count k * f k + (if a = k then f k else 0))).sum =
        ((List.range N).map (fun k => as.count k * f k)).sum +
        ((List.range N).map (fun k => if a = k then f k else 0)).sum := by
      induction List.range N with
      | nil => simp
      | cons x xs2 ih2 => simp [ih2, Nat.add_assoc, Nat.add_left_comm]
    rw [hsum_add, ih', sum_range_eq_single N a f ha]
    omega

private theorem count_range_shift (s n k : Nat) :
    ((List.range n).map (fun i => s + i)).count k = if s ≤ k && k < s + n then 1 else 0 := by
  let l := (List.range n).map (fun i => s + i)
  have hnodup : l.Nodup := by
    dsimp [l]
    induction n with
    | zero => simp
    | succ n ih =>
      rw [List.range_succ, List.map_append, List.nodup_append]
      refine ⟨ih, by simp, ?_⟩
      intro a ha b hb
      simp at hb
      subst b
      simp only [List.mem_map] at ha
      obtain ⟨i, hi, rfl⟩ := ha
      simp only [List.mem_range] at hi
      omega
  have hmem : k ∈ l ↔ s ≤ k && k < s + n := by
    dsimp [l]
    simp only [List.mem_map, List.mem_range]
    constructor
    · rintro ⟨i, hi, rfl⟩
      simp
      omega
    · intro h
      have h' : s ≤ k ∧ k < s + n := by simpa using h
      refine ⟨k - s, ?_, ?_⟩
      · omega
      · have hsk : k - s + s = k := Nat.sub_add_cancel h'.1
        omega
  by_cases hm : k ∈ l
  · rw [count_eq_one_of_mem_nodup hnodup hm]
    have hcond : s ≤ k && k < s + n := hmem.mp hm
    simp [hcond]
  · rw [List.count_eq_zero_of_not_mem hm]
    have hcond : ¬ (s ≤ k && k < s + n) := by
      intro h
      exact hm (hmem.mpr h)
    simp [hcond]

private theorem mass_eq_count (rs : List (Nat × Nat)) (k : Nat) :
    bag.mass rs k = (bag.expand rs).count k := by
  induction rs with
  | nil => simp [bag]
  | cons p rs ih =>
    rcases p with ⟨s, n⟩
    dsimp [bag]
    rw [List.countP_cons]
    simp only [List.flatMap_cons]
    rw [List.count_append]
    have ih' :
        List.countP (fun x : Nat × Nat => decide (x.1 ≤ k) && decide (k < x.1 + x.2)) rs =
          List.count k (List.flatMap (fun x : Nat × Nat => List.map (fun i => x.1 + i) (List.range x.2)) rs) := by
      simpa [bag] using ih
    rw [ih', count_range_shift]
    omega

private theorem peak_cons_eq (y : Nat) (ys : List Nat) : peak (y :: ys) = max y (peak ys) := by
  cases h : ys.max? with
  | none => simp [peak, List.max?_cons, h]
  | some m => simp [peak, List.max?_cons, h]

private theorem mem_le_peak {xs : List Nat} {x : Nat} (hx : x ∈ xs) : x ≤ peak xs := by
  induction xs with
  | nil => simp at hx
  | cons y ys ih =>
    rw [peak_cons_eq]
    simp only [List.mem_cons] at hx
    cases hx with
    | inl hxy =>
      subst x
      exact Nat.le_max_left _ _
    | inr hxys =>
      exact Nat.le_trans (ih hxys) (Nat.le_max_right _ _)

private theorem mem_expand_lt_peak {rs : List (Nat × Nat)} {x : Nat}
    (hx : x ∈ bag.expand rs) : x < peak (rs.map (fun p : Nat × Nat => p.1 + p.2)) := by
  induction rs with
  | nil => simp [bag] at hx
  | cons p rs ih =>
    rcases p with ⟨s, n⟩
    dsimp [bag] at hx ⊢
    simp only [List.flatMap_cons, List.mem_append] at hx
    simp only [List.map_cons]
    rw [peak_cons_eq]
    cases hx with
    | inl hx1 =>
      simp only [List.mem_map, List.mem_range] at hx1
      obtain ⟨i, hi, rfl⟩ := hx1
      exact Nat.lt_of_lt_of_le (by omega : s + i < s + n) (Nat.le_max_left _ _)
    | inr hx2 =>
      exact Nat.lt_of_lt_of_le (ih hx2) (Nat.le_max_right _ _)

private theorem weighted_eq_expand_sum (rs : List (Nat × Nat)) (f : Nat → Nat) :
    weighted bag rs f = ((bag.expand rs).map f).sum := by
  unfold weighted
  have hkeys : (bag.keys rs).eraseDups = bag.keys rs := by
    dsimp [bag]
    exact eraseDups_eq_self_of_nodup List.nodup_range
  rw [hkeys]
  dsimp [bag]
  rw [show
      (List.range (peak (rs.map (fun p : Nat × Nat => p.1 + p.2)))).map
          (fun k => List.countP (fun x : Nat × Nat => x.1 ≤ k && k < x.1 + x.2) rs * f k) =
      (List.range (peak (rs.map (fun p : Nat × Nat => p.1 + p.2)))).map
          (fun k => (bag.expand rs).count k * f k) by
    apply List.map_congr_left
    intro k hk
    simpa [bag] using congrArg (fun v => v * f k) (mass_eq_count rs k)]
  exact sum_range_count (bag.expand rs) (peak (rs.map (fun p : Nat × Nat => p.1 + p.2))) f
    (by intro x hx; exact mem_expand_lt_peak hx)

private theorem pairs_length (xs ys : List Nat) (pred : Nat → Nat → Bool) :
    (pairs xs ys pred).length = (xs.map (fun x => (ys.filter (pred x)).length)).sum := by
  simp [pairs, List.length_flatMap]

private theorem filter_length_toNat_sum (ys : List Nat) (x slack : Nat) :
    (ys.filter (fun y => x ≤ y && y ≤ x + slack)).length =
      ((ys.map (fun y => (decide (x ≤ y ∧ y ≤ x + slack)).toNat)).sum) := by
  induction ys with
  | nil => simp
  | cons y ys ih =>
    by_cases h : x ≤ y && y ≤ x + slack
    · have hp : x ≤ y ∧ y ≤ x + slack := by simpa using h
      simp [hp, ih]
      omega
    · have hp : ¬ (x ≤ y ∧ y ≤ x + slack) := by
        intro h'
        exact h (by simp [h'.1, h'.2])
      simp [h, ih]

theorem sound_JoinFanout : IntervalCover.JoinFanout.Goal := by
  intro a c slack
  calc
    weighted bag a (fun x => weighted bag c (fun y => (decide (x ≤ y ∧ y ≤ x + slack)).toNat))
        = ((bag.expand a).map
            (fun x => weighted bag c (fun y => (decide (x ≤ y ∧ y ≤ x + slack)).toNat))).sum := by
          exact weighted_eq_expand_sum a _
    _ = ((bag.expand a).map
          (fun x => ((bag.expand c).filter (fun y => x ≤ y && y ≤ x + slack)).length)).sum := by
          congr 1
          apply List.map_congr_left
          intro x hx
          rw [weighted_eq_expand_sum c (fun y => (decide (x ≤ y ∧ y ≤ x + slack)).toNat)]
          exact (filter_length_toNat_sum (bag.expand c) x slack).symm
    _ = (pairs (bag.expand a) (bag.expand c) (fun x y => x ≤ y && y ≤ x + slack)).length := by
          rw [pairs_length]

end TinylambOpt.ResearchProofs.StorageExecution.Goal_BagKernels_IntervalCover_JoinFanout
