import TinylambOpt.ResearchGoals.StorageExecution.BagKernels

/-!
A grouped or join aggregate can operate on distinct keys and multiplicities instead of
expanded non-NULL rows when the weighted result agrees.

Proved

- `IntervalCover.AntiRevenue` — Compute revenue of all left rows whose key is absent on
  the right.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_BagKernels_IntervalCover_AntiRevenue

open TinylambOpt.ResearchGoals.StorageExecution.BagKernels
open TinylambOpt.ResearchGoals.StorageExecution.BagKernels.IntervalCover
open TinylambOpt.ResearchGoals.Compression.Wire

private theorem countP_eq_sum_toNat {α : Type} (p : α → Bool) (l : List α) :
    l.countP p = (l.map (fun x => (p x).toNat)).sum := by
  induction l with
  | nil => simp
  | cons x xs ih =>
      by_cases h : p x = true <;> simp [ih, h, Nat.add_comm]

private theorem count_range_map_add (s n k : Nat) :
    List.count k ((List.range n).map (fun i => s + i)) =
      (decide (s ≤ k && k < s + n)).toNat := by
  induction n with
  | zero =>
      by_cases hs : s ≤ k
      · have hk : ¬ k < s := by omega
        simp [hs, hk]
      · simp [hs]
  | succ n ih =>
      rw [List.range_succ, List.map_append, List.count_append, ih]
      simp only [List.map_singleton]
      clear ih
      by_cases hs : s ≤ k
      · by_cases hk1 : k < s + n
        · have hk2 : k < s + (n + 1) := by omega
          have hne : s + n ≠ k := by omega
          simp [hs, hk1, hk2, hne]
        · by_cases hkn : s + n == k
          · have heq : s + n = k := by simpa using hkn
            have hk2 : k < s + (n + 1) := by omega
            simp [hs, heq, hk2]
          · have hne : s + n ≠ k := by simpa using hkn
            have hk2 : ¬ k < s + (n + 1) := by omega
            simp [hs, hk1, hne, hk2]
      · have hne : s + n ≠ k := by omega
        simp [hs, hne]

private theorem count_flat_eq (rs : List (Nat × Nat)) (k : Nat) :
    (rs.flatMap (fun (s, n) => (List.range n).map (fun i => s + i))).count k =
      rs.countP (fun r : Nat × Nat => decide (r.1 ≤ k) && decide (k < r.1 + r.2)) := by
  induction rs with
  | nil => simp
  | cons r rs ih =>
      simp [ih, count_range_map_add, countP_eq_sum_toNat]

private theorem count_expand_eq_mass (rs : List (Nat × Nat)) (k : Nat) :
    (bag.expand rs).count k = bag.mass rs k := by
  simpa [bag] using count_flat_eq rs k

private theorem peak_cons (x : Nat) (xs : List Nat) : peak (x :: xs) = max x (peak xs) := by
  unfold peak
  rw [List.max?_cons]
  cases h : xs.max? with
  | none => simp
  | some m => simp

private theorem mem_le_peak (xs : List Nat) {v : Nat} (hv : v ∈ xs) : v ≤ peak xs := by
  induction xs with
  | nil => simp at hv
  | cons x xs ih =>
      rw [peak_cons]
      simp at hv
      rcases hv with hv | hv
      · subst v
        exact Nat.le_max_left x (peak xs)
      · exact Nat.le_trans (ih hv) (Nat.le_max_right x (peak xs))

private theorem expand_mem_lt_peak (rs : List (Nat × Nat)) {k : Nat}
    (hk : k ∈ bag.expand rs) :
    k < peak (rs.map (fun r : Nat × Nat => r.1 + r.2)) := by
  simp only [bag] at hk
  rw [List.mem_flatMap] at hk
  rcases hk with ⟨⟨s, n⟩, hmem, hk⟩
  rw [List.mem_map] at hk
  rcases hk with ⟨i, hi, rfl⟩
  have hi' : i < n := by simpa using hi
  have hle : s + n ≤ peak (rs.map (fun r : Nat × Nat => r.1 + r.2)) := by
    apply mem_le_peak
    exact List.mem_map.mpr ⟨(s, n), hmem, rfl⟩
  omega

private theorem eraseDups_eq_self_of_nodup {α : Type} [BEq α] [LawfulBEq α]
    (l : List α) (hl : l.Nodup) : l.eraseDups = l := by
  induction l with
  | nil => rfl
  | cons x xs ih =>
      have hx : x ∉ xs := (List.nodup_cons.mp hl).1
      have hxs : xs.Nodup := (List.nodup_cons.mp hl).2
      rw [List.eraseDups_cons]
      have hfilter : List.filter (fun b : α => !b == x) xs = xs := by
        apply List.filter_eq_self.mpr
        intro b hb
        have hne : b ≠ x := by
          intro h
          apply hx
          simpa [h] using hb
        simp [hne]
      rw [hfilter, ih hxs]

private theorem eraseDups_range (P : Nat) : (List.range P).eraseDups = List.range P :=
  eraseDups_eq_self_of_nodup (List.range P) List.nodup_range

private theorem sum_eq_zero_of_pointwise {α : Type} (l : List α) (g : α → Nat)
    (h : ∀ x ∈ l, g x = 0) : (l.map g).sum = 0 := by
  induction l with
  | nil => simp
  | cons x xs ih =>
      simp [h x (by simp), ih (fun y hy => h y (by simp [hy]))]

private theorem sum_map_add {α : Type} (l : List α) (g h : α → Nat) :
    (l.map (fun x => g x + h x)).sum = (l.map g).sum + (l.map h).sum := by
  induction l with
  | nil => simp
  | cons x xs ih =>
      simp [ih, Nat.add_assoc, Nat.add_left_comm]

private theorem sum_range_singleton (P x : Nat) (hx : x < P) (f : Nat → Nat) :
    ((List.range P).map (fun k => List.count k [x] * f k)).sum = f x := by
  induction P with
  | zero => omega
  | succ P ih =>
      by_cases h : x == P
      · have heq : x = P := by simpa using h
        subst x
        have hnot : ∀ k ∈ List.range P, List.count k [P] = 0 := by
          intro k hk
          apply List.count_eq_zero_of_not_mem
          have hklt : k < P := by simpa using hk
          have hkne : k ≠ P := Nat.ne_of_lt hklt
          simp [hkne]
        have hsum0 : ((List.range P).map (fun k => List.count k [P] * f k)).sum = 0 := by
          apply sum_eq_zero_of_pointwise
          intro k hk
          simp [hnot k hk]
        simp [List.range_succ, hsum0]
      · have hne : x ≠ P := by simpa using h
        have hxP : x < P := by omega
        have hcnt : List.count P [x] = 0 := by simp [hne]
        rw [List.range_succ, List.map_append, List.sum_append, ih hxP]
        simp [hcnt]

private theorem sum_count_add (P : Nat) (xs ys : List Nat) (f : Nat → Nat) :
    ((List.range P).map (fun k => (xs.count k + ys.count k) * f k)).sum =
      ((List.range P).map (fun k => xs.count k * f k)).sum +
      ((List.range P).map (fun k => ys.count k * f k)).sum := by
  induction P with
  | zero => simp
  | succ P ih =>
      simp [List.range_succ, Nat.add_mul, sum_map_add, Nat.add_assoc, Nat.add_left_comm, Nat.add_comm]

private theorem sum_map_eq_range_count (P : Nat) (xs : List Nat)
    (hxs : ∀ x ∈ xs, x < P) (f : Nat → Nat) :
    (xs.map f).sum = ((List.range P).map (fun k => xs.count k * f k)).sum := by
  induction xs with
  | nil =>
      simpa using (sum_eq_zero_of_pointwise (List.range P) (fun _ => (0 : Nat)) (by simp)).symm
  | cons x xs ih =>
      have hxlt : x < P := hxs x (by simp)
      have hxs' : ∀ y ∈ xs, y < P := by
        intro y hy
        exact hxs y (by simp [hy])
      have hcnt : ∀ k : Nat, List.count k (x :: xs) = List.count k [x] + List.count k xs := by
        intro k
        simpa using (List.count_append (a := k) (l₁ := [x]) (l₂ := xs))
      calc
        (List.map f (x :: xs)).sum
            = f x + (List.map f xs).sum := by simp
        _ = ((List.range P).map (fun k => List.count k [x] * f k)).sum +
            ((List.range P).map (fun k => List.count k xs * f k)).sum := by
              rw [sum_range_singleton P x hxlt f, ih hxs']
        _ = ((List.range P).map (fun k => (List.count k [x] + List.count k xs) * f k)).sum := by
              rw [sum_count_add]
        _ = ((List.range P).map (fun k => List.count k (x :: xs) * f k)).sum := by
              simp [hcnt, Nat.add_mul]

private theorem weighted_eq_expand_sum (rs : List (Nat × Nat)) (f : Nat → Nat) :
    weighted bag rs f = ((bag.expand rs).map f).sum := by
  let P := peak (rs.map (fun r : Nat × Nat => r.1 + r.2))
  have hkeys : bag.keys rs = List.range P := by rfl
  unfold weighted
  rw [hkeys, eraseDups_range]
  rw [sum_map_eq_range_count P (bag.expand rs)]
  · simp [count_expand_eq_mass]
  · intro x hx
    exact expand_mem_lt_peak rs hx

private theorem filter_sum_eq_map_ite (l : List Nat) (p : Nat → Bool) :
    (l.filter p).sum = (l.map (fun x => if p x then x else 0)).sum := by
  induction l with
  | nil => simp
  | cons x xs ih =>
      by_cases h : p x = true <;> simp [h, ih]

theorem sound_AntiRevenue : IntervalCover.AntiRevenue.Goal := by
  intro a c
  rw [weighted_eq_expand_sum]
  have hbool : ∀ x : Nat, (bag.mass c x == 0) = (!(bag.expand c).contains x) := by
    intro x
    rw [← count_expand_eq_mass c x]
    by_cases hmem : x ∈ bag.expand c
    · have hcnt : (bag.expand c).count x ≠ 0 := by
        intro hz
        have hpos : 0 < (bag.expand c).count x := List.count_pos_iff.mpr hmem
        omega
      simp [hcnt, hmem]
    · have hcnt : (bag.expand c).count x = 0 := List.count_eq_zero_of_not_mem hmem
      simp [hcnt, hmem]
  rw [filter_sum_eq_map_ite]
  simp [hbool]

end TinylambOpt.ResearchProofs.StorageExecution.Goal_BagKernels_IntervalCover_AntiRevenue
