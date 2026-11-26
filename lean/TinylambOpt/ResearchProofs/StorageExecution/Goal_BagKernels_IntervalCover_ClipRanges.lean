import TinylambOpt.ResearchGoals.StorageExecution.BagKernels

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_BagKernels_IntervalCover_ClipRanges

open TinylambOpt.ResearchGoals.Compression.Wire
open TinylambOpt.ResearchGoals.StorageExecution.BagKernels
open TinylambOpt.ResearchGoals.StorageExecution.BagKernels.IntervalCover

private theorem eraseDups_of_nodup : ∀ {l : List Nat}, l.Nodup → l.eraseDups = l
| [], _ => rfl
| a :: as, h => by
    rw [List.eraseDups_cons]
    have htail : as.Nodup := (List.nodup_cons.mp h).2
    have hne : ∀ b ∈ as, b ≠ a := by
      intro b hb hba
      exact (List.nodup_cons.mp h).1 (by simpa [hba] using hb)
    have hfilter : (as.filter (fun b => !b == a)) = as := by
      apply List.filter_eq_self.mpr
      intro b hb
      simp [hne b hb]
    simp [hfilter, eraseDups_of_nodup htail]

private theorem eraseDups_range (n : Nat) : (List.range n).eraseDups = List.range n :=
  eraseDups_of_nodup List.nodup_range

private theorem le_peak {xs : List Nat} {x : Nat} (hx : x ∈ xs) : x ≤ peak xs := by
  cases xs with
  | nil => simp at hx
  | cons a as =>
      cases hm : (a :: as).max? with
      | none =>
          rw [List.max?_cons] at hm
          simp at hm
      | some m =>
          have hmm := (List.max?_eq_some_iff).mp hm
          simpa [peak, hm] using hmm.2 x hx

private theorem sum_map_zero {f : Nat → Nat} : ∀ {l : List Nat}, (∀ x ∈ l, f x = 0) → (l.map f).sum = 0
| [], _ => by simp
| a :: l, h => by
    have ha : f a = 0 := h a (by simp)
    have hl : (l.map f).sum = 0 := sum_map_zero (fun x hx => h x (by simp [hx]))
    simp [ha, hl]

private theorem sum_map_add (f g : Nat → Nat) : ∀ l : List Nat,
    (l.map (fun k => f k + g k)).sum = (l.map f).sum + (l.map g).sum
| [] => by simp
| a :: l => by
    simp [sum_map_add f g l, Nat.add_assoc, Nat.add_left_comm]

private theorem shifted_count (s lim : Nat) : ∀ n : Nat,
    ((List.range n).map (fun i => (decide (s + i < lim)).toNat)).sum = min n (lim - s)
| 0 => by simp
| n + 1 => by
    rw [List.range_succ]
    simp only [List.map_append, List.map_cons, List.map_nil, List.sum_append, List.sum_cons,
      List.sum_nil, Nat.add_zero]
    rw [shifted_count s lim n]
    by_cases h : s + n < lim
    · simp [h]
      omega
    · simp [h]
      omega

private theorem single_prefix (s n lim : Nat) :
    ((List.range (s + n)).map
      (fun k => (decide (s ≤ k && k < s + n)).toNat * (decide (k < lim)).toNat)).sum
    = ((List.range n).map (fun i => (decide (s + i < lim)).toNat)).sum := by
  rw [List.range_add]
  simp only [List.map_append, List.sum_append]
  have h0 : ((List.range s).map
      (fun k => (decide (s ≤ k && k < s + n)).toNat * (decide (k < lim)).toNat)).sum = 0 := by
    apply sum_map_zero
    intro k hk
    simp only [List.mem_range] at hk
    have hnot : ¬s ≤ k := by omega
    simp [hnot]
  rw [h0, Nat.zero_add]
  rw [List.map_map]
  apply congrArg List.sum
  apply List.map_congr_left
  intro i hi
  simp only [List.mem_range] at hi
  have hs : s ≤ s + i := by omega
  have hlt : s + i < s + n := by omega
  simp [hs, hlt]

private theorem single (s n lim M : Nat) (hM : s + n ≤ M) :
    ((List.range M).map
      (fun k => (decide (s ≤ k && k < s + n)).toNat * (decide (k < lim)).toNat)).sum
    = min n (lim - s) := by
  have hM' : M = (s + n) + (M - (s + n)) := by omega
  rw [hM', List.range_add]
  simp only [List.map_append, List.sum_append]
  rw [single_prefix, shifted_count]
  have h0 : (((List.range (M - (s + n))).map (fun i => (s + n) + i)).map
      (fun k => (decide (s ≤ k && k < s + n)).toNat * (decide (k < lim)).toNat)).sum = 0 := by
    rw [List.map_map]
    apply sum_map_zero
    intro i hi
    have hnot : ¬(s + n) + i < s + n := by omega
    simp [hnot]
  rw [h0, Nat.add_zero]

private def S (rs : List (Nat × Nat)) (M lim : Nat) :=
  ((List.range M).map
    (fun k => (rs.countP (fun (s, n) => s ≤ k && k < s + n)) * (decide (k < lim)).toNat)).sum

private theorem range_count_sum (rs : List (Nat × Nat)) (M lim : Nat)
    (h : ∀ r ∈ rs, r.1 + r.2 ≤ M) :
    S rs M lim = (rs.map (fun (s, n) => min n (lim - s))).sum := by
  induction rs generalizing M with
  | nil => simp [S, sum_map_zero]
  | cons r rs ih =>
      rcases r with ⟨s, n⟩
      have h1 : s + n ≤ M := h (s, n) (by simp)
      have ht : ∀ r ∈ rs, r.1 + r.2 ≤ M := by
        intro r hr
        exact h r (by simp [hr])
      simp only [S, List.countP_cons]
      simp only [Nat.add_mul]
      rw [sum_map_add]
      rw [← show S rs M lim = ((List.range M).map (fun k =>
        (rs.countP (fun (a, b) => a ≤ k && k < a + b)) * (decide (k < lim)).toNat)).sum from rfl]
      rw [ih M ht]
      rw [show ((List.range M).map (fun k =>
            (if (decide (s ≤ k) && decide (k < s + n)) = true then 1 else 0) *
              (decide (k < lim)).toNat)).sum =
          ((List.range M).map (fun k =>
            (decide (s ≤ k && k < s + n)).toNat * (decide (k < lim)).toNat)).sum by
            apply congrArg List.sum
            apply List.map_congr_left
            intro k hk
            by_cases hp : (decide (s ≤ k) && decide (k < s + n)) = true <;> simp [hp]]
      rw [single s n lim M h1]
      rw [Nat.add_comm]
      simp

theorem sound_ClipRanges : IntervalCover.ClipRanges.Goal := by
  intro rs lim
  rw [show weighted bag rs (fun x => (decide (x < lim)).toNat) =
      S rs (peak (rs.map (fun (s, n) => s + n))) lim by
    simp [weighted, bag, S, eraseDups_range]]
  apply range_count_sum
  intro r hr
  apply le_peak
  simp only [List.mem_map]
  exact ⟨r, hr, rfl⟩

end TinylambOpt.ResearchProofs.StorageExecution.Goal_BagKernels_IntervalCover_ClipRanges
