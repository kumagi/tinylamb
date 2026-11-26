import TinylambOpt.ResearchGoals.StorageExecution.BagKernels

/-!
A grouped or join aggregate can operate on distinct keys and multiplicities instead of
expanded non-NULL rows when the weighted result agrees.

Proved

- `RotationCycle.RangePerCycle` — `∀ m k s lo hi, lo≤hi → hi≤m+1 → weighted bag
  (k*(m+1),m,s) (fun v => (decide (lo≤v ∧ v<hi)).toNat) = k*(hi-lo)`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_BagKernels_RotationCycle_RangePerCycle

open TinylambOpt.ResearchGoals.StorageExecution.BagKernels
open TinylambOpt.ResearchGoals.StorageExecution.BagKernels.RotationCycle

private theorem sum_map_zero {α : Type} (l : List α) : (l.map (fun _ => 0)).sum = 0 := by
  induction l with
  | nil => simp
  | cons a as ih => simp [ih]

private theorem sum_map_add_nat {α : Type} (l : List α) (f g : α → Nat) :
    (l.map (fun x => f x + g x)).sum = (l.map f).sum + (l.map g).sum := by
  induction l with
  | nil => simp
  | cons a as ih =>
      simp [ih, Nat.add_assoc, Nat.add_left_comm]

private theorem swap_sum {α β : Type} (as : List α) (bs : List β) (f : α → β → Nat) :
    (as.map (fun a => (bs.map (fun b => f a b)).sum)).sum =
      (bs.map (fun b => (as.map (fun a => f a b)).sum)).sum := by
  induction as with
  | nil =>
      simp only [List.map_nil, List.sum_nil]
      exact (sum_map_zero bs).symm
  | cons a as ih =>
      simp only [List.map_cons, List.sum_cons] at ih ⊢
      rw [ih]
      exact (sum_map_add_nat bs (fun b => f a b)
        (fun b => (as.map (fun a' => f a' b)).sum)).symm

private theorem length_filter_eq_sum {α : Type} (p : α → Bool) (l : List α) :
    (l.filter p).length = (l.map (fun x => (p x).toNat)).sum := by
  induction l with
  | nil => simp
  | cons a as ih =>
      cases h : p a <;> simp [h, ih, Nat.add_comm]

private theorem count_eq_one_of_mem_nodup {α : Type} [BEq α] [LawfulBEq α]
    {l : List α} {a : α} (hn : l.Nodup) (ha : a ∈ l) : l.count a = 1 := by
  induction l with
  | nil => cases ha
  | cons b bs ih =>
      rw [List.nodup_cons] at hn
      simp only [List.mem_cons] at ha
      rw [List.count_cons]
      by_cases hba : b = a
      · subst a
        simp [List.count_eq_zero_of_not_mem hn.1]
      · have ha' : a ∈ bs := by
          cases ha with
          | inl h => exact (hba h.symm).elim
          | inr h => exact h
        have hbeq : (b == a) = false := by simp [hba]
        simp [hbeq, ih hn.2 ha']

private theorem shift_range_nodup (s n : Nat) : ((List.range n).map (fun i => s + i)).Nodup := by
  exact List.Pairwise.map (R := fun a b : Nat => a ≠ b) (S := fun a b : Nat => a ≠ b)
    (fun i => s + i) (by intro a b hne heq; omega) List.nodup_range

private theorem count_shift_range (s n k : Nat) :
    ((List.range n).map (fun i => s + i)).count k = (decide (s ≤ k ∧ k < s + n)).toNat := by
  by_cases hmem : k ∈ (List.range n).map (fun i => s + i)
  · have hcnt : ((List.range n).map (fun i => s + i)).count k = 1 :=
      count_eq_one_of_mem_nodup (shift_range_nodup s n) hmem
    have hcond : s ≤ k ∧ k < s + n := by
      rw [List.mem_map] at hmem
      obtain ⟨i, hi, hi_eq⟩ := hmem
      rw [List.mem_range] at hi
      omega
    simp [hcnt, hcond]
  · have hcnt : ((List.range n).map (fun i => s + i)).count k = 0 :=
      List.count_eq_zero_of_not_mem hmem
    have hcond : ¬ (s ≤ k ∧ k < s + n) := by
      intro hc
      apply hmem
      rw [List.mem_map]
      refine ⟨k - s, ?_, ?_⟩
      · rw [List.mem_range]
        omega
      · omega
    simp [hcnt, hcond]

private theorem nodup_eraseDups {α : Type} [BEq α] [LawfulBEq α] (l : List α) :
    l.eraseDups.Nodup := by
  refine (Nat.strongRecOn l.length
    (motive := fun n => ∀ l : List α, l.length = n → l.eraseDups.Nodup) ?_) l rfl
  intro n ih l hlen
  cases l with
  | nil => simp [List.eraseDups_nil]
  | cons b bs =>
      rw [List.eraseDups_cons, List.nodup_cons]
      constructor
      · intro hb
        rw [List.mem_eraseDups] at hb
        have hb' := (List.mem_filter.mp hb).2
        simp at hb'
      · have hle := List.length_filter_le (fun x => !x == b) bs
        have hn : n = bs.length + 1 := by simpa using hlen.symm
        have hlt : (bs.filter (fun x => !x == b)).length < n := by omega
        exact ih _ hlt _ rfl

private theorem filter_range_eq_singleton (a P : Nat) (ha : a < P) :
    (List.range P).filter (fun k => decide (a == k)) = [a] := by
  induction P with
  | zero => omega
  | succ P ih =>
      rw [List.range_succ, List.filter_append]
      by_cases h : a < P
      · rw [ih h]
        have hne : a ≠ P := by omega
        simp [hne]
      · have haeq : a = P := by omega
        subst a
        have hfil : (List.range P).filter (fun k => decide (P == k)) = [] := by
          rw [List.filter_eq_nil_iff]
          intro x hx
          rw [List.mem_range] at hx
          have hne : P ≠ x := by omega
          simp [hne]
        rw [hfil]
        simp

private theorem sum_indicator_eq_one (a P : Nat) (ha : a < P) :
    ((List.range P).map (fun k => (decide (a == k)).toNat)).sum = 1 := by
  rw [← length_filter_eq_sum (fun k => decide (a == k)) (List.range P)]
  rw [filter_range_eq_singleton a P ha]
  simp

private theorem sum_count_range (xs : List Nat) (P : Nat) (h : ∀ x ∈ xs, x < P) :
    ((List.range P).map (fun k => xs.count k)).sum = xs.length := by
  induction xs with
  | nil =>
      simpa using (sum_map_zero (List.range P))
  | cons a as ih =>
      have ha : a < P := h a (by simp)
      have has : ∀ x ∈ as, x < P := by
        intro x hx
        exact h x (by simp [hx])
      calc
        ((List.range P).map (fun k => (a :: as).count k)).sum
            = ((List.range P).map (fun k => as.count k + (decide (a == k)).toNat)).sum := by
                apply congrArg List.sum
                apply List.map_congr_left
                intro k hk
                by_cases hk' : a = k <;> simp [hk']
        _ = ((List.range P).map (fun k => as.count k)).sum +
              ((List.range P).map (fun k => (decide (a == k)).toNat)).sum := by
                rw [sum_map_add_nat]
        _ = as.length + 1 := by rw [ih has, sum_indicator_eq_one a P ha]
        _ = (a :: as).length := by simp

private theorem sum_cover_indicator (s n P : Nat) (h : s + n ≤ P) :
    ((List.range P).map (fun k => (decide (s ≤ k ∧ k < s + n)).toNat)).sum = n := by
  have hp : ∀ k, (decide (s ≤ k ∧ k < s + n)).toNat =
      ((List.range n).map (fun i => s + i)).count k := by
    intro k
    rw [count_shift_range]
  have hmap : (List.range P).map (fun k => (decide (s ≤ k ∧ k < s + n)).toNat) =
      (List.range P).map (fun k => ((List.range n).map (fun i => s + i)).count k) := by
    apply List.map_congr_left
    intro k hk
    exact hp k
  rw [hmap]
  rw [sum_count_range]
  · simp
  · intro x hx
    rw [List.mem_map] at hx
    obtain ⟨i, hi, rfl⟩ := hx
    rw [List.mem_range] at hi
    omega

private theorem perm_eraseDups_range (P : Nat) : (List.range P).eraseDups.Perm (List.range P) := by
  rw [List.perm_iff_count]
  intro k
  by_cases hk : k ∈ List.range P
  · rw [count_eq_one_of_mem_nodup (nodup_eraseDups (List.range P)) (by simpa using hk),
        count_eq_one_of_mem_nodup List.nodup_range hk]
  · rw [List.count_eq_zero_of_not_mem (by simpa using hk), List.count_eq_zero_of_not_mem hk]

private theorem sum_map_perm {l₁ l₂ : List Nat} (h : l₁.Perm l₂) (f : Nat → Nat) :
    (l₁.map f).sum = (l₂.map f).sum := by
  induction h with
  | nil => simp
  | cons a h ih => simp [ih]
  | swap a b l => simp [Nat.add_left_comm]
  | trans h₁ h₂ ih₁ ih₂ => exact ih₁.trans ih₂

private theorem sum_map_mul_left_nat (l : List Nat) (c : Nat) (f : Nat → Nat) :
    (l.map (fun x => c * f x)).sum = c * (l.map f).sum := by
  induction l with
  | nil => simp
  | cons a as ih =>
      simp [ih, Nat.mul_add]

theorem sound_RangePerCycle : RotationCycle.RangePerCycle.Goal := by
  intro m k s lo hi hlo hhi
  have hperm : (List.range (m + 1)).eraseDups.Perm (List.range (m + 1)) :=
    perm_eraseDups_range (m + 1)
  have hmass : ∀ v ∈ List.range (m + 1), bag.mass (k * (m + 1), m, s) v = k := by
    intro v hv
    rw [List.mem_range] at hv
    have hvle : v ≤ m := by omega
    simp [bag, hvle]
  rw [weighted]
  change ((List.range (m + 1)).eraseDups.map
      (fun v => bag.mass (k * (m + 1), m, s) v * (decide (lo ≤ v ∧ v < hi)).toNat)).sum =
    k * (hi - lo)
  rw [sum_map_perm hperm]
  have hmap : (List.range (m + 1)).map
        (fun v => bag.mass (k * (m + 1), m, s) v * (decide (lo ≤ v ∧ v < hi)).toNat) =
      (List.range (m + 1)).map (fun v => k * (decide (lo ≤ v ∧ v < hi)).toNat) := by
    apply List.map_congr_left
    intro v hv
    rw [hmass v hv]
  rw [hmap, sum_map_mul_left_nat]
  have hcover0 : ((List.range (m + 1)).map
      (fun v => (decide (lo ≤ v ∧ v < lo + (hi - lo))).toNat)).sum = hi - lo := by
    apply sum_cover_indicator
    omega
  have hcover : ((List.range (m + 1)).map
      (fun v => (decide (lo ≤ v ∧ v < hi)).toNat)).sum = hi - lo := by
    rw [← hcover0]
    apply congrArg List.sum
    apply List.map_congr_left
    intro v hv
    have hp : (lo ≤ v ∧ v < hi) = (lo ≤ v ∧ v < lo + (hi - lo)) := by
      apply propext
      constructor <;> intro h <;> constructor <;> omega
    simp [hp]
  rw [hcover]

end TinylambOpt.ResearchProofs.StorageExecution.Goal_BagKernels_RotationCycle_RangePerCycle
