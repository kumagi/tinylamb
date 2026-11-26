import TinylambOpt.ResearchGoals.StorageExecution.BagKernels

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_BagKernels_IntervalCover_FilteredMoment

open TinylambOpt.ResearchGoals.Compression.Wire
open TinylambOpt.ResearchGoals.StorageExecution.BagKernels
open TinylambOpt.ResearchGoals.StorageExecution.BagKernels.IntervalCover

private theorem eraseDups_eq_self_of_nodup {α : Type} [BEq α] [LawfulBEq α]
    {l : List α} (h : l.Nodup) : l.eraseDups = l := by
  induction l with
  | nil => simp
  | cons a as ih =>
      rw [List.nodup_cons] at h
      rw [List.eraseDups_cons]
      have hf : (as.filter fun b => !b == a) = as := by
        apply List.filter_eq_self.mpr
        intro b hb
        have hba : b ≠ a := by
          intro heq
          exact h.1 (heq ▸ hb)
        simp [hba]
      rw [hf, ih h.2]

private theorem sum_map_add {α : Type} (f g : α → Nat) (l : List α) :
    (l.map (fun x => f x + g x)).sum = (l.map f).sum + (l.map g).sum := by
  induction l with
  | nil => simp
  | cons a l ih => simp [ih, Nat.add_assoc, Nat.add_left_comm]

private theorem sum_bool_mul_filter {α : Type} (p : α → Bool) (f : α → Nat) (l : List α) :
    (l.map (fun x => (p x).toNat * f x)).sum = ((l.filter p).map f).sum := by
  induction l with
  | nil => simp
  | cons a l ih =>
      by_cases h : p a = true
      · simp [h, ih]
      · have hf : p a = false := by cases hpa : p a <;> simp_all
        simp [hf, ih]

private theorem sum_map_ite_filter {α : Type} (p : α → Bool) (f : α → Nat) (l : List α) :
    (l.map (fun x => if p x then f x else 0)).sum = ((l.filter p).map f).sum := by
  induction l with
  | nil => simp
  | cons a l ih => by_cases h : p a <;> simp [h, ih]

private theorem filter_range_add_self (s n : Nat) :
    (List.range (s+n)).filter (fun k => s ≤ k && k < s+n) = (List.range n).map (fun i => s+i) := by
  rw [List.range_add]
  rw [List.filter_append]
  have h1 : (List.range s).filter (fun k => s ≤ k && k < s+n) = [] := by
    apply List.filter_eq_nil_iff.mpr
    intro k hk
    have hks : k < s := List.mem_range.mp hk
    simp [Nat.not_le.mpr hks]
  have h2 : ((List.range n).map (fun i => s+i)).filter (fun k => s ≤ k && k < s+n) =
      (List.range n).map (fun i => s+i) := by
    apply List.filter_eq_self.mpr
    intro k hk
    rcases List.mem_map.mp hk with ⟨i, hi, rfl⟩
    have hin : i < n := List.mem_range.mp hi
    simp [hin]
  rw [h1, h2]
  simp

private theorem sum_filter_range_interval (s n m : Nat) (f : Nat → Nat) (hm : s+n ≤ m) :
    (((List.range m).filter (fun k => s ≤ k && k < s+n)).map f).sum =
      (((List.range n).map (fun i => s+i)).map f).sum := by
  let t := m - (s+n)
  have hm' : s+n+t = m := by omega
  rw [← hm']
  rw [List.range_add]
  rw [List.filter_append]
  have htail : ((List.range t).map (fun i => s+n+i)).filter (fun k => s ≤ k && k < s+n) = [] := by
    apply List.filter_eq_nil_iff.mpr
    intro k hk
    rcases List.mem_map.mp hk with ⟨i, hi, rfl⟩
    simp
  rw [htail]
  simp [filter_range_add_self]

private theorem le_peak_of_mem {xs : List Nat} {a : Nat} (h : a ∈ xs) : a ≤ peak xs := by
  unfold peak
  cases hm : xs.max? with
  | none =>
      induction xs with
      | nil => simp at h
      | cons b bs ih => simp [List.max?_cons] at hm
  | some v =>
      have hv := (List.max?_eq_some_iff.mp hm).2 a h
      simpa [hm] using hv

private theorem peak_cons_ge (a : Nat) (xs : List Nat) : peak xs ≤ peak (a::xs) := by
  unfold peak
  cases hm : xs.max? with
  | none => simp [List.max?_cons, hm]
  | some v => simp [List.max?_cons, hm, Nat.le_max_right]

private theorem mass_eq_zero_of_ge_peak (rs : List (Nat × Nat)) {k : Nat}
    (hk : peak (rs.map (fun r => r.1+r.2)) ≤ k) : bag.mass rs k = 0 := by
  unfold bag
  rw [List.countP_eq_zero]
  intro r hr
  have hrle : r.1 + r.2 ≤ peak (rs.map (fun x => x.1+x.2)) :=
    le_peak_of_mem (List.mem_map.mpr ⟨r, hr, rfl⟩)
  have hnot : ¬ (r.1 ≤ k ∧ k < r.1 + r.2) := by omega
  simpa using hnot

private theorem sum_eq_zero_of_all_zero (l : List Nat) (h : ∀ x ∈ l, x = 0) : l.sum = 0 := by
  induction l with
  | nil => simp
  | cons a l ih =>
      have ha := h a (by simp)
      have hr : ∀ x ∈ l, x = 0 := by intro x hx; exact h x (by simp [hx])
      simp [ha, ih hr]

private theorem sum_mass_range_tail (rs : List (Nat × Nat)) (f : Nat → Nat)
    (M M' : Nat) (hM : M = peak (rs.map (fun r => r.1+r.2)))
    (hMle : M ≤ M') :
    ((List.range M').map (fun k => bag.mass rs k * f k)).sum =
      ((List.range M).map (fun k => bag.mass rs k * f k)).sum := by
  let t := M' - M
  have ht : M + t = M' := by omega
  rw [← ht, List.range_add, List.map_append, List.sum_append]
  have hzero : (((List.range t).map (fun x => M+x)).map (fun k => bag.mass rs k * f k)).sum = 0 := by
    simp only [List.map_map]
    apply sum_eq_zero_of_all_zero
    intro z hz
    rcases List.mem_map.mp hz with ⟨i, hi, rfl⟩
    change bag.mass rs (M+i) * f (M+i) = 0
    rw [mass_eq_zero_of_ge_peak rs]
    · simp
    · rw [← hM]
      have hit : i < t := List.mem_range.mp hi
      omega
  rw [hzero, Nat.add_zero]

private theorem sum_indicator_range (s n m : Nat) (f : Nat → Nat) (hm : s+n ≤ m) :
    ((List.range m).map (fun k => (decide (s ≤ k ∧ k < s+n)).toNat * f k)).sum =
      (((List.range n).map (fun i => s+i)).map f).sum := by
  rw [sum_bool_mul_filter]
  exact sum_filter_range_interval s n m f hm

private theorem mass_cons (s n k : Nat) (rs : List (Nat × Nat)) :
    bag.mass ((s,n)::rs) k = (decide (s ≤ k ∧ k < s+n)).toNat + bag.mass rs k := by
  by_cases h : s ≤ k ∧ k < s+n
  · simp [bag, h, Nat.add_comm]
  · simp [bag, h, Nat.add_comm]

private theorem weighted_eq_sum_expand (f : Nat → Nat) :
    ∀ rs : List (Nat × Nat), ((bag.keys rs).map (fun k => bag.mass rs k * f k)).sum =
      ((bag.expand rs).map f).sum := by
  intro rs
  induction rs with
  | nil => simp [bag, peak]
  | cons r rs ih =>
      obtain ⟨s,n⟩ := r
      let M := peak (rs.map (fun x => x.1+x.2))
      let M' := peak (((s,n)::rs).map (fun x => x.1+x.2))
      have hMle : M ≤ M' := by
        dsimp [M, M']
        simpa using peak_cons_ge (s+n) (rs.map (fun x => x.1+x.2))
      have hnle : s+n ≤ M' := by
        dsimp [M']
        exact le_peak_of_mem (by simp)
      change ((List.range M').map (fun k => bag.mass ((s,n)::rs) k * f k)).sum =
        ((bag.expand ((s,n)::rs)).map f).sum
      have hpoint : (fun k => bag.mass ((s,n)::rs) k * f k) =
          (fun k => ((decide (s ≤ k ∧ k < s+n)).toNat + bag.mass rs k) * f k) := by
        funext k
        rw [mass_cons]
      rw [hpoint]
      rw [show (List.range M').map (fun k => ((decide (s ≤ k ∧ k < s+n)).toNat +
          bag.mass rs k) * f k) =
          (List.range M').map (fun k => (decide (s ≤ k ∧ k < s+n)).toNat * f k +
          bag.mass rs k * f k) by
            apply List.ext_getElem?; intro i; simp [Nat.add_mul]]
      rw [sum_map_add]
      rw [sum_indicator_range s n M' f hnle]
      rw [sum_mass_range_tail rs f M M' rfl hMle]
      rw [show bag.expand ((s,n)::rs) = (List.range n).map (fun i => s+i) ++ bag.expand rs by
        simp [bag]]
      simp only [List.map_append, List.sum_append]
      rw [← ih]
      rfl

theorem sound_FilteredMoment : FilteredMoment.Goal := by
  intro p lo hi
  unfold weighted
  rw [eraseDups_eq_self_of_nodup]
  · rw [weighted_eq_sum_expand]
    rw [sum_map_ite_filter]
  · simpa [bag] using
      (List.nodup_range : (List.range (peak (p.map (fun r => r.1+r.2)))).Nodup)

end TinylambOpt.ResearchProofs.StorageExecution.Goal_BagKernels_IntervalCover_FilteredMoment
