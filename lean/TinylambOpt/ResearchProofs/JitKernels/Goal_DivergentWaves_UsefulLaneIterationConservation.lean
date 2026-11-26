import TinylambOpt.ResearchGoals.JitKernels.DivergentWaves

set_option maxHeartbeats 0
set_option maxRecDepth 10000

/-! The area under the active-mask population curve equals the scalar lane work: every lane contributes once per iteration it is active, so vectorized masked probes preserve the exact work count. -/

namespace TinylambOpt.ResearchProofs.JitKernels.Goal_DivergentWaves_UsefulLaneIterationConservation

open TinylambOpt.ResearchGoals.JitKernels.DivergentWaves

theorem max?_some_of_mem_nat : ∀ (l : List Nat) (a : Nat), a ∈ l → ∃ m, l.max? = some m := by
  intro l
  induction l with
  | nil => simp
  | cons x xs ih =>
      intro a ha
      cases xs with
      | nil => simp
      | cons y ys => simp [List.max?_cons]

theorem le_peak : ∀ (l : List Nat) (a : Nat), a ∈ l → a ≤ peak l := by
  intro l
  induction l with
  | nil => simp
  | cons x xs ih =>
      intro a ha
      simp only [List.mem_cons] at ha
      rcases ha with rfl | ha
      · cases xs with
        | nil => simp [peak]
        | cons y ys =>
            obtain ⟨m, hm⟩ := max?_some_of_mem_nat (y :: ys) y (by simp)
            simp [peak, List.max?_cons, hm]
            exact Nat.le_max_left a m
      · cases xs with
        | nil => simp at ha
        | cons y ys =>
            obtain ⟨m, hm⟩ := max?_some_of_mem_nat (y :: ys) y (by simp)
            have hle := ih a ha
            simp [peak, List.max?_cons, hm] at hle ⊢
            omega

theorem sum_indicator_lt : ∀ T n : Nat, ((List.range T).map (fun t => if t < n then 1 else 0)).sum = min n T := by
  intro T
  induction T with
  | zero => intro n; simp
  | succ T ih =>
      intro n
      rw [List.range_succ, List.map_append, List.sum_append]
      simp only [List.map_cons, List.map_nil, List.sum_cons, List.sum_nil, Nat.add_zero, ih]
      by_cases h : T < n
      · by_cases h' : n ≤ T + 1
        · have hn : n = T + 1 := by omega
          subst n
          simp
        · have hn : T + 1 < n := by omega
          have hle : T ≤ n := Nat.le_of_lt h
          simp [h, h', Nat.min_def, hle]
      · have hn : n ≤ T := by omega
        simp [h, Nat.min_def, hn]
        omega

theorem sum_range_add : ∀ (T : Nat) (f g : Nat → Nat),
    ((List.range T).map (fun t => f t + g t)).sum =
      ((List.range T).map f).sum + ((List.range T).map g).sum := by
  intro T
  induction T with
  | zero => intro f g; simp
  | succ T ih =>
      intro f g
      simp [List.range_succ, List.map_append, List.sum_append, ih]
      omega

theorem count_mask_cons (n t : Nat) (xs : List Nat) :
    (maskAt (n :: xs) t).count true = (maskAt xs t).count true + (if t < n then 1 else 0) := by
  simp [maskAt, List.count_cons]

theorem sum_map_zero : ∀ T : Nat, ((List.range T).map (fun _ : Nat => 0)).sum = 0 := by
  intro T
  induction T with
  | zero => simp
  | succ T ih => simp [List.range_succ, ih]

theorem f_eq_min_sum : ∀ (xs : List Nat) (T : Nat),
    ((List.range T).map (fun t => (maskAt xs t).count true)).sum =
      (xs.map (fun n => min n T)).sum := by
  intro xs
  induction xs with
  | nil =>
      intro T
      rw [show (List.map (fun t => (maskAt [] t).count true) (List.range T)) =
          (List.map (fun _ : Nat => 0) (List.range T)) by
        apply List.map_congr_left
        intro t ht
        simp [maskAt]]
      simp [sum_map_zero]
  | cons n xs ih =>
      intro T
      have hmap : (List.map (fun t => (maskAt (n :: xs) t).count true) (List.range T)) =
          (List.map (fun t => (maskAt xs t).count true + (if t < n then 1 else 0)) (List.range T)) := by
        apply List.map_congr_left
        intro t ht
        exact count_mask_cons n t xs
      rw [hmap, sum_range_add, ih, sum_indicator_lt]
      rw [List.map_cons, List.sum_cons]
      omega

theorem sum_min_eq_sum : ∀ (xs : List Nat) (T : Nat), (∀ n ∈ xs, n ≤ T) →
    (xs.map (fun n => min n T)).sum = xs.sum := by
  intro xs
  induction xs with
  | nil => simp
  | cons n xs ih =>
      intro T hbound
      have hn : min n T = n := Nat.min_eq_left (hbound n (by simp))
      have htail : ∀ m ∈ xs, m ≤ T := by intro m hm; exact hbound m (by simp [hm])
      simp only [List.map_cons, List.sum_cons]
      rw [hn, ih T htail]

theorem sound : TinylambOpt.ResearchGoals.JitKernels.DivergentWaves.UsefulLaneIterationConservation.Goal := by
  intro xs
  rw [f_eq_min_sum]
  apply sum_min_eq_sum
  intro n hn
  exact le_peak xs n hn

end TinylambOpt.ResearchProofs.JitKernels.Goal_DivergentWaves_UsefulLaneIterationConservation
