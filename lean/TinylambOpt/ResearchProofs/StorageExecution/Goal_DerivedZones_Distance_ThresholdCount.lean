import TinylambOpt.ResearchGoals.StorageExecution.DerivedZones

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_DerivedZones_Distance_ThresholdCount

open TinylambOpt.ResearchGoals.StorageExecution.DerivedZones
open TinylambOpt.ResearchGoals.StorageExecution.DerivedZones.Distance

abbrev Page := List (Option Row) × Bounds × Bounds

-- gapMin is a lower bound on the gap of any covered pair.
private theorem gap_min_le {a b : Bounds} {x y : Nat}
    (hx : a.lo ≤ x ∧ x ≤ a.hi) (hy : b.lo ≤ y ∧ y ≤ b.hi) :
    gapMin a b ≤ kernel.score 0 (x, y) := by
  dsimp [gapMin, kernel, gap]
  rw [Nat.max_le]
  constructor
  · exact Nat.le_trans (Nat.sub_le_sub_left hy.2 a.lo)
      (Nat.le_trans (Nat.sub_le_sub_right hx.1 y)
        (Nat.le_add_right (x - y) (y - x)))
  · exact Nat.le_trans (Nat.sub_le_sub_left hx.2 b.lo)
      (Nat.le_trans (Nat.sub_le_sub_right hy.1 x)
        (Nat.le_add_left (y - x) (x - y)))

-- gapMax is an upper bound on the gap of any covered pair.
private theorem gap_le_max {a b : Bounds} {x y : Nat}
    (hx : a.lo ≤ x ∧ x ≤ a.hi) (hy : b.lo ≤ y ∧ y ≤ b.hi) :
    kernel.score 0 (x, y) ≤ gapMax a b := by
  dsimp [gapMax, kernel, gap]
  by_cases hxy : y ≤ x
  · have h0 : y - x = 0 := Nat.sub_eq_zero_of_le hxy
    rw [h0, Nat.add_zero]
    exact Nat.le_trans
      (Nat.le_trans (Nat.sub_le_sub_right hx.2 y) (Nat.sub_le_sub_left hy.1 a.hi))
      (Nat.le_max_left _ _)
  · have hyx : x ≤ y := Nat.le_of_lt (Nat.lt_of_not_ge hxy)
    have h0 : x - y = 0 := Nat.sub_eq_zero_of_le hyx
    rw [h0, Nat.zero_add]
    exact Nat.le_trans
      (Nat.le_trans (Nat.sub_le_sub_right hy.2 x) (Nat.sub_le_sub_left hx.1 b.hi))
      (Nat.le_max_right _ _)

private theorem scores_length (t : Nat) (p : List (Option Row)) :
    (scores kernel t p).length = (p.filterMap id).length := by
  induction p with
  | nil => simp [scores]
  | cons r p ih =>
      rcases r with _ | ⟨x, y⟩
      · change (scores kernel t p).length = (p.filterMap id).length
        exact ih
      · change (scores kernel t p).length + 1 = (p.filterMap id).length + 1
        omega

/-- Every passing score comes from a non-NULL row. -/
private theorem cnt_le_n (t cut : Nat) (p : List (Option Row)) :
    (scores kernel t p).countP (fun v => cut ≤ v) ≤ (p.filterMap id).length := by
  rw [← scores_length t p]
  exact List.countP_le_length

/-- If the certified lower bound reaches the cut, every score in the page passes. -/
private theorem score_ge_of_lo (t cut : Nat) {p : List (Option Row)} {a b : Bounds}
    (hcov : covers p a b) (hlo : cut ≤ (kernel.bounds t a b).lo) :
    ∀ v ∈ scores kernel t p, cut ≤ v := by
  induction p with
  | nil => simp [scores]
  | cons r p ih =>
      have hcov' : covers p a b := by
        intro r hr x y hxy
        exact hcov r (by simp [hr]) x y hxy
      have ih' := ih hcov'
      intro v hv
      rcases r with _ | ⟨x, y⟩
      · simpa [scores] using ih' v hv
      · have hx : (a.lo ≤ x ∧ x ≤ a.hi) ∧ (b.lo ≤ y ∧ y ≤ b.hi) :=
          hcov (some (x, y)) (by simp) x y rfl
        have hxy : cut ≤ kernel.score t (x, y) := by
          dsimp [kernel] at hlo ⊢
          exact Nat.le_trans hlo (gap_min_le hx.1 hx.2)
        simp only [scores, List.filterMap_cons, List.mem_cons, Option.map_some] at hv
        rcases hv with hveq | hv
        · simpa [hveq] using hxy
        · exact ih' v hv

private theorem cnt_eq_n_of_lo (t cut : Nat) {p : List (Option Row)} {a b : Bounds}
    (hcov : covers p a b) (hlo : cut ≤ (kernel.bounds t a b).lo) :
    (scores kernel t p).countP (fun v => cut ≤ v) = (p.filterMap id).length := by
  rw [List.countP_eq_length.mpr]
  · exact scores_length t p
  · intro v hv
    simp [score_ge_of_lo t cut hcov hlo v hv]

/-- If the certified upper bound is below the cut, no score in the page passes. -/
private theorem score_lt_of_hi (t cut : Nat) {p : List (Option Row)} {a b : Bounds}
    (hcov : covers p a b) (hhi : (kernel.bounds t a b).hi < cut) :
    ∀ v ∈ scores kernel t p, ¬ cut ≤ v := by
  induction p with
  | nil => simp [scores]
  | cons r p ih =>
      have hcov' : covers p a b := by
        intro r hr x y hxy
        exact hcov r (by simp [hr]) x y hxy
      have ih' := ih hcov'
      intro v hv
      rcases r with _ | ⟨x, y⟩
      · simpa [scores] using ih' v hv
      · have hx : (a.lo ≤ x ∧ x ≤ a.hi) ∧ (b.lo ≤ y ∧ y ≤ b.hi) :=
          hcov (some (x, y)) (by simp) x y rfl
        have hle : kernel.score t (x, y) ≤ (kernel.bounds t a b).hi := by
          dsimp [kernel] at hhi ⊢
          exact gap_le_max hx.1 hx.2
        have hxy : ¬ cut ≤ kernel.score t (x, y) := by
          exact Nat.not_le_of_gt (Nat.lt_of_le_of_lt hle hhi)
        simp only [scores, List.filterMap_cons, List.mem_cons, Option.map_some] at hv
        rcases hv with hveq | hv
        · simpa [hveq] using hxy
        · exact ih' v hv

private theorem cnt_eq_zero_of_hi (t cut : Nat) {p : List (Option Row)} {a b : Bounds}
    (hcov : covers p a b) (hhi : (kernel.bounds t a b).hi < cut) :
    (scores kernel t p).countP (fun v => cut ≤ v) = 0 := by
  rw [List.countP_eq_zero.mpr]
  intro v hv
  simp [score_lt_of_hi t cut hcov hhi v hv]

/-- The flattened actual count is the sum of the per-page counts. -/
private theorem actual_eq_sum (t cut : Nat) (ps : List Page) :
    ((ps.map Prod.fst).flatten.filterMap (fun r => r.map (kernel.score t))).countP (fun v => cut ≤ v) =
      (ps.map (fun e => (scores kernel t e.1).countP (fun v => cut ≤ v))).sum := by
  induction ps with
  | nil => simp [scores]
  | cons e ps ih =>
      simp only [List.map_cons, List.flatten_cons, List.filterMap_append, List.countP_append, ih]
      rfl

theorem sound_ThresholdCount : Distance.ThresholdCount.Goal := by
  unfold Distance.ThresholdCount.Goal thresholdCount
  intro t ps cut hcov
  dsimp only
  rw [actual_eq_sum]
  induction ps with
  | nil => simp
  | cons e ps ih =>
      have hcov_e : covers e.1 e.2.1 e.2.2 := hcov e (by simp)
      have hcov_ps : ∀ x ∈ ps, covers x.1 x.2.1 x.2.2 := by
        intro x hx
        exact hcov x (by simp [hx])
      have ih' := ih hcov_ps
      constructor
      · by_cases hlo : cut ≤ (kernel.bounds t e.2.1 e.2.2).lo
        · have hcnt := cnt_eq_n_of_lo t cut hcov_e hlo
          simp [hlo, hcnt]
          omega
        · simp [hlo]
          omega
      · by_cases hhi : cut ≤ (kernel.bounds t e.2.1 e.2.2).hi
        · have hcnt := cnt_le_n t cut e.1
          simp [hhi]
          omega
        · have hcnt := cnt_eq_zero_of_hi t cut hcov_e (Nat.lt_of_not_ge hhi)
          simp [hhi, hcnt]
          omega

end TinylambOpt.ResearchProofs.StorageExecution.Goal_DerivedZones_Distance_ThresholdCount
