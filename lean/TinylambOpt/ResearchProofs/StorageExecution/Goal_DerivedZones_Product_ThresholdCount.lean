import TinylambOpt.ResearchGoals.StorageExecution.DerivedZones

/-!
Scan pruning can exploit derived bounds only when every matching row remains covered by
the block summary.

Proved

- `Product.ThresholdCount` — Estimate the number of passing rows using only certified
  and possible pages.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_DerivedZones_Product_ThresholdCount

open TinylambOpt.ResearchGoals.StorageExecution.DerivedZones
open TinylambOpt.ResearchGoals.StorageExecution.DerivedZones.Product

abbrev Page := List (Option Row) × Bounds × Bounds

/-- Every passing score comes from a non-NULL row. -/
private theorem cnt_le_n (t cut : Nat) (p : List (Option Row)) :
    (scores kernel t p).countP (fun v => cut ≤ v) ≤ (p.filterMap id).length := by
  induction p with
  | nil => simp [scores]
  | cons r p ih =>
      rcases r with _ | ⟨x, y⟩
      · simpa [scores] using ih
      · by_cases hxy : cut ≤ x * y
        · simp [scores, kernel, hxy] at ih ⊢
          omega
        · simp [scores, kernel, hxy] at ih ⊢
          omega

/-- If the certified lower bound reaches the cut, every non-NULL row passes. -/
private theorem cnt_eq_n_of_lo (t cut : Nat) {p : List (Option Row)} {a b : Bounds}
    (hcov : covers p a b) (hlo : cut ≤ (kernel.bounds t a b).lo) :
    (scores kernel t p).countP (fun v => cut ≤ v) = (p.filterMap id).length := by
  induction p with
  | nil => simp [scores]
  | cons r p ih =>
      have hcov' : covers p a b := by
        intro r hr x y hxy
        exact hcov r (by simp [hr]) x y hxy
      have ih' := ih hcov'
      rcases r with _ | ⟨x, y⟩
      · simpa [scores] using ih'
      · have hx : (a.lo ≤ x ∧ x ≤ a.hi) ∧ (b.lo ≤ y ∧ y ≤ b.hi) :=
          hcov (some (x, y)) (by simp) x y rfl
        have hxy : cut ≤ x * y := by
          dsimp [kernel] at hlo ⊢
          exact Nat.le_trans hlo (Nat.mul_le_mul hx.1.1 hx.2.1)
        simp [scores, kernel, hxy] at ih' ⊢
        omega

/-- If the certified upper bound is below the cut, no row passes. -/
private theorem cnt_eq_zero_of_hi (t cut : Nat) {p : List (Option Row)} {a b : Bounds}
    (hcov : covers p a b) (hhi : (kernel.bounds t a b).hi < cut) :
    (scores kernel t p).countP (fun v => cut ≤ v) = 0 := by
  induction p with
  | nil => simp [scores]
  | cons r p ih =>
      have hcov' : covers p a b := by
        intro r hr x y hxy
        exact hcov r (by simp [hr]) x y hxy
      have ih' := ih hcov'
      rcases r with _ | ⟨x, y⟩
      · simpa [scores] using ih'
      · have hx : (a.lo ≤ x ∧ x ≤ a.hi) ∧ (b.lo ≤ y ∧ y ≤ b.hi) :=
          hcov (some (x, y)) (by simp) x y rfl
        have hxy : ¬ cut ≤ x * y := by
          dsimp [kernel] at hhi ⊢
          exact Nat.not_le_of_gt (Nat.le_trans (Nat.succ_le_succ (Nat.mul_le_mul hx.1.2 hx.2.2)) hhi)
        simp [scores, kernel, hxy] at ih' ⊢
        omega

/-- Pages certified entirely above the cut contribute at least their row count. -/
private theorem sum_lo_le_sum_cnt (t cut : Nat) (ps : List Page)
    (hcov : ∀ e ∈ ps, covers e.1 e.2.1 e.2.2) :
    ((ps.filter (fun e => cut ≤ e.2.1.lo * e.2.2.lo)).map (fun e => (e.1.filterMap id).length)).sum ≤
    (ps.map (fun e => (scores kernel t e.1).countP (fun v => cut ≤ v))).sum := by
  induction ps with
  | nil => simp
  | cons e ps ih =>
      have hcov_e : covers e.1 e.2.1 e.2.2 := hcov e (by simp)
      have hcov_ps : ∀ x ∈ ps, covers x.1 x.2.1 x.2.2 := by
        intro x hx
        exact hcov x (by simp [hx])
      have ih' := ih hcov_ps
      by_cases hlo : cut ≤ e.2.1.lo * e.2.2.lo
      · have hcnt := cnt_eq_n_of_lo t cut hcov_e (by simpa [kernel] using hlo)
        simp [hlo, hcnt] at ih' ⊢
        omega
      · simp [hlo] at ih' ⊢
        omega

/-- Every passing row lives in a page whose upper bound reaches the cut. -/
private theorem sum_cnt_le_sum_hi (t cut : Nat) (ps : List Page)
    (hcov : ∀ e ∈ ps, covers e.1 e.2.1 e.2.2) :
    (ps.map (fun e => (scores kernel t e.1).countP (fun v => cut ≤ v))).sum ≤
    ((ps.filter (fun e => cut ≤ e.2.1.hi * e.2.2.hi)).map (fun e => (e.1.filterMap id).length)).sum := by
  induction ps with
  | nil => simp
  | cons e ps ih =>
      have hcov_e : covers e.1 e.2.1 e.2.2 := hcov e (by simp)
      have hcov_ps : ∀ x ∈ ps, covers x.1 x.2.1 x.2.2 := by
        intro x hx
        exact hcov x (by simp [hx])
      have ih' := ih hcov_ps
      by_cases hhi : cut ≤ e.2.1.hi * e.2.2.hi
      · have hcnt := cnt_le_n t cut e.1
        simp [hhi] at ih' ⊢
        omega
      · have hcnt := cnt_eq_zero_of_hi t cut hcov_e (by
          simpa [kernel] using (Nat.lt_of_not_ge hhi))
        simp [hhi] at ih' ⊢
        omega

theorem sound_ThresholdCount : Product.ThresholdCount.Goal := by
  unfold Product.ThresholdCount.Goal thresholdCount
  intro t ps cut hcov
  have hactual :
      ((ps.map Prod.fst).flatten.filterMap (fun r => r.map (kernel.score t))).countP (fun v => cut ≤ v) =
        (ps.map (fun e => (scores kernel t e.1).countP (fun v => cut ≤ v))).sum := by
    simp [scores, kernel, List.filterMap_flatten, List.countP_flatten, Function.comp_def]
  rw [hactual]
  constructor
  · exact sum_lo_le_sum_cnt t cut ps hcov
  · exact sum_cnt_le_sum_hi t cut ps hcov

end TinylambOpt.ResearchProofs.StorageExecution.Goal_DerivedZones_Product_ThresholdCount
