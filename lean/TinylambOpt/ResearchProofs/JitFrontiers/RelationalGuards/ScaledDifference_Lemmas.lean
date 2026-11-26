import TinylambOpt.ResearchProofs.JitFrontiers.RelationalGuards.Common

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitFrontiers.RelationalGuards.ScaledDifferenceLemmas

open TinylambOpt.ResearchGoals.JitFrontiers.RelationalGuards
open TinylambOpt.ResearchProofs.JitFrontiers.RelationalGuards.Common
open TinylambOpt.ResearchProofs.JitFrontiers.RelationalGuards.CorrelatedSubLemmas

abbrev sd := TinylambOpt.ResearchGoals.JitFrontiers.RelationalGuards.ScaledDifference.expr

theorem range_sd {t : Int} {z : Zone} {p : Point} (hp : inZone z p = true) :
    min (t * max z.dl (z.xl-z.yh)) (t * min z.dh (z.xh-z.yl)) ≤ sd.score t p ∧
    sd.score t p ≤ max (t * max z.dl (z.xl-z.yh)) (t * min z.dh (z.xh-z.yl)) := by
  rcases score_range hp with ⟨hlo, hhi⟩
  have hd : max z.dl (z.xl-z.yh) ≤ min z.dh (z.xh-z.yl) := by omega
  by_cases ht : 0 ≤ t
  · have h1 := Int.mul_le_mul_of_nonneg_left hlo ht
    have h2 := Int.mul_le_mul_of_nonneg_left hhi ht
    have h12 : t * max z.dl (z.xl-z.yh) ≤ t * min z.dh (z.xh-z.yl) :=
      Int.mul_le_mul_of_nonneg_left hd ht
    rw [Int.min_eq_left h12, Int.max_eq_right h12]
    exact ⟨h1, h2⟩
  · have ht0 : t ≤ 0 := by omega
    have h1 : t * min z.dh (z.xh-z.yl) ≤ t * (p.1-p.2) := Int.mul_le_mul_of_nonpos_left ht0 hhi
    have h2 : t * (p.1-p.2) ≤ t * max z.dl (z.xl-z.yh) := Int.mul_le_mul_of_nonpos_left ht0 hlo
    have h12 : t * min z.dh (z.xh-z.yl) ≤ t * max z.dl (z.xl-z.yh) :=
      Int.mul_le_mul_of_nonpos_left ht0 hd
    rw [Int.min_eq_right h12, Int.max_eq_left h12]
    exact ⟨h1, h2⟩

theorem estimate_sd_nonempty {t : Int} {z : Zone} (hne : ∃ p, inZone z p = true) :
    estimate sd t z = some (min (t * max z.dl (z.xl-z.yh)) (t * min z.dh (z.xh-z.yl)),
                            max (t * max z.dl (z.xl-z.yh)) (t * min z.dh (z.xh-z.yl))) := by
  rcases minDiff_vertex hne with ⟨vmin, hvmin, _, hsmin⟩
  rcases maxDiff_vertex hne with ⟨vmax, hvmax, _, hsmax⟩
  apply bounds_eq_minmax
  · by_cases ht : 0 ≤ t
    · refine List.mem_map.mpr ⟨vmin, hvmin, ?_⟩
      have hlo : t * max z.dl (z.xl-z.yh) ≤ t * min z.dh (z.xh-z.yl) := by
        apply Int.mul_le_mul_of_nonneg_left (nonempty_bounds hne) ht
      rw [Int.min_eq_left hlo]
      change t * (vmin.1 - vmin.2) = _
      rw [hsmin]
    · have ht0 : t ≤ 0 := by omega
      refine List.mem_map.mpr ⟨vmax, hvmax, ?_⟩
      have hlo : t * min z.dh (z.xh-z.yl) ≤ t * max z.dl (z.xl-z.yh) := by
        apply Int.mul_le_mul_of_nonpos_left ht0 (nonempty_bounds hne)
      rw [Int.min_eq_right hlo]
      change t * (vmax.1 - vmax.2) = _
      rw [hsmax]
  · intro e he
    rcases List.mem_map.mp he with ⟨v, hv, rfl⟩
    exact (range_sd (vertex_inZone hv)).1
  · by_cases ht : 0 ≤ t
    · refine List.mem_map.mpr ⟨vmax, hvmax, ?_⟩
      have hlo : t * max z.dl (z.xl-z.yh) ≤ t * min z.dh (z.xh-z.yl) := by
        apply Int.mul_le_mul_of_nonneg_left (nonempty_bounds hne) ht
      rw [Int.max_eq_right hlo]
      change t * (vmax.1 - vmax.2) = _
      rw [hsmax]
    · have ht0 : t ≤ 0 := by omega
      refine List.mem_map.mpr ⟨vmin, hvmin, ?_⟩
      have hlo : t * min z.dh (z.xh-z.yl) ≤ t * max z.dl (z.xl-z.yh) := by
        apply Int.mul_le_mul_of_nonpos_left ht0 (nonempty_bounds hne)
      rw [Int.max_eq_left hlo]
      change t * (vmin.1 - vmin.2) = _
      rw [hsmin]
  · intro e he
    rcases List.mem_map.mp he with ⟨v, hv, rfl⟩
    exact (range_sd (vertex_inZone hv)).2

theorem extrema_sd_nonempty {t : Int} {z : Zone} (hne : ∃ p, inZone z p = true) :
    extrema sd t z = some (min (t * max z.dl (z.xl-z.yh)) (t * min z.dh (z.xh-z.yl)),
                            max (t * max z.dl (z.xl-z.yh)) (t * min z.dh (z.xh-z.yl))) := by
  apply bounds_eq_minmax
  · by_cases ht : 0 ≤ t
    · rcases minDiff_vertex hne with ⟨v, hv, hvz, hvs⟩
      refine List.mem_map.mpr ⟨v, point_mem_points hvz, ?_⟩
      have hlo : t * max z.dl (z.xl-z.yh) ≤ t * min z.dh (z.xh-z.yl) := by
        apply Int.mul_le_mul_of_nonneg_left (nonempty_bounds hne) ht
      rw [Int.min_eq_left hlo]
      change t * (v.1 - v.2) = _
      rw [hvs]
    · have ht0 : t ≤ 0 := by omega
      rcases maxDiff_vertex hne with ⟨v, hv, hvz, hvs⟩
      refine List.mem_map.mpr ⟨v, point_mem_points hvz, ?_⟩
      have hlo : t * min z.dh (z.xh-z.yl) ≤ t * max z.dl (z.xl-z.yh) := by
        apply Int.mul_le_mul_of_nonpos_left ht0 (nonempty_bounds hne)
      rw [Int.min_eq_right hlo]
      change t * (v.1 - v.2) = _
      rw [hvs]
  · intro e he
    rcases List.mem_map.mp he with ⟨p, hp, rfl⟩
    exact (range_sd (List.mem_filter.mp hp).2).1
  · by_cases ht : 0 ≤ t
    · rcases maxDiff_vertex hne with ⟨v, hv, hvz, hvs⟩
      refine List.mem_map.mpr ⟨v, point_mem_points hvz, ?_⟩
      have hlo : t * max z.dl (z.xl-z.yh) ≤ t * min z.dh (z.xh-z.yl) := by
        apply Int.mul_le_mul_of_nonneg_left (nonempty_bounds hne) ht
      rw [Int.max_eq_right hlo]
      change t * (v.1 - v.2) = _
      rw [hvs]
    · have ht0 : t ≤ 0 := by omega
      rcases minDiff_vertex hne with ⟨v, hv, hvz, hvs⟩
      refine List.mem_map.mpr ⟨v, point_mem_points hvz, ?_⟩
      have hlo : t * min z.dh (z.xh-z.yl) ≤ t * max z.dl (z.xl-z.yh) := by
        apply Int.mul_le_mul_of_nonpos_left ht0 (nonempty_bounds hne)
      rw [Int.max_eq_left hlo]
      change t * (v.1 - v.2) = _
      rw [hvs]
  · intro e he
    rcases List.mem_map.mp he with ⟨p, hp, rfl⟩
    exact (range_sd (List.mem_filter.mp hp).2).2

theorem minimalHull_sd : minimalHull sd := by
  intro t z
  classical
  by_cases hne : ∃ p, inZone z p = true
  · rw [estimate_sd_nonempty hne, extrema_sd_nonempty hne]
  · have he : estimate sd t z = none := (deadEdge_general sd t z).2 (by
      intro p; by_cases h : inZone z p = true
      · exact False.elim (hne ⟨p, h⟩)
      · simpa using h)
    have hpnil : points z = [] := by
      apply List.eq_nil_iff_forall_not_mem.mpr
      intro p hp; exact hne ⟨p, (List.mem_filter.mp hp).2⟩
    change bounds (List.map (sd.score t) (vertices z (sd.cuts t))) =
      bounds (List.map (sd.score t) (points z))
    rw [show bounds (List.map (sd.score t) (vertices z (sd.cuts t))) = none by simpa [estimate] using he]
    simp [hpnil, bounds]

theorem coverage_sd : ∀ t z p, inZone z p = true → ∀ lo hi, estimate sd t z = some (lo,hi) →
    lo ≤ sd.score t p ∧ sd.score t p ≤ hi := by
  intro t z p hp lo hi he
  exact covers_of_estimate_eq hp he (minimalHull_sd t z)

theorem negativeScale_sd : ∀ z t l h, t < 0 → estimate CorrelatedSub.expr 0 z = some (l,h) →
    estimate sd t z = some (t*h, t*l) := by
  intro z t l h ht he
  classical
  by_cases hne : ∃ p, inZone z p = true
  · rw [estimate_sd_nonempty hne]
    have heq : l = max z.dl (z.xl-z.yh) := by
      have he' := estimate_eq_nonempty hne
      rw [he'] at he
      cases he
      rfl
    have heq2 : h = min z.dh (z.xh-z.yl) := by
      have he' := estimate_eq_nonempty hne
      rw [he'] at he
      cases he
      rfl
    have ht0 : t ≤ 0 := by omega
    have hlo : t * min z.dh (z.xh-z.yl) ≤ t * max z.dl (z.xl-z.yh) := by
      apply Int.mul_le_mul_of_nonpos_left ht0 (nonempty_bounds hne)
    rw [Int.min_eq_right hlo, Int.max_eq_left hlo, ← heq, ← heq2]
  · rw [estimate_eq_empty hne] at he
    contradiction

end TinylambOpt.ResearchProofs.JitFrontiers.RelationalGuards.ScaledDifferenceLemmas
