import TinylambOpt.ResearchProofs.JitFrontiers.RelationalGuards.Common

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitFrontiers.RelationalGuards.PositiveExcessLemmas

open TinylambOpt.ResearchGoals.JitFrontiers.RelationalGuards
open TinylambOpt.ResearchProofs.JitFrontiers.RelationalGuards.Common
open TinylambOpt.ResearchProofs.JitFrontiers.RelationalGuards.CorrelatedSubLemmas

abbrev pe := TinylambOpt.ResearchGoals.JitFrontiers.RelationalGuards.PositiveExcess.expr

theorem range_pe {t : Int} {z : Zone} {p : Point} (hp : inZone z p = true) :
    max 0 (max z.dl (z.xl-z.yh) - t) ≤ pe.score t p ∧
    pe.score t p ≤ max 0 (min z.dh (z.xh-z.yl) - t) := by
  rcases score_range hp with ⟨hlo, hhi⟩
  change max 0 (max z.dl (z.xl-z.yh) - t) ≤ max 0 (p.1-p.2-t) ∧
    max 0 (p.1-p.2-t) ≤ max 0 (min z.dh (z.xh-z.yl) - t)
  constructor
  · rw [Int.max_le]
    constructor
    · exact Int.le_max_left _ _
    · exact Int.le_trans (by omega : max z.dl (z.xl-z.yh) - t ≤ p.1-p.2-t) (Int.le_max_right _ _)
  · rw [Int.max_le]
    constructor
    · exact Int.le_max_left _ _
    · exact Int.le_trans (by omega : p.1-p.2-t ≤ min z.dh (z.xh-z.yl)-t) (Int.le_max_right _ _)

theorem lo_vertex {t : Int} {z : Zone} (hne : ∃ p, inZone z p = true) :
    ∃ v, v ∈ vertices z [t] ∧ pe.score t v = max 0 (max z.dl (z.xl-z.yh) - t) := by
  rcases minDiff_vertex hne with ⟨vmin, hvmin, _, hsmin⟩
  rcases maxDiff_vertex hne with ⟨vmax, hvmax, _, hsmax⟩
  rcases hne with ⟨p, hp⟩
  rcases score_range hp with ⟨hDlo, hDhi⟩
  have hxlxh : z.xl ≤ z.xh := by
    rcases inZone_iff.mp hp with ⟨h1, h2, _⟩
    omega
  have hylyh : z.yl ≤ z.yh := by
    rcases inZone_iff.mp hp with ⟨_, _, h1, h2, _⟩
    omega
  by_cases hcase : t ≤ max z.dl (z.xl-z.yh)
  · refine ⟨vmin, vertices_mono_ds (z := z) (ds := []) (ds' := [t]) (by simp) hvmin, ?_⟩
    change max 0 (vmin.1 - vmin.2 - t) = _
    rw [hsmin]
  · by_cases hhi : t ≤ min z.dh (z.xh-z.yl)
    · rcases diff_vertex (z := z) (ds := [t]) (d := t) hxlxh hylyh
          ⟨by omega, by omega⟩ (by right; right; simp) with ⟨v, hv, hvz, hvs⟩
      refine ⟨v, hv, ?_⟩
      change max 0 (v.1 - v.2 - t) = max 0 (max z.dl (z.xl-z.yh) - t)
      rw [hvs]
      rw [Int.max_eq_left (by omega : max z.dl (z.xl-z.yh) - t ≤ 0)]
      simp
    · refine ⟨vmax, vertices_mono_ds (z := z) (ds := []) (ds' := [t]) (by simp) hvmax, ?_⟩
      change max 0 (vmax.1 - vmax.2 - t) = max 0 (max z.dl (z.xl-z.yh) - t)
      rw [hsmax]
      rw [Int.max_eq_left (by omega : min z.dh (z.xh-z.yl) - t ≤ 0)]
      rw [Int.max_eq_left (by omega : max z.dl (z.xl-z.yh) - t ≤ 0)]

theorem estimate_pe_nonempty {t : Int} {z : Zone} (hne : ∃ p, inZone z p = true) :
    estimate pe t z = some (max 0 (max z.dl (z.xl-z.yh) - t),
                            max 0 (min z.dh (z.xh-z.yl) - t)) := by
  rcases lo_vertex hne with ⟨vlo, hvlo, hslo⟩
  rcases maxDiff_vertex hne with ⟨vhi, hvhi0, _, hshi⟩
  have hvhi : vhi ∈ vertices z [t] := vertices_mono_ds (z := z) (ds := []) (ds' := [t]) (by simp) hvhi0
  apply bounds_eq_minmax
  · exact List.mem_map.mpr ⟨vlo, hvlo, hslo⟩
  · intro e he
    rcases List.mem_map.mp he with ⟨v, hv, rfl⟩
    exact (range_pe (vertex_inZone hv)).1
  · refine List.mem_map.mpr ⟨vhi, hvhi, ?_⟩
    change max 0 (vhi.1 - vhi.2 - t) = max 0 (min z.dh (z.xh-z.yl) - t)
    rw [hshi]
  · intro e he
    rcases List.mem_map.mp he with ⟨v, hv, rfl⟩
    exact (range_pe (vertex_inZone hv)).2

theorem extrema_pe_nonempty {t : Int} {z : Zone} (hne : ∃ p, inZone z p = true) :
    extrema pe t z = some (max 0 (max z.dl (z.xl-z.yh) - t),
                           max 0 (min z.dh (z.xh-z.yl) - t)) := by
  apply bounds_eq_minmax
  · rcases lo_vertex hne with ⟨vlo, hvlo, hslo⟩
    exact List.mem_map.mpr ⟨vlo, point_mem_points (vertex_inZone hvlo), hslo⟩
  · intro e he
    rcases List.mem_map.mp he with ⟨p, hp, rfl⟩
    exact (range_pe (List.mem_filter.mp hp).2).1
  · rcases maxDiff_vertex hne with ⟨vhi, hvhi, hvz, hshi⟩
    refine List.mem_map.mpr ⟨vhi, point_mem_points hvz, ?_⟩
    change max 0 (vhi.1 - vhi.2 - t) = max 0 (min z.dh (z.xh-z.yl) - t)
    rw [hshi]
  · intro e he
    rcases List.mem_map.mp he with ⟨p, hp, rfl⟩
    exact (range_pe (List.mem_filter.mp hp).2).2

theorem minimalHull_pe : minimalHull pe := by
  intro t z
  classical
  by_cases hne : ∃ p, inZone z p = true
  · rw [estimate_pe_nonempty hne, extrema_pe_nonempty hne]
  · have he : estimate pe t z = none := (deadEdge_general pe t z).2 (by
      intro p; by_cases h : inZone z p = true
      · exact False.elim (hne ⟨p, h⟩)
      · simpa using h)
    have hpnil : points z = [] := by
      apply List.eq_nil_iff_forall_not_mem.mpr
      intro p hp; exact hne ⟨p, (List.mem_filter.mp hp).2⟩
    change bounds (List.map (pe.score t) (vertices z (pe.cuts t))) =
      bounds (List.map (pe.score t) (points z))
    rw [show bounds (List.map (pe.score t) (vertices z (pe.cuts t))) = none by simpa [estimate] using he]
    simp [hpnil, bounds]

theorem coverage_pe : ∀ t z p, inZone z p = true → ∀ lo hi, estimate pe t z = some (lo,hi) →
    lo ≤ pe.score t p ∧ pe.score t p ≤ hi := by
  intro t z p hp lo hi he
  exact covers_of_estimate_eq hp he (minimalHull_pe t z)

theorem inactive_pe : ∀ z t l h, z.dh ≤ t → estimate pe t z = some (l,h) → l = 0 ∧ h = 0 := by
  intro z t l h hdh he
  classical
  by_cases hne : ∃ p, inZone z p = true
  · rw [estimate_pe_nonempty hne] at he
    cases he
    rcases hne with ⟨p, hp⟩
    rcases score_range hp with ⟨hDlo, hDhi⟩
    constructor
    · rw [Int.max_eq_left]
      omega
    · rw [Int.max_eq_left]
      omega
  · rw [(deadEdge_general pe t z).2 (by
        intro p; by_cases hp : inZone z p = true
        · exact False.elim (hne ⟨p, hp⟩)
        · simpa using hp)] at he
    contradiction

end TinylambOpt.ResearchProofs.JitFrontiers.RelationalGuards.PositiveExcessLemmas
