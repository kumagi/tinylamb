import TinylambOpt.ResearchProofs.JitFrontiers.RelationalGuards.Common

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitFrontiers.RelationalGuards.CorrelatedSubLemmas

open TinylambOpt.ResearchGoals.JitFrontiers.RelationalGuards
open TinylambOpt.ResearchProofs.JitFrontiers.RelationalGuards.Common

abbrev ex := TinylambOpt.ResearchGoals.JitFrontiers.RelationalGuards.CorrelatedSub.expr

theorem nonempty_bounds {z : Zone} (hne : ∃ p, inZone z p = true) :
    max z.dl (z.xl - z.yh) ≤ min z.dh (z.xh - z.yl) := by
  rcases hne with ⟨p, hp⟩
  rcases inZone_iff.mp hp with ⟨hxl,hxh,hyl,hyh,hdl,hdh⟩
  omega

theorem maxDiff_vertex {z : Zone} (hne : ∃ p, inZone z p = true) :
    ∃ v, v ∈ vertices z [] ∧ inZone z v = true ∧ v.1 - v.2 = min z.dh (z.xh - z.yl) := by
  rcases hne with ⟨p, hp⟩
  rcases inZone_iff.mp hp with ⟨hxl,hxh,hyl,hyh,hdl,hdh⟩
  by_cases hcase : z.dh ≤ z.xh - z.yl
  · have hmin : min z.dh (z.xh - z.yl) = z.dh := Int.min_eq_left hcase
    by_cases hylc : z.xl - z.dh ≤ z.yl
    · refine ⟨(z.yl + z.dh, z.yl), ?_, ?_, ?_⟩
      · apply mem_vertices_of_y (z := z) (ds := []) (y := z.yl) (d := z.dh) (Or.inl rfl) (Or.inr (Or.inl rfl))
        apply inZone_iff.mpr; dsimp; omega
      · apply inZone_iff.mpr; dsimp; omega
      · dsimp; omega
    · refine ⟨(z.xl, z.xl - z.dh), ?_, ?_, ?_⟩
      · apply mem_vertices_of_x (z := z) (ds := []) (x := z.xl) (d := z.dh) (Or.inl rfl) (Or.inr (Or.inl rfl))
        apply inZone_iff.mpr; dsimp; omega
      · apply inZone_iff.mpr; dsimp; omega
      · dsimp; omega
  · have hmin : min z.dh (z.xh - z.yl) = z.xh - z.yl := Int.min_eq_right (by omega : z.xh - z.yl ≤ z.dh)
    refine ⟨(z.xh, z.yl), ?_, ?_, ?_⟩
    · apply mem_vertices_corner (z := z) (ds := []) (x := z.xh) (y := z.yl) (Or.inr rfl) (Or.inl rfl)
      apply inZone_iff.mpr; dsimp; omega
    · apply inZone_iff.mpr; dsimp; omega
    · dsimp; omega

theorem minDiff_vertex {z : Zone} (hne : ∃ p, inZone z p = true) :
    ∃ v, v ∈ vertices z [] ∧ inZone z v = true ∧ v.1 - v.2 = max z.dl (z.xl - z.yh) := by
  rcases hne with ⟨p, hp⟩
  rcases inZone_iff.mp hp with ⟨hxl,hxh,hyl,hyh,hdl,hdh⟩
  by_cases hcase : z.xl - z.yh ≤ z.dl
  · have hmax : max z.dl (z.xl - z.yh) = z.dl := Int.max_eq_left hcase
    by_cases hyhc : z.xl - z.dl ≤ z.yh
    · by_cases hylc : z.xl - z.dl ≤ z.yl
      · refine ⟨(z.yl + z.dl, z.yl), ?_, ?_, ?_⟩
        · apply mem_vertices_of_y (z := z) (ds := []) (y := z.yl) (d := z.dl) (Or.inl rfl) (Or.inl rfl)
          apply inZone_iff.mpr; dsimp; omega
        · apply inZone_iff.mpr; dsimp; omega
        · dsimp; omega
      · refine ⟨(z.xl, z.xl - z.dl), ?_, ?_, ?_⟩
        · apply mem_vertices_of_x (z := z) (ds := []) (x := z.xl) (d := z.dl) (Or.inl rfl) (Or.inl rfl)
          apply inZone_iff.mpr; dsimp; omega
        · apply inZone_iff.mpr; dsimp; omega
        · dsimp; omega
    · refine ⟨(z.xl, z.yh), ?_, ?_, ?_⟩
      · apply mem_vertices_corner (z := z) (ds := []) (x := z.xl) (y := z.yh) (Or.inl rfl) (Or.inr rfl)
        apply inZone_iff.mpr; dsimp; omega
      · apply inZone_iff.mpr; dsimp; omega
      · dsimp; omega
  · have hmax : max z.dl (z.xl - z.yh) = z.xl - z.yh := Int.max_eq_right (by omega : z.dl ≤ z.xl - z.yh)
    refine ⟨(z.xl, z.yh), ?_, ?_, ?_⟩
    · apply mem_vertices_corner (z := z) (ds := []) (x := z.xl) (y := z.yh) (Or.inl rfl) (Or.inr rfl)
      apply inZone_iff.mpr; dsimp; omega
    · apply inZone_iff.mpr; dsimp; omega
    · dsimp; omega

theorem score_range {z : Zone} {p : Point} (hp : inZone z p = true) :
    max z.dl (z.xl - z.yh) ≤ p.1 - p.2 ∧ p.1 - p.2 ≤ min z.dh (z.xh - z.yl) := by
  rcases inZone_iff.mp hp with ⟨hxl,hxh,hyl,hyh,hdl,hdh⟩
  omega

theorem extrema_eq_nonempty {z : Zone} (hne : ∃ p, inZone z p = true) :
    extrema ex 0 z = some (max z.dl (z.xl-z.yh), min z.dh (z.xh-z.yl)) := by
  apply bounds_eq_minmax
  · rcases minDiff_vertex hne with ⟨v, hv, hvz, hvs⟩
    exact List.mem_map.mpr ⟨v, point_mem_points hvz, by simpa [ex, hvs]⟩
  · intro e he
    rcases List.mem_map.mp he with ⟨p, hp, rfl⟩
    exact (score_range (List.mem_filter.mp hp).2).1
  · rcases maxDiff_vertex hne with ⟨v, hv, hvz, hvs⟩
    exact List.mem_map.mpr ⟨v, point_mem_points hvz, by simpa [ex, hvs]⟩
  · intro e he
    rcases List.mem_map.mp he with ⟨p, hp, rfl⟩
    exact (score_range (List.mem_filter.mp hp).2).2

theorem estimate_eq_nonempty {z : Zone} (hne : ∃ p, inZone z p = true) :
    estimate ex 0 z = some (max z.dl (z.xl-z.yh), min z.dh (z.xh-z.yl)) := by
    rcases maxDiff_vertex hne with ⟨vmax, hvmax, hzmax, hsmax⟩
    rcases minDiff_vertex hne with ⟨vmin, hvmin, hzmin, hsmin⟩
    have hvest : bounds (List.map (ex.score 0) (vertices z (ex.cuts 0))) =
        some (max z.dl (z.xl-z.yh), min z.dh (z.xh-z.yl)) := by
      apply bounds_eq_minmax
      · exact List.mem_map.mpr ⟨vmin, hvmin, by simpa [ex, hsmin]⟩
      · intro e he
        rcases List.mem_map.mp he with ⟨v, hv, rfl⟩
        exact (score_range (vertex_inZone hv)).1
      · exact List.mem_map.mpr ⟨vmax, hvmax, by simpa [ex, hsmax]⟩
      · intro e he
        rcases List.mem_map.mp he with ⟨v, hv, rfl⟩
        exact (score_range (vertex_inZone hv)).2
    have hpest : bounds (List.map (ex.score 0) (points z)) =
        some (max z.dl (z.xl-z.yh), min z.dh (z.xh-z.yl)) := by
      apply bounds_eq_minmax
      · rcases minDiff_vertex hne with ⟨v, hv, hvz, hvs⟩
        exact List.mem_map.mpr ⟨v, point_mem_points hvz, by simpa [ex, hvs]⟩
      · intro e he
        rcases List.mem_map.mp he with ⟨p, hp, rfl⟩
        exact (score_range (List.mem_filter.mp hp).2).1
      · rcases maxDiff_vertex hne with ⟨v, hv, hvz, hvs⟩
        exact List.mem_map.mpr ⟨v, point_mem_points hvz, by simpa [ex, hvs]⟩
      · intro e he
        rcases List.mem_map.mp he with ⟨p, hp, rfl⟩
        exact (score_range (List.mem_filter.mp hp).2).2
    change bounds (List.map (ex.score 0) (vertices z (ex.cuts 0))) =
      some (max z.dl (z.xl-z.yh), min z.dh (z.xh-z.yl))
    exact hvest

theorem estimate_eq_empty {z : Zone} (hn : ¬ ∃ p, inZone z p = true) : estimate ex 0 z = none := by
  exact (deadEdge_general ex 0 z).2 (by
    intro p
    by_cases h : inZone z p = true
    · exact False.elim (hn ⟨p, h⟩)
    · simpa using h)


theorem minimalHull_cs : minimalHull ex := by
  intro t z
  classical
  by_cases hne : ∃ p, inZone z p = true
  · have he' : bounds (List.map (ex.score 0) (vertices z (ex.cuts 0))) =
        some (max z.dl (z.xl-z.yh), min z.dh (z.xh-z.yl)) := by
      simpa [estimate] using estimate_eq_nonempty hne
    have hp' : bounds (List.map (ex.score 0) (points z)) =
        some (max z.dl (z.xl-z.yh), min z.dh (z.xh-z.yl)) := by
      simpa [extrema] using extrema_eq_nonempty hne
    change bounds (List.map (ex.score 0) (vertices z (ex.cuts 0))) =
      bounds (List.map (ex.score 0) (points z))
    rw [he', hp']
  · have he := estimate_eq_empty hne
    have hpnil : points z = [] := by
      apply List.eq_nil_iff_forall_not_mem.mpr
      intro p hp
      exact hne ⟨p, (List.mem_filter.mp hp).2⟩
    have he' : bounds (List.map (ex.score 0) (vertices z (ex.cuts 0))) = none := by
      simpa [estimate] using he
    change bounds (List.map (ex.score 0) (vertices z (ex.cuts 0))) =
      bounds (List.map (ex.score 0) (points z))
    rw [he']
    simp [hpnil, bounds]

theorem coverage_cs : ∀ t z p, inZone z p = true → ∀ lo hi, estimate ex t z = some (lo,hi) →
    lo ≤ ex.score t p ∧ ex.score t p ≤ hi := by
  intro t z p hp lo hi he
  exact covers_of_estimate_eq hp he (minimalHull_cs t z)

end TinylambOpt.ResearchProofs.JitFrontiers.RelationalGuards.CorrelatedSubLemmas
