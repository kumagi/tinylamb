import TinylambOpt.ResearchProofs.JitFrontiers.RelationalGuards.Common

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitFrontiers.RelationalGuards.LeastShiftLemmas

open TinylambOpt.ResearchGoals.JitFrontiers.RelationalGuards
open TinylambOpt.ResearchProofs.JitFrontiers.RelationalGuards.Common
open TinylambOpt.ResearchProofs.JitFrontiers.RelationalGuards.CorrelatedSubLemmas

abbrev ls := TinylambOpt.ResearchGoals.JitFrontiers.RelationalGuards.LeastShift.expr

theorem min_mono {a b c d : Int} (h1 : a ≤ c) (h2 : b ≤ d) : min a b ≤ min c d := by
  have h1' : min a b ≤ c := Int.le_trans (Int.min_le_left a b) h1
  have h2' : min a b ≤ d := Int.le_trans (Int.min_le_right a b) h2
  exact Int.le_min.mpr ⟨h1', h2'⟩

theorem range_ls {z : Zone} {p : Point} (hp : inZone z p = true) :
    max z.dl (z.xl - z.yh) ≤ p.1 - p.2 ∧ p.1 - p.2 ≤ min z.dh (z.xh - z.yl) :=
  score_range hp

/-- Maximal vertex for min(x+t, y): some in-zone vertex dominates the point. -/
theorem ls_dom_hi (t : Int) (z : Zone) (p : Point) (hp : inZone z p = true) :
    ∃ v, v ∈ vertices z (ls.cuts t) ∧ ls.score t p ≤ ls.score t v := by
  rcases inZone_iff.mp hp with ⟨hxl,hxh,hyl,hyh,hdl,hdh⟩
  have hcuts : ls.cuts t = [-t] := by rfl
  rw [hcuts]
  by_cases hcase : z.dl ≤ z.xh - z.yh ∧ z.xh - z.yh ≤ z.dh
  · -- the (xh, yh) corner is inside the zone
    refine ⟨(z.xh, z.yh), mem_vertices_corner (z := z) (ds := [-t]) (x := z.xh) (y := z.yh)
        (Or.inr rfl) (Or.inr rfl) (by apply inZone_iff.mpr; dsimp; omega), ?_⟩
    change min (p.1 + t) p.2 ≤ min (z.xh + t) z.yh
    exact min_mono (by omega) (by omega)
  · by_cases hlow : z.xh - z.yh < z.dl
    · refine ⟨(z.xh, z.xh - z.dl), mem_vertices_of_x (z := z) (ds := [-t]) (x := z.xh) (d := z.dl)
        (Or.inr rfl) (Or.inl rfl) (by apply inZone_iff.mpr; dsimp; omega), ?_⟩
      change min (p.1 + t) p.2 ≤ min (z.xh + t) (z.xh - z.dl)
      exact min_mono (by omega) (by omega)
    · have hhigh : z.dh < z.xh - z.yh := by
        have : ¬ (z.dl ≤ z.xh - z.yh ∧ z.xh - z.yh ≤ z.dh) := hcase
        omega
      refine ⟨(z.yh + z.dh, z.yh), mem_vertices_of_y (z := z) (ds := [-t]) (y := z.yh) (d := z.dh)
          (Or.inr rfl) (Or.inr (Or.inl rfl)) (by apply inZone_iff.mpr; dsimp; omega), ?_⟩
      change min (p.1 + t) p.2 ≤ min (z.yh + z.dh + t) z.yh
      exact min_mono (by omega) (by omega)

/-- Minimal vertex for min(x+t, y): some in-zone vertex is dominated by the point. -/
theorem ls_dom_lo (t : Int) (z : Zone) (p : Point) (hp : inZone z p = true) :
    ∃ v, v ∈ vertices z (ls.cuts t) ∧ ls.score t v ≤ ls.score t p := by
  rcases inZone_iff.mp hp with ⟨hxl,hxh,hyl,hyh,hdl,hdh⟩
  have hcuts : ls.cuts t = [-t] := by rfl
  rw [hcuts]
  by_cases hcase : z.dl ≤ z.xl - z.yl ∧ z.xl - z.yl ≤ z.dh
  · refine ⟨(z.xl, z.yl), mem_vertices_corner (z := z) (ds := [-t]) (x := z.xl) (y := z.yl)
        (Or.inl rfl) (Or.inl rfl) (by apply inZone_iff.mpr; dsimp; omega), ?_⟩
    change min (z.xl + t) z.yl ≤ min (p.1 + t) p.2
    exact min_mono (by omega) (by omega)
  · by_cases hlow : z.xl - z.yl < z.dl
    · refine ⟨(z.yl + z.dl, z.yl), mem_vertices_of_y (z := z) (ds := [-t]) (y := z.yl) (d := z.dl)
        (Or.inl rfl) (Or.inl rfl) (by apply inZone_iff.mpr; dsimp; omega), ?_⟩
      change min (z.yl + z.dl + t) z.yl ≤ min (p.1 + t) p.2
      exact min_mono (by omega) (by omega)
    · have hhigh : z.dh < z.xl - z.yl := by
        have : ¬ (z.dl ≤ z.xl - z.yl ∧ z.xl - z.yl ≤ z.dh) := hcase
        omega
      refine ⟨(z.xl, z.xl - z.dh), mem_vertices_of_x (z := z) (ds := [-t]) (x := z.xl) (d := z.dh)
          (Or.inl rfl) (Or.inr (Or.inl rfl)) (by apply inZone_iff.mpr; dsimp; omega), ?_⟩
      change min (z.xl + t) (z.xl - z.dh) ≤ min (p.1 + t) p.2
      exact min_mono (by omega) (by omega)

theorem minimalHull_ls : minimalHull ls := minimalHull_of_dom ls_dom_hi ls_dom_lo

theorem coverage_ls : ∀ t z p, inZone z p = true → ∀ lo hi, estimate ls t z = some (lo,hi) →
    lo ≤ ls.score t p ∧ ls.score t p ≤ hi := coverage_of_dom ls_dom_hi ls_dom_lo

end TinylambOpt.ResearchProofs.JitFrontiers.RelationalGuards.LeastShiftLemmas
