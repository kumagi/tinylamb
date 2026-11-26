import TinylambOpt.ResearchProofs.JitFrontiers.RelationalGuards.Common

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.JitFrontiers.RelationalGuards.WeightedMixLemmas

open TinylambOpt.ResearchGoals.JitFrontiers.RelationalGuards
open TinylambOpt.ResearchProofs.JitFrontiers.RelationalGuards.Common
open TinylambOpt.ResearchProofs.JitFrontiers.RelationalGuards.CorrelatedSubLemmas

abbrev wm := TinylambOpt.ResearchGoals.JitFrontiers.RelationalGuards.WeightedMix.expr

/-- Affine bookkeeping identities for coupled weighted-mix steps. -/
theorem id1 (t x d : Int) : t*x + (1-t)*(x - d) = x - (1-t)*d := by
  rw [Int.mul_sub, Int.sub_mul, Int.one_mul]
  omega
theorem id2 (t x d : Int) : t*x + (1-t)*(x + d) = x + (1-t)*d := by
  rw [Int.mul_add, Int.sub_mul, Int.one_mul]
  omega
theorem id3 (t y d : Int) : t*(y + d) + (1-t)*y = y + t*d := by
  rw [Int.mul_add, Int.sub_mul, Int.one_mul]
  omega
theorem id4 (t y d : Int) : t*(y - d) + (1-t)*y = y - t*d := by
  rw [Int.mul_sub, Int.sub_mul, Int.one_mul]
  omega

/-- Canonical forms of the weighted mix (usable via `exact`, which unfolds `score` up to defeq). -/
theorem sp1 (t x y : Int) : t*x + (1-t)*y = y + t*(x - y) := by
  have h : x = y + (x - y) := by omega
  rw [h]
  have h2 : y + (x - y) - y = x - y := by omega
  rw [h2]
  exact id3 t y (x - y)
theorem sp2 (t x y : Int) : t*x + (1-t)*y = x - (1-t)*(x - y) := by
  have h : y = x - (x - y) := by omega
  rw [h]
  have h2 : x - (x - (x - y)) = x - y := by omega
  rw [h2]
  exact id1 t x (x - y)

/-- A point's weighted mix is dominated by some in-zone vertex score. -/
theorem wm_dom_hi (t : Int) (z : Zone) (p : Point) (hp : inZone z p = true) :
    ∃ v, v ∈ vertices z (wm.cuts t) ∧ wm.score t p ≤ wm.score t v := by
  rcases inZone_iff.mp hp with ⟨hxl,hxh,hyl,hyh,hdl,hdh⟩
  have hcuts : wm.cuts t = [] := by rfl
  rw [hcuts]
  by_cases hA : 0 ≤ t
  · by_cases hB : 0 ≤ 1 - t
    · -- Q++: increasing in both coordinates
      by_cases hcase : z.dl ≤ z.xh - z.yh ∧ z.xh - z.yh ≤ z.dh
      · refine ⟨(z.xh, z.yh), mem_vertices_corner (z := z) (ds := []) (x := z.xh) (y := z.yh)
            (Or.inr rfl) (Or.inr rfl) (by apply inZone_iff.mpr; dsimp; omega), ?_⟩
        change t*p.1 + (1-t)*p.2 ≤ t*z.xh + (1-t)*z.yh
        exact Int.add_le_add (Int.mul_le_mul_of_nonneg_left (by omega) hA)
          (Int.mul_le_mul_of_nonneg_left (by omega) hB)
      · by_cases hlow : z.xh - z.yh < z.dl
        · refine ⟨(z.xh, z.xh - z.dl), mem_vertices_of_x (z := z) (ds := []) (x := z.xh) (d := z.dl)
              (Or.inr rfl) (Or.inl rfl) (by apply inZone_iff.mpr; dsimp; omega), ?_⟩
          change t*p.1 + (1-t)*p.2 ≤ t*z.xh + (1-t)*(z.xh - z.dl)
          exact Int.add_le_add (Int.mul_le_mul_of_nonneg_left (by omega) hA)
            (Int.mul_le_mul_of_nonneg_left (by omega) hB)
        · refine ⟨(z.yh + z.dh, z.yh), mem_vertices_of_y (z := z) (ds := []) (y := z.yh) (d := z.dh)
              (Or.inr rfl) (Or.inr (Or.inl rfl)) (by apply inZone_iff.mpr; dsimp; omega), ?_⟩
          change t*p.1 + (1-t)*p.2 ≤ t*(z.yh + z.dh) + (1-t)*z.yh
          exact Int.add_le_add (Int.mul_le_mul_of_nonneg_left (by omega) hA)
            (Int.mul_le_mul_of_nonneg_left (by omega) hB)
    · -- Q+-: increasing in x, decreasing in y
      by_cases hcase : z.dl ≤ z.xh - z.yl ∧ z.xh - z.yl ≤ z.dh
      · refine ⟨(z.xh, z.yl), mem_vertices_corner (z := z) (ds := []) (x := z.xh) (y := z.yl)
            (Or.inr rfl) (Or.inl rfl) (by apply inZone_iff.mpr; dsimp; omega), ?_⟩
        change t*p.1 + (1-t)*p.2 ≤ t*z.xh + (1-t)*z.yl
        exact Int.add_le_add (Int.mul_le_mul_of_nonneg_left (by omega) hA)
          (Int.mul_le_mul_of_nonpos_left (by omega : 1 - t ≤ 0) (by omega))
      · by_cases hlow : z.xh - z.yl < z.dl
        · exfalso; omega
        · by_cases hcap : z.xh - z.dh ≤ z.yh
          · refine ⟨(z.xh, z.xh - z.dh), mem_vertices_of_x (z := z) (ds := []) (x := z.xh) (d := z.dh)
                (Or.inr rfl) (Or.inr (Or.inl rfl)) (by apply inZone_iff.mpr; dsimp; omega), ?_⟩
            have h1 : (1-t)*p.2 ≤ (1-t)*(p.1 - z.dh) :=
              Int.mul_le_mul_of_nonpos_left (by omega : 1 - t ≤ 0) (by omega : p.1 - z.dh ≤ p.2)
            have h2 : t*p.1 + (1-t)*(p.1 - z.dh) ≤ t*z.xh + (1-t)*(z.xh - z.dh) := by
              rw [id1 t p.1 z.dh, id1 t z.xh z.dh]
              omega
            calc t*p.1 + (1-t)*p.2 ≤ t*p.1 + (1-t)*(p.1 - z.dh) := Int.add_le_add_left h1 _
              _ ≤ t*z.xh + (1-t)*(z.xh - z.dh) := h2
          · refine ⟨(z.yh + z.dh, z.yh), mem_vertices_of_y (z := z) (ds := []) (y := z.yh) (d := z.dh)
                (Or.inr rfl) (Or.inr (Or.inl rfl)) (by apply inZone_iff.mpr; dsimp; omega), ?_⟩
            have h2 : t*(p.1 - p.2) ≤ t*z.dh := Int.mul_le_mul_of_nonneg_left (by omega) hA
            calc t*p.1 + (1-t)*p.2 = p.2 + t*(p.1 - p.2) := sp1 t p.1 p.2
              _ ≤ p.2 + t*z.dh := Int.add_le_add_left h2 _
              _ ≤ z.yh + t*z.dh := Int.add_le_add_right (by omega) _
              _ = t*(z.yh + z.dh) + (1-t)*z.yh := (id3 t z.yh z.dh).symm
  · by_cases hB : 0 ≤ 1 - t
    · -- Q-+: decreasing in x, increasing in y
      by_cases hcase : z.dl ≤ z.xl - z.yh ∧ z.xl - z.yh ≤ z.dh
      · refine ⟨(z.xl, z.yh), mem_vertices_corner (z := z) (ds := []) (x := z.xl) (y := z.yh)
            (Or.inl rfl) (Or.inr rfl) (by apply inZone_iff.mpr; dsimp; omega), ?_⟩
        change t*p.1 + (1-t)*p.2 ≤ t*z.xl + (1-t)*z.yh
        exact Int.add_le_add (Int.mul_le_mul_of_nonpos_left (by omega : t ≤ 0) (by omega))
          (Int.mul_le_mul_of_nonneg_left (by omega) hB)
      · by_cases hlow : z.xl - z.yh < z.dl
        · by_cases hcap : z.yh + z.dl ≤ z.xh
          · refine ⟨(z.yh + z.dl, z.yh), mem_vertices_of_y (z := z) (ds := []) (y := z.yh) (d := z.dl)
                (Or.inr rfl) (Or.inl rfl) (by apply inZone_iff.mpr; dsimp; omega), ?_⟩
            have h1 : t*p.1 ≤ t*(p.2 + z.dl) :=
              Int.mul_le_mul_of_nonpos_left (by omega : t ≤ 0) (by omega)
            calc t*p.1 + (1-t)*p.2 ≤ t*(p.2 + z.dl) + (1-t)*p.2 := Int.add_le_add_right h1 _
              _ = p.2 + t*z.dl := id3 t p.2 z.dl
              _ ≤ z.yh + t*z.dl := by omega
              _ = t*(z.yh + z.dl) + (1-t)*z.yh := (id3 t z.yh z.dl).symm
          · have hy : z.yl ≤ z.xh - z.dl := by omega
            refine ⟨(z.xh, z.xh - z.dl), mem_vertices_of_x (z := z) (ds := []) (x := z.xh) (d := z.dl)
                (Or.inr rfl) (Or.inl rfl) (by apply inZone_iff.mpr; dsimp; omega), ?_⟩
            have h1 : t*p.1 ≤ t*(p.2 + z.dl) :=
              Int.mul_le_mul_of_nonpos_left (by omega : t ≤ 0) (by omega)
            have e : (z.xh - z.dl) + t*z.dl = z.xh - (1-t)*z.dl := by
              simp only [Int.sub_mul, Int.mul_sub, Int.one_mul]
              omega
            calc t*p.1 + (1-t)*p.2 ≤ t*(p.2 + z.dl) + (1-t)*p.2 := Int.add_le_add_right h1 _
              _ = p.2 + t*z.dl := id3 t p.2 z.dl
              _ ≤ (z.xh - z.dl) + t*z.dl := by omega
              _ = z.xh - (1-t)*z.dl := e
              _ = t*z.xh + (1-t)*(z.xh - z.dl) := (id1 t z.xh z.dl).symm
        · -- hlow false: dh < xl - yh
          exfalso
          have : ¬ (z.dl ≤ z.xl - z.yh ∧ z.xl - z.yh ≤ z.dh) := hcase
          omega
    · exfalso; omega

/-- An in-zone vertex score dominates some point's weighted mix from below. -/
theorem wm_dom_lo (t : Int) (z : Zone) (p : Point) (hp : inZone z p = true) :
    ∃ v, v ∈ vertices z (wm.cuts t) ∧ wm.score t v ≤ wm.score t p := by
  rcases inZone_iff.mp hp with ⟨hxl,hxh,hyl,hyh,hdl,hdh⟩
  have hcuts : wm.cuts t = [] := by rfl
  rw [hcuts]
  by_cases hA : 0 ≤ t
  · by_cases hB : 0 ≤ 1 - t
    · -- Q++: minimize: go to lower-left
      by_cases hcase : z.dl ≤ z.xl - z.yl ∧ z.xl - z.yl ≤ z.dh
      · refine ⟨(z.xl, z.yl), mem_vertices_corner (z := z) (ds := []) (x := z.xl) (y := z.yl)
            (Or.inl rfl) (Or.inl rfl) (by apply inZone_iff.mpr; dsimp; omega), ?_⟩
        change t*z.xl + (1-t)*z.yl ≤ t*p.1 + (1-t)*p.2
        exact Int.add_le_add (Int.mul_le_mul_of_nonneg_left (by omega) hA)
          (Int.mul_le_mul_of_nonneg_left (by omega) hB)
      · by_cases hlow : z.xl - z.yl < z.dl
        · refine ⟨(z.yl + z.dl, z.yl), mem_vertices_of_y (z := z) (ds := []) (y := z.yl) (d := z.dl)
              (Or.inl rfl) (Or.inl rfl) (by apply inZone_iff.mpr; dsimp; omega), ?_⟩
          change t*(z.yl + z.dl) + (1-t)*z.yl ≤ t*p.1 + (1-t)*p.2
          exact Int.add_le_add (Int.mul_le_mul_of_nonneg_left (by omega) hA)
            (Int.mul_le_mul_of_nonneg_left (by omega) hB)
        · -- dh < xl - yl
          refine ⟨(z.xl, z.xl - z.dh), mem_vertices_of_x (z := z) (ds := []) (x := z.xl) (d := z.dh)
              (Or.inl rfl) (Or.inr (Or.inl rfl)) (by apply inZone_iff.mpr; dsimp; omega), ?_⟩
          change t*z.xl + (1-t)*(z.xl - z.dh) ≤ t*p.1 + (1-t)*p.2
          exact Int.add_le_add (Int.mul_le_mul_of_nonneg_left (by omega) hA)
            (Int.mul_le_mul_of_nonneg_left (by omega) hB)
    · -- Q+-: t ≥ 0, 1-t ≤ 0: minimize: x → xl, y → yh
      by_cases hcase : z.dl ≤ z.xl - z.yh ∧ z.xl - z.yh ≤ z.dh
      · refine ⟨(z.xl, z.yh), mem_vertices_corner (z := z) (ds := []) (x := z.xl) (y := z.yh)
            (Or.inl rfl) (Or.inr rfl) (by apply inZone_iff.mpr; dsimp; omega), ?_⟩
        change t*z.xl + (1-t)*z.yh ≤ t*p.1 + (1-t)*p.2
        exact Int.add_le_add (Int.mul_le_mul_of_nonneg_left (by omega) hA)
          (Int.mul_le_mul_of_nonpos_left (by omega : 1 - t ≤ 0) (by omega))
      · by_cases hlow : z.xl - z.yh < z.dl
        · by_cases hq : z.yl ≤ z.xl - z.dl
          · refine ⟨(z.xl, z.xl - z.dl), mem_vertices_of_x (z := z) (ds := []) (x := z.xl) (d := z.dl)
                (Or.inl rfl) (Or.inl rfl) (by apply inZone_iff.mpr; dsimp; omega), ?_⟩
            have h1 : (1-t)*(p.1 - p.2) ≤ (1-t)*z.dl :=
              Int.mul_le_mul_of_nonpos_left (by omega : 1 - t ≤ 0) (by omega)
            calc t*z.xl + (1-t)*(z.xl - z.dl) = z.xl - (1-t)*z.dl := id1 t z.xl z.dl
              _ ≤ z.xl - (1-t)*(p.1 - p.2) := Int.sub_le_sub_left h1 _
              _ ≤ p.1 - (1-t)*(p.1 - p.2) := Int.sub_le_sub_right (by omega) _
              _ = t*p.1 + (1-t)*p.2 := (sp2 t p.1 p.2).symm
          · refine ⟨(z.yl + z.dl, z.yl), mem_vertices_of_y (z := z) (ds := []) (y := z.yl) (d := z.dl)
                (Or.inl rfl) (Or.inl rfl) (by apply inZone_iff.mpr; dsimp; omega), ?_⟩
            have sv : t*(z.yl + z.dl) + (1-t)*z.yl = z.yl + t*z.dl := id3 t z.yl z.dl
            have sp : t*p.1 + (1-t)*p.2 = p.2 + t*(p.1 - p.2) := sp1 t p.1 p.2
            have h2 : t*z.dl ≤ t*(p.1 - p.2) := Int.mul_le_mul_of_nonneg_left (by omega) hA
            change t*(z.yl+z.dl)+(1-t)*z.yl ≤ t*p.1+(1-t)*p.2
            rw [sv, sp]
            omega
        · exfalso
          have : ¬ (z.dl ≤ z.xl - z.yh ∧ z.xl - z.yh ≤ z.dh) := hcase
          omega
  · by_cases hB : 0 ≤ 1 - t
    · -- Q-+: t ≤ 0, 1-t ≥ 0: minimize: x → xh, y → yl
      by_cases hcase : z.dl ≤ z.xh - z.yl ∧ z.xh - z.yl ≤ z.dh
      · refine ⟨(z.xh, z.yl), mem_vertices_corner (z := z) (ds := []) (x := z.xh) (y := z.yl)
            (Or.inr rfl) (Or.inl rfl) (by apply inZone_iff.mpr; dsimp; omega), ?_⟩
        change t*z.xh + (1-t)*z.yl ≤ t*p.1 + (1-t)*p.2
        exact Int.add_le_add (Int.mul_le_mul_of_nonpos_left (by omega : t ≤ 0) (by omega))
          (Int.mul_le_mul_of_nonneg_left (by omega) hB)
      · by_cases hlow : z.xh - z.yl < z.dl
        · exfalso; omega
        · by_cases hq2 : z.xl ≤ z.yl + z.dh
          · refine ⟨(z.yl + z.dh, z.yl), mem_vertices_of_y (z := z) (ds := []) (y := z.yl) (d := z.dh)
                (Or.inl rfl) (Or.inr (Or.inl rfl)) (by apply inZone_iff.mpr; dsimp; omega), ?_⟩
            have sv : t*(z.yl + z.dh) + (1-t)*z.yl = z.yl + t*z.dh := id3 t z.yl z.dh
            have sp : t*p.1 + (1-t)*p.2 = p.2 + t*(p.1 - p.2) := sp1 t p.1 p.2
            have h2 : t*z.dh ≤ t*(p.1 - p.2) :=
              Int.mul_le_mul_of_nonpos_left (by omega : t ≤ 0) (by omega)
            change t*(z.yl+z.dh)+(1-t)*z.yl ≤ t*p.1+(1-t)*p.2
            rw [sv, sp]
            omega
          · refine ⟨(z.xl, z.xl - z.dh), mem_vertices_of_x (z := z) (ds := []) (x := z.xl) (d := z.dh)
                (Or.inl rfl) (Or.inr (Or.inl rfl)) (by apply inZone_iff.mpr; dsimp; omega), ?_⟩
            have h1 : t*(p.2 + z.dh) ≤ t*p.1 :=
              Int.mul_le_mul_of_nonpos_left (by omega : t ≤ 0) (by omega)
            have e : z.xl - (1-t)*z.dh = (z.xl - z.dh) + t*z.dh := by
              simp only [Int.sub_mul, Int.mul_sub, Int.one_mul]
              omega
            calc t*z.xl + (1-t)*(z.xl - z.dh) = z.xl - (1-t)*z.dh := id1 t z.xl z.dh
              _ = (z.xl - z.dh) + t*z.dh := e
              _ ≤ p.2 + t*z.dh := by omega
              _ = t*(p.2 + z.dh) + (1-t)*p.2 := (id3 t p.2 z.dh).symm
              _ ≤ t*p.1 + (1-t)*p.2 := Int.add_le_add_right h1 _
    · exfalso; omega

theorem minimalHull_wm : minimalHull wm := minimalHull_of_dom wm_dom_hi wm_dom_lo

theorem coverage_wm : ∀ t z p, inZone z p = true → ∀ lo hi, estimate wm t z = some (lo,hi) →
    lo ≤ wm.score t p ∧ wm.score t p ≤ hi := coverage_of_dom wm_dom_hi wm_dom_lo

end TinylambOpt.ResearchProofs.JitFrontiers.RelationalGuards.WeightedMixLemmas
