import TinylambOpt.ResearchProofs.JitFrontiers.RelationalGuards.Common

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.JitFrontiers.RelationalGuards.ProductCornersLemmas

open TinylambOpt.ResearchGoals.JitFrontiers.RelationalGuards
open TinylambOpt.ResearchProofs.JitFrontiers.RelationalGuards.Common

abbrev pc := TinylambOpt.ResearchGoals.JitFrontiers.RelationalGuards.ProductCorners.expr

theorem vertices_nil_of_map_nil {z : Zone} {ds : List Int} {f : Point → Int}
    (h : (vertices z ds).map f = []) : vertices z ds = [] := by
  cases hv : vertices z ds with
  | nil => rfl
  | cons v vs => simp [hv] at h

theorem sqdiff_up (U y d : Int) : U*(U+d) - y*(y+d) = (U-y)*(U+y+d) := by
  simp only [Int.sub_mul, Int.mul_add, Int.add_mul]
  rw [Int.mul_comm y U]
  omega

theorem sqdiff_lo (y L d : Int) : y*(y+d) - L*(L+d) = (y-L)*(y+L+d) := by
  simp only [Int.sub_mul, Int.mul_add, Int.add_mul]
  rw [Int.mul_comm L y]
  omega

theorem vertex_on_xy_boundary {z : Zone} {ds : List Int} {v : Point} (hv : v ∈ vertices z ds) :
    (v.1 = z.xl ∨ v.1 = z.xh) ∨ (v.2 = z.yl ∨ v.2 = z.yh) := by
  rw [vertices] at hv
  rcases (List.mem_filter.mp hv).1 with hraw
  simp only [List.mem_append, List.mem_flatMap, List.mem_map] at hraw
  rcases hraw with (hraw | hraw) | hraw
  · rcases hraw with ⟨x, hxmem, hyrest⟩
    rcases hyrest with ⟨y, hymem, hxy⟩
    simp at hxmem hymem
    subst v
    omega
  · rcases hraw with ⟨x, hxmem, hdrest⟩
    rcases hdrest with ⟨d, hdmem, hxd⟩
    simp at hxmem
    subst v
    omega
  · rcases hraw with ⟨y, hymem, hdrest⟩
    rcases hdrest with ⟨d, hdmem, hyd⟩
    simp at hymem
    subst v
    omega

theorem d_line_upper {z : Zone} {d y : Int}
    (hd : d = z.dl ∨ d = z.dh)
    (hp : inZone z (y+d, y) = true) :
    ∃ v, v ∈ vertices z [] ∧ (y+d)*y ≤ v.1*v.2 := by
  rcases inZone_iff.mp hp with ⟨hxl, hxh, hyl, hyh, hdl, hdh⟩
  have hd_bounds : z.dl ≤ d ∧ d ≤ z.dh := by omega
  let L : Int := max z.yl (z.xl - d)
  let U : Int := min z.yh (z.xh - d)
  have hLy : L ≤ y := Int.max_le.mpr ⟨hyl, by omega⟩
  have hyU : y ≤ U := Int.le_min.mpr ⟨hyh, by omega⟩
  have hLU : L ≤ U := Int.le_trans hLy hyU
  by_cases hlow : y + L + d ≤ 0
  · refine ⟨(L+d, L), ?_, ?_⟩
    · by_cases hcase : z.yl ≤ z.xl - d
      · have hLval : L = z.xl - d := Int.max_eq_right hcase
        rw [hLval, show z.xl - d + d = z.xl by omega]
        apply mem_vertices_of_x (z := z) (ds := []) (x := z.xl) (d := d) (Or.inl rfl)
        · rcases hd with rfl | rfl <;> simp
        · apply inZone_iff.mpr
          dsimp
          omega
      · have hLval : L = z.yl := Int.max_eq_left (by omega : z.xl - d ≤ z.yl)
        rw [hLval]
        apply mem_vertices_of_y (z := z) (ds := []) (y := z.yl) (d := d) (Or.inl rfl)
        · rcases hd with rfl | rfl <;> simp
        · apply inZone_iff.mpr
          dsimp
          omega
    · have hprod : (y+d)*y - (L+d)*L ≤ 0 := by
        rw [show (y+d)*y = y*(y+d) by rw [Int.mul_comm],
            show (L+d)*L = L*(L+d) by rw [Int.mul_comm]]
        rw [sqdiff_lo y L d]
        exact Int.mul_nonpos_of_nonneg_of_nonpos (by omega) hlow
      change (y+d)*y ≤ (L+d)*L
      omega
  · have hgt : 0 ≤ U + y + d := by
      have : 0 < y + L + d := by omega
      omega
    refine ⟨(U+d, U), ?_, ?_⟩
    · by_cases hcase : z.yh ≤ z.xh - d
      · have hUval : U = z.yh := Int.min_eq_left hcase
        rw [hUval]
        apply mem_vertices_of_y (z := z) (ds := []) (y := z.yh) (d := d) (Or.inr rfl)
        · rcases hd with rfl | rfl <;> simp
        · apply inZone_iff.mpr
          dsimp
          omega
      · have hUval : U = z.xh - d := Int.min_eq_right (by omega : z.xh - d ≤ z.yh)
        rw [hUval, show z.xh - d + d = z.xh by omega]
        apply mem_vertices_of_x (z := z) (ds := []) (x := z.xh) (d := d) (Or.inr rfl)
        · rcases hd with rfl | rfl <;> simp
        · apply inZone_iff.mpr
          dsimp
          omega
    · have hprod : 0 ≤ (U+d)*U - (y+d)*y := by
        rw [show (U+d)*U = U*(U+d) by rw [Int.mul_comm],
            show (y+d)*y = y*(y+d) by rw [Int.mul_comm]]
        rw [sqdiff_up U y d]
        exact Int.mul_nonneg (by omega) hgt
      change (y+d)*y ≤ (U+d)*U
      omega

theorem x_boundary_upper {z : Zone} {x y : Int}
    (hx : x = z.xl ∨ x = z.xh)
    (hp : inZone z (x, y) = true) :
    ∃ v, v ∈ vertices z [] ∧ x*y ≤ v.1*v.2 := by
  rcases inZone_iff.mp hp with ⟨hxl, hxh, hyl, hyh, hdl, hdh⟩
  let L : Int := max z.yl (x - z.dh)
  let U : Int := min z.yh (x - z.dl)
  have hLy : L ≤ y := Int.max_le.mpr ⟨hyl, by omega⟩
  have hyU : y ≤ U := Int.le_min.mpr ⟨hyh, by omega⟩
  by_cases hxpos : 0 ≤ x
  · refine ⟨(x, U), ?_, ?_⟩
    · by_cases hcase : z.yh ≤ x - z.dl
      · have hUval : U = z.yh := Int.min_eq_left hcase
        rw [hUval]
        exact mem_vertices_corner (z := z) (ds := []) hx (Or.inr rfl) (by apply inZone_iff.mpr; dsimp; omega)
      · have hUval : U = x - z.dl := Int.min_eq_right (by omega : x - z.dl ≤ z.yh)
        rw [hUval]
        apply mem_vertices_of_x (z := z) (ds := []) (x := x) (d := z.dl) hx (Or.inl rfl)
        apply inZone_iff.mpr
        dsimp
        omega
    · exact Int.mul_le_mul_of_nonneg_left hyU hxpos
  · have hxnonpos : x ≤ 0 := by omega
    refine ⟨(x, L), ?_, ?_⟩
    · by_cases hcase : z.yl ≤ x - z.dh
      · have hLval : L = x - z.dh := Int.max_eq_right hcase
        rw [hLval]
        apply mem_vertices_of_x (z := z) (ds := []) (x := x) (d := z.dh) hx (Or.inr (Or.inl rfl))
        apply inZone_iff.mpr
        dsimp
        omega
      · have hLval : L = z.yl := Int.max_eq_left (by omega : x - z.dh ≤ z.yl)
        rw [hLval]
        exact mem_vertices_corner (z := z) (ds := []) hx (Or.inl rfl) (by apply inZone_iff.mpr; dsimp; omega)
    · exact Int.mul_le_mul_of_nonpos_left hxnonpos hLy

theorem x_boundary_lower {z : Zone} {x y : Int}
    (hx : x = z.xl ∨ x = z.xh)
    (hp : inZone z (x, y) = true) :
    ∃ v, v ∈ vertices z [] ∧ v.1*v.2 ≤ x*y := by
  rcases inZone_iff.mp hp with ⟨hxl, hxh, hyl, hyh, hdl, hdh⟩
  let L : Int := max z.yl (x - z.dh)
  let U : Int := min z.yh (x - z.dl)
  have hLy : L ≤ y := Int.max_le.mpr ⟨hyl, by omega⟩
  have hyU : y ≤ U := Int.le_min.mpr ⟨hyh, by omega⟩
  by_cases hxpos : 0 ≤ x
  · refine ⟨(x, L), ?_, ?_⟩
    · by_cases hcase : z.yl ≤ x - z.dh
      · have hLval : L = x - z.dh := Int.max_eq_right hcase
        rw [hLval]
        apply mem_vertices_of_x (z := z) (ds := []) (x := x) (d := z.dh) hx (Or.inr (Or.inl rfl))
        apply inZone_iff.mpr
        dsimp
        omega
      · have hLval : L = z.yl := Int.max_eq_left (by omega : x - z.dh ≤ z.yl)
        rw [hLval]
        exact mem_vertices_corner (z := z) (ds := []) hx (Or.inl rfl) (by apply inZone_iff.mpr; dsimp; omega)
    · exact Int.mul_le_mul_of_nonneg_left hLy hxpos
  · have hxnonpos : x ≤ 0 := by omega
    refine ⟨(x, U), ?_, ?_⟩
    · by_cases hcase : z.yh ≤ x - z.dl
      · have hUval : U = z.yh := Int.min_eq_left hcase
        rw [hUval]
        exact mem_vertices_corner (z := z) (ds := []) hx (Or.inr rfl) (by apply inZone_iff.mpr; dsimp; omega)
      · have hUval : U = x - z.dl := Int.min_eq_right (by omega : x - z.dl ≤ z.yh)
        rw [hUval]
        apply mem_vertices_of_x (z := z) (ds := []) (x := x) (d := z.dl) hx (Or.inl rfl)
        apply inZone_iff.mpr
        dsimp
        omega
    · exact Int.mul_le_mul_of_nonpos_left hxnonpos hyU

theorem y_boundary_lower {z : Zone} {x y : Int}
    (hy : y = z.yl ∨ y = z.yh)
    (hp : inZone z (x, y) = true) :
    ∃ v, v ∈ vertices z [] ∧ v.1*v.2 ≤ x*y := by
  rcases inZone_iff.mp hp with ⟨hxl, hxh, hyl, hyh, hdl, hdh⟩
  let L : Int := max z.xl (y + z.dl)
  let U : Int := min z.xh (y + z.dh)
  have hLx : L ≤ x := Int.max_le.mpr ⟨hxl, by omega⟩
  have hxU : x ≤ U := Int.le_min.mpr ⟨hxh, by omega⟩
  by_cases hypos : 0 ≤ y
  · refine ⟨(L, y), ?_, ?_⟩
    · by_cases hcase : z.xl ≤ y + z.dl
      · have hLval : L = y + z.dl := Int.max_eq_right hcase
        rw [hLval]
        apply mem_vertices_of_y (z := z) (ds := []) (y := y) (d := z.dl) hy (Or.inl rfl)
        apply inZone_iff.mpr
        dsimp
        omega
      · have hLval : L = z.xl := Int.max_eq_left (by omega : y + z.dl ≤ z.xl)
        rw [hLval]
        exact mem_vertices_corner (z := z) (ds := []) (Or.inl rfl) hy (by apply inZone_iff.mpr; dsimp; omega)
    · exact Int.mul_le_mul_of_nonneg_right hLx hypos
  · have hynonpos : y ≤ 0 := by omega
    refine ⟨(U, y), ?_, ?_⟩
    · by_cases hcase : z.xh ≤ y + z.dh
      · have hUval : U = z.xh := Int.min_eq_left hcase
        rw [hUval]
        exact mem_vertices_corner (z := z) (ds := []) (Or.inr rfl) hy (by apply inZone_iff.mpr; dsimp; omega)
      · have hUval : U = y + z.dh := Int.min_eq_right (by omega : y + z.dh ≤ z.xh)
        rw [hUval]
        apply mem_vertices_of_y (z := z) (ds := []) (y := y) (d := z.dh) hy (Or.inr (Or.inl rfl))
        apply inZone_iff.mpr
        dsimp
        omega
    · exact Int.mul_le_mul_of_nonpos_right hxU hynonpos

theorem y_boundary_upper {z : Zone} {x y : Int}
    (hy : y = z.yl ∨ y = z.yh)
    (hp : inZone z (x, y) = true) :
    ∃ v, v ∈ vertices z [] ∧ x*y ≤ v.1*v.2 := by
  rcases inZone_iff.mp hp with ⟨hxl, hxh, hyl, hyh, hdl, hdh⟩
  let L : Int := max z.xl (y + z.dl)
  let U : Int := min z.xh (y + z.dh)
  have hLx : L ≤ x := Int.max_le.mpr ⟨hxl, by omega⟩
  have hxU : x ≤ U := Int.le_min.mpr ⟨hxh, by omega⟩
  by_cases hypos : 0 ≤ y
  · refine ⟨(U, y), ?_, ?_⟩
    · by_cases hcase : z.xh ≤ y + z.dh
      · have hUval : U = z.xh := Int.min_eq_left hcase
        rw [hUval]
        exact mem_vertices_corner (z := z) (ds := []) (Or.inr rfl) hy (by apply inZone_iff.mpr; dsimp; omega)
      · have hUval : U = y + z.dh := Int.min_eq_right (by omega : y + z.dh ≤ z.xh)
        rw [hUval]
        apply mem_vertices_of_y (z := z) (ds := []) (y := y) (d := z.dh) hy (Or.inr (Or.inl rfl))
        apply inZone_iff.mpr
        dsimp
        omega
    · exact Int.mul_le_mul_of_nonneg_right hxU hypos
  · have hynonpos : y ≤ 0 := by omega
    refine ⟨(L, y), ?_, ?_⟩
    · by_cases hcase : z.xl ≤ y + z.dl
      · have hLval : L = y + z.dl := Int.max_eq_right hcase
        rw [hLval]
        apply mem_vertices_of_y (z := z) (ds := []) (y := y) (d := z.dl) hy (Or.inl rfl)
        apply inZone_iff.mpr
        dsimp
        omega
      · have hLval : L = z.xl := Int.max_eq_left (by omega : y + z.dl ≤ z.xl)
        rw [hLval]
        exact mem_vertices_corner (z := z) (ds := []) (Or.inl rfl) hy (by apply inZone_iff.mpr; dsimp; omega)
    · exact Int.mul_le_mul_of_nonpos_right hLx hynonpos

theorem prod_upper {z : Zone} {p : Point} (hp : inZone z p = true) :
    ∃ v, v ∈ vertices z [] ∧ p.1*p.2 ≤ v.1*v.2 := by
  rcases inZone_iff.mp hp with ⟨hxl, hxh, hyl, hyh, hdl, hdh⟩
  by_cases hypos : 0 ≤ p.2
  · let x' : Int := min z.xh (p.2 + z.dh)
    have hp_le : p.1*p.2 ≤ x'*p.2 := Int.mul_le_mul_of_nonneg_right (Int.le_min.mpr ⟨hxh, by omega⟩) hypos
    have hxzone : inZone z (x', p.2) = true := by
      apply inZone_iff.mpr
      dsimp [x']
      constructor
      · exact Int.le_trans hxl (Int.le_min.mpr ⟨by omega, by omega⟩)
      constructor
      · exact Int.min_le_left _ _
      constructor
      · exact hyl
      constructor
      · exact hyh
      constructor
      · omega
      · omega
    by_cases hxc : x' = z.xh
    · rcases x_boundary_upper (z := z) (x := z.xh) (y := p.2) (Or.inr rfl) (by simpa [hxc] using hxzone) with ⟨v, hv, hvle⟩
      have hp_le' : p.1*p.2 ≤ z.xh*p.2 := by simpa [hxc] using hp_le
      exact ⟨v, hv, Int.le_trans hp_le' hvle⟩
    · have hxd : x' = p.2 + z.dh := by
        have hnot : ¬ z.xh ≤ p.2 + z.dh := by
          intro h
          exact hxc (Int.min_eq_left h)
        have : p.2 + z.dh ≤ z.xh := by omega
        exact Int.min_eq_right this
      rcases d_line_upper (z := z) (d := z.dh) (Or.inr rfl) (by simpa [hxd] using hxzone) with ⟨v, hv, hvle⟩
      exact ⟨v, hv, Int.le_trans hp_le (by simpa [hxd] using hvle)⟩
  · have hynonpos : p.2 ≤ 0 := by omega
    let x' : Int := max z.xl (p.2 + z.dl)
    have hp_le : p.1*p.2 ≤ x'*p.2 := by
      exact Int.mul_le_mul_of_nonpos_right (by dsimp [x']; exact Int.max_le.mpr ⟨hxl, by omega⟩) hynonpos
    have hxzone : inZone z (x', p.2) = true := by
      apply inZone_iff.mpr
      dsimp [x']
      constructor
      · exact Int.le_max_left _ _
      constructor
      · exact Int.max_le.mpr ⟨by omega, by omega⟩
      constructor
      · exact hyl
      constructor
      · exact hyh
      constructor
      · omega
      · omega
    by_cases hxc : x' = z.xl
    · rcases x_boundary_upper (z := z) (x := z.xl) (y := p.2) (Or.inl rfl) (by simpa [hxc] using hxzone) with ⟨v, hv, hvle⟩
      have hp_le' : p.1*p.2 ≤ z.xl*p.2 := by simpa [hxc] using hp_le
      exact ⟨v, hv, Int.le_trans hp_le' hvle⟩
    · have hxd : x' = p.2 + z.dl := by
        have hnot : ¬ p.2 + z.dl ≤ z.xl := by
          intro h
          exact hxc (Int.max_eq_left h)
        have : z.xl ≤ p.2 + z.dl := by omega
        exact Int.max_eq_right this
      rcases d_line_upper (z := z) (d := z.dl) (Or.inl rfl) (by simpa [hxd] using hxzone) with ⟨v, hv, hvle⟩
      exact ⟨v, hv, Int.le_trans hp_le (by simpa [hxd] using hvle)⟩

theorem prod_lower_boundary {z : Zone} {p : Point} (hp : inZone z p = true)
    (hb : (p.1 = z.xl ∨ p.1 = z.xh) ∨ (p.2 = z.yl ∨ p.2 = z.yh)) :
    ∃ v, v ∈ vertices z [] ∧ v.1*v.2 ≤ p.1*p.2 := by
  rcases hb with hx | hy
  · rcases hx with hx | hx
    · exact x_boundary_lower (Or.inl hx) hp
    · exact x_boundary_lower (Or.inr hx) hp
  · rcases hy with hy | hy
    · exact y_boundary_lower (Or.inl hy) hp
    · exact y_boundary_lower (Or.inr hy) hp

theorem rectangle_inZone {z : Zone} {p : Point}
    (hdl : z.dl ≤ z.xl - z.yh) (hdh : z.xh - z.yl ≤ z.dh)
    (hxp : z.xl ≤ p.1 ∧ p.1 ≤ z.xh) (hyp : z.yl ≤ p.2 ∧ p.2 ≤ z.yh) :
    inZone z p = true := by
  apply inZone_iff.mpr
  rcases hxp with ⟨hxl, hxh⟩
  rcases hyp with ⟨hyl, hyh⟩
  refine ⟨hxl, hxh, hyl, hyh, ?_, ?_⟩ <;> omega

theorem prod_lower_rect {z : Zone} {p : Point}
    (hdl : z.dl ≤ z.xl - z.yh) (hdh : z.xh - z.yl ≤ z.dh)
    (hxp : z.xl ≤ p.1 ∧ p.1 ≤ z.xh) (hyp : z.yl ≤ p.2 ∧ p.2 ≤ z.yh) :
    ∃ v, v ∈ vertices z [] ∧ v.1*v.2 ≤ p.1*p.2 := by
  by_cases hypos : 0 ≤ p.2
  · have hxzone : inZone z (z.xl, p.2) = true := rectangle_inZone hdl hdh ⟨Int.le_refl _, by omega⟩ hyp
    have hstep : z.xl*p.2 ≤ p.1*p.2 := Int.mul_le_mul_of_nonneg_right hxp.1 hypos
    rcases x_boundary_lower (z := z) (x := z.xl) (y := p.2) (Or.inl rfl) hxzone with ⟨v, hv, hvle⟩
    exact ⟨v, hv, Int.le_trans hvle hstep⟩
  · have hynonpos : p.2 ≤ 0 := by omega
    have hxzone : inZone z (z.xh, p.2) = true := rectangle_inZone hdl hdh ⟨by omega, Int.le_refl _⟩ hyp
    have hstep : z.xh*p.2 ≤ p.1*p.2 := Int.mul_le_mul_of_nonpos_right hxp.2 hynonpos
    rcases x_boundary_lower (z := z) (x := z.xh) (y := p.2) (Or.inr rfl) hxzone with ⟨v, hv, hvle⟩
    exact ⟨v, hv, Int.le_trans hvle hstep⟩


/-- Rectangle zones have exactly the four corners as their hull. -/
theorem hull_rect {z : Zone}
    (hdl : z.dl ≤ z.xl - z.yh) (hdh : z.xh - z.yl ≤ z.dh) :
    estimate pc 0 z = extrema pc 0 z := by
  classical
  let ds := pc.cuts 0
  by_cases hne : ∃ p, inZone z p = true
  · have hvn : vertices z ds ≠ [] := by
      rcases hne with ⟨p, hp⟩
      rcases exists_vertex_of_inZone (z := z) (ds := ds) hp with ⟨v, hv⟩
      intro hnil
      simp [hnil] at hv
    rcases hm : ((vertices z ds).map (pc.score 0)).min? with _ | lo
    · have hnil : (vertices z ds).map (pc.score 0) = [] := List.min?_eq_none_iff.mp hm
      have hnilv : vertices z ds = [] := vertices_nil_of_map_nil hnil
      exact False.elim (hvn hnilv)
    · rcases hM : ((vertices z ds).map (pc.score 0)).max? with _ | hi
      · have hnil : (vertices z ds).map (pc.score 0) = [] := List.max?_eq_none_iff.mp hM
        have : vertices z ds = [] := by simpa using hnil
        exact False.elim (hvn this)
      · have hest : estimate pc 0 z = some (lo, hi) := by
          simp [estimate, bounds, ds, hm, hM]
        have hmono {v : Point} (hv : v ∈ vertices z []) : v ∈ vertices z ds := by
          apply vertices_mono_ds (z := z) (ds := []) (ds' := ds)
          · intro d hd
            simp at hd
          · exact hv
        have hcov (p : Point) (hp : inZone z p = true) : lo ≤ p.1*p.2 ∧ p.1*p.2 ≤ hi := by
          have hb (v : Int) (hvsc : v ∈ (vertices z ds).map (pc.score 0)) : lo ≤ v ∧ v ≤ hi :=
            bounds_of_mem (by simpa [estimate, ds] using hest) hvsc
          rcases prod_upper hp with ⟨v, hv, hvle⟩
          have hp_rect : z.xl ≤ p.1 ∧ p.1 ≤ z.xh ∧ z.yl ≤ p.2 ∧ p.2 ≤ z.yh := by
            rcases inZone_iff.mp hp with ⟨h1,h2,h3,h4,_,_⟩
            exact ⟨h1,h2,h3,h4⟩
          rcases prod_lower_rect hdl hdh ⟨hp_rect.1, hp_rect.2.1⟩ ⟨hp_rect.2.2.1, hp_rect.2.2.2⟩ with ⟨w, hw, hwle⟩
          have hwsc : pc.score 0 w ∈ (vertices z ds).map (pc.score 0) := List.mem_map.mpr ⟨w, hmono hw, rfl⟩
          have hvsc : pc.score 0 v ∈ (vertices z ds).map (pc.score 0) := List.mem_map.mpr ⟨v, hmono hv, rfl⟩
          have hbw := hb (pc.score 0 w) hwsc
          have hbv := hb (pc.score 0 v) hvsc
          change lo ≤ w.1*w.2 ∧ w.1*w.2 ≤ hi at hbw
          change lo ≤ v.1*v.2 ∧ v.1*v.2 ≤ hi at hbv
          constructor
          · exact Int.le_trans hbw.1 hwle
          · exact Int.le_trans hvle hbv.2
        have hminmem : lo ∈ (points z).map (pc.score 0) := by
          have hvm : lo ∈ (vertices z ds).map (pc.score 0) := (List.min?_eq_some_iff.mp hm).1
          rcases List.mem_map.mp hvm with ⟨v, hv, rfl⟩
          exact List.mem_map.mpr ⟨v, point_mem_points (vertex_inZone hv), rfl⟩
        have hmaxmem : hi ∈ (points z).map (pc.score 0) := by
          have hvm : hi ∈ (vertices z ds).map (pc.score 0) := (List.max?_eq_some_iff.mp hM).1
          rcases List.mem_map.mp hvm with ⟨v, hv, rfl⟩
          exact List.mem_map.mpr ⟨v, point_mem_points (vertex_inZone hv), rfl⟩
        have hext : bounds ((points z).map (pc.score 0)) = some (lo, hi) := by
          apply bounds_eq_minmax
          · exact hminmem
          · intro e he
            rcases List.mem_map.mp he with ⟨p, hp, rfl⟩
            exact (hcov p (List.mem_filter.mp hp).2).1
          · exact hmaxmem
          · intro e he
            rcases List.mem_map.mp he with ⟨p, hp, rfl⟩
            rcases inZone_iff.mp (List.mem_filter.mp hp).2 with ⟨h1,h2,h3,h4,_,_⟩
            have hpz : inZone z p = true := (List.mem_filter.mp hp).2
            rcases prod_upper hpz with ⟨v, hv, hvle⟩
            have hvsc : pc.score 0 v ∈ (vertices z ds).map (pc.score 0) :=
              List.mem_map.mpr ⟨v, hmono hv, rfl⟩
            have hb := bounds_of_mem (by simpa [estimate, ds] using hest) hvsc
            have hvbd : v.1*v.2 ≤ hi := by change pc.score 0 v ≤ hi; exact hb.2
            change p.1*p.2 ≤ hi
            exact Int.le_trans hvle hvbd
        simpa [estimate, extrema, hest, hext]
  · have hest : estimate pc 0 z = none := (deadEdge_general pc 0 z).2 (by
      intro p
      by_cases hp : inZone z p = true
      · exact False.elim (hne ⟨p, hp⟩)
      · simpa using hp)
    have hpnil : points z = [] := by
      apply List.eq_nil_iff_forall_not_mem.mpr
      intro p hp
      exact hne ⟨p, (List.mem_filter.mp hp).2⟩
    change estimate pc 0 z = extrema pc 0 z
    rw [hest]
    simp [extrema, hpnil, bounds]

/-- The upper domination gives the fold-compare property even without a tight lower hull. -/
theorem foldCompare_pc : foldCompare pc := by
  intro t z lo hi lit p he hhi hp
  have hb (v : Int) (hvsc : v ∈ (vertices z (pc.cuts t)).map (pc.score t)) : lo ≤ v ∧ v ≤ hi :=
    bounds_of_mem (by simpa [estimate] using he) hvsc
  rcases prod_upper hp with ⟨v, hv, hvle⟩
  have hv' : v ∈ vertices z (pc.cuts t) :=
    vertices_mono_ds (z := z) (ds := []) (ds' := pc.cuts t) (by intro d hd; simp at hd) hv
  have hvmap : pc.score t v ∈ (vertices z (pc.cuts t)).map (pc.score t) :=
    List.mem_map.mpr ⟨v, hv', rfl⟩
  have hvbd : v.1*v.2 ≤ hi := by change pc.score t v ≤ hi; exact (hb _ hvmap).2
  have hple : pc.score t p ≤ hi := by
    change p.1*p.2 ≤ hi
    exact Int.le_trans hvle hvbd
  omega

/-- Splitting a difference interval never loses a vertex extreme. -/
theorem branchSplit_pc : branchSplit pc := by
  intro z t cut
  let a : Zone := (split z cut).1
  let b : Zone := (split z cut).2
  let sc := pc.score t
  let ds := pc.cuts t
  let vz := (vertices z ds).map sc
  let va := (vertices a ds).map sc
  let vb := (vertices b ds).map sc
  have hmono {w : Zone} {v : Point} (hv : v ∈ vertices w []) : v ∈ vertices w ds := by
    apply vertices_mono_ds (z := w) (ds := []) (ds' := ds)
    · intro d hd
      simp at hd
    · exact hv
  have hzcover : ∀ v, v ∈ vertices z ds → v ∈ vertices a ds ∨ v ∈ vertices b ds := by
    intro v hv
    exact vertices_split_cover (z := z) (ds := ds) (cut := cut) hv
  have hzaZone : ∀ v, v ∈ vertices a ds → inZone z v := by
    intro v hv
    exact inZone_split_fst (vertex_inZone hv)
  have hzbZone : ∀ v, v ∈ vertices b ds → inZone z v := by
    intro v hv
    exact inZone_split_snd (vertex_inZone hv)
  by_cases hzempty : ∀ p, inZone z p = false
  · have hez : estimate pc t z = none := (deadEdge_general pc t z).2 hzempty
    have hea : estimate pc t a = none := (deadEdge_general pc t a).2 (by
      intro p
      by_cases hp : inZone a p = true
      · have hzp := hzempty p
        have hzt : inZone z p = true := inZone_split_fst (z := z) (cut := cut) (p := p) hp
        rw [hzt] at hzp
        contradiction
      · simpa using hp)
    have heb : estimate pc t b = none := (deadEdge_general pc t b).2 (by
      intro p
      by_cases hp : inZone b p = true
      · have hzp := hzempty p
        have hzt : inZone z p = true := inZone_split_snd (z := z) (cut := cut) (p := p) hp
        rw [hzt] at hzp
        contradiction
      · simpa using hp)
    change estimate pc t z = bounds (([estimate pc t a, estimate pc t b].filterMap id).flatMap
      (fun (l,h) => [l,h]))
    simp [hea, heb, hez, bounds]
  · have hpz : ∃ p, inZone z p = true := by
      by_cases hAll : ∃ p, inZone z p = true
      · exact hAll
      · exfalso
        apply hzempty
        intro p
        by_cases hp : inZone z p = true
        · exact False.elim (hAll ⟨p, hp⟩)
        · simpa using hp
    rcases hpz with ⟨p0, hp0⟩
    have hvn : vertices z ds ≠ [] := by
      rcases exists_vertex_of_inZone (z := z) (ds := ds) hp0 with ⟨v, hv⟩
      intro hnil
      simp [hnil] at hv
    rcases hez : estimate pc t z with _ | ⟨lo, hi⟩
    · have hnil : (vertices z ds).map sc = [] := by
        apply bounds_eq_none.mp
        simpa [estimate] using hez
      have hnilv : vertices z ds = [] := vertices_nil_of_map_nil hnil
      exact False.elim (hvn hnilv)
    · have hzv : bounds vz = some (lo,hi) := by simpa [vz, estimate] using hez
      have hminmem : lo ∈ vz := by
        simpa [vz, estimate] using bounds_min_mem (vs := vz) hzv
      have hmaxmem : hi ∈ vz := by
        simpa [vz, estimate] using bounds_max_mem (vs := vz) hzv
      have hcov_vertex : ∀ p, inZone z p = true →
          ((p.1 = z.xl ∨ p.1 = z.xh) ∨ (p.2 = z.yl ∨ p.2 = z.yh)) →
          lo ≤ sc p ∧ sc p ≤ hi := by
        intro p hp hbnd
        have hb (v : Int) (hvsc : v ∈ vz) : lo ≤ v ∧ v ≤ hi := bounds_of_mem hzv hvsc
        rcases prod_lower_boundary hp hbnd with ⟨w, hw, hwle⟩
        rcases prod_upper hp with ⟨v, hv, hvle⟩
        have hwmap : sc w ∈ vz := List.mem_map.mpr ⟨w, hmono hw, rfl⟩
        have hvmap : sc v ∈ vz := List.mem_map.mpr ⟨v, hmono hv, rfl⟩
        constructor
        · have hwbd : w.1*w.2 ≤ sc p := by change w.1*w.2 ≤ p.1*p.2; exact hwle
          have hwbd' : lo ≤ w.1*w.2 := by change lo ≤ sc w; exact (hb _ hwmap).1
          exact Int.le_trans hwbd' hwbd
        · have hvbd : sc p ≤ v.1*v.2 := by change p.1*p.2 ≤ v.1*v.2; exact hvle
          have hvbd' : v.1*v.2 ≤ hi := by change sc v ≤ hi; exact (hb _ hvmap).2
          exact Int.le_trans hvbd hvbd'
      have havb : bounds (va ++ vb) = some (lo,hi) := by
        apply bounds_eq_minmax
        · rcases List.mem_map.mp hminmem with ⟨v, hv, hvlo⟩
          rcases hzcover v hv with hva | hvb
          · exact List.mem_append.mpr (Or.inl (List.mem_map.mpr ⟨v, hva, hvlo⟩))
          · exact List.mem_append.mpr (Or.inr (List.mem_map.mpr ⟨v, hvb, hvlo⟩))
        · intro e he
          simp at he
          rcases he with he | he
          · rcases List.mem_map.mp he with ⟨v, hva, rfl⟩
            exact (hcov_vertex v (hzaZone v hva)
              (by simpa [a, split] using vertex_on_xy_boundary (z := a) (ds := ds) hva)).1
          · rcases List.mem_map.mp he with ⟨v, hvb, rfl⟩
            exact (hcov_vertex v (hzbZone v hvb)
              (by simpa [b, split] using vertex_on_xy_boundary (z := b) (ds := ds) hvb)).1
        · rcases List.mem_map.mp hmaxmem with ⟨v, hv, hvhi⟩
          rcases hzcover v hv with hva | hvb
          · exact List.mem_append.mpr (Or.inl (List.mem_map.mpr ⟨v, hva, hvhi⟩))
          · exact List.mem_append.mpr (Or.inr (List.mem_map.mpr ⟨v, hvb, hvhi⟩))
        · intro e he
          simp at he
          rcases he with he | he
          · rcases List.mem_map.mp he with ⟨v, hva, rfl⟩
            exact (hcov_vertex v (hzaZone v hva)
              (by simpa [a, split] using vertex_on_xy_boundary (z := a) (ds := ds) hva)).2
          · rcases List.mem_map.mp he with ⟨v, hvb, rfl⟩
            exact (hcov_vertex v (hzbZone v hvb)
              (by simpa [b, split] using vertex_on_xy_boundary (z := b) (ds := ds) hvb)).2
      exact havb.symm.trans (bounds_estimates_concat pc t z cut).symm

end TinylambOpt.ResearchProofs.JitFrontiers.RelationalGuards.ProductCornersLemmas
