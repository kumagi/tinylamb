import TinylambOpt.ResearchProofs.JitFrontiers.RelationalGuards.Common

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitFrontiers.RelationalGuards.DoubleMinusLemmas

open TinylambOpt.ResearchGoals.JitFrontiers.RelationalGuards
open TinylambOpt.ResearchProofs.JitFrontiers.RelationalGuards.Common

abbrev dm := TinylambOpt.ResearchGoals.JitFrontiers.RelationalGuards.DoubleMinus.expr

theorem dm_dom_hi (t : Int) {z : Zone} {p : Point} (hp : inZone z p = true) :
    ∃ v, v ∈ vertices z [] ∧ dm.score t p ≤ dm.score t v := by
  simp [inZone] at hp
  let C (x y : Int) : Prop := z.xl ≤ x ∧ x ≤ z.xh ∧ z.yl ≤ y ∧ y ≤ z.yh ∧
    z.dl ≤ x-y ∧ x-y ≤ z.dh ∧ 2*p.1-p.2+t ≤ 2*x-y+t
  have hsel : C z.xl z.yl ∨ C z.xl z.yh ∨ C z.xh z.yl ∨ C z.xh z.yh ∨
      C z.xl (z.xl-z.dl) ∨ C z.xl (z.xl-z.dh) ∨
      C z.xh (z.xh-z.dl) ∨ C z.xh (z.xh-z.dh) ∨
      C (z.yl+z.dl) z.yl ∨ C (z.yl+z.dh) z.yl ∨
      C (z.yh+z.dl) z.yh ∨ C (z.yh+z.dh) z.yh := by
    dsimp [C]
    omega
  rcases hsel with h | h | h | h | h | h | h | h | h | h | h | h
  · rcases h with ⟨hx1,hx2,hy1,hy2,hd1,hd2,hs⟩
    refine ⟨(z.xl,z.yl), ?_, ?_⟩
    · apply mem_vertices_corner (z := z) (ds := []) (Or.inl rfl : z.xl = z.xl ∨ z.xl = z.xh) (Or.inl rfl : z.yl = z.yl ∨ z.yl = z.yh)
      apply inZone_iff.mpr
      dsimp
      omega
    · change 2*p.1-p.2+t ≤ 2*z.xl-z.yl+t
      omega
  · rcases h with ⟨hx1,hx2,hy1,hy2,hd1,hd2,hs⟩
    refine ⟨(z.xl,z.yh), ?_, ?_⟩
    · apply mem_vertices_corner (z := z) (ds := []) (Or.inl rfl : z.xl = z.xl ∨ z.xl = z.xh) (Or.inr rfl : z.yh = z.yl ∨ z.yh = z.yh)
      apply inZone_iff.mpr
      dsimp
      omega
    · change 2*p.1-p.2+t ≤ 2*z.xl-z.yh+t
      omega
  · rcases h with ⟨hx1,hx2,hy1,hy2,hd1,hd2,hs⟩
    refine ⟨(z.xh,z.yl), ?_, ?_⟩
    · apply mem_vertices_corner (z := z) (ds := []) (Or.inr rfl : z.xh = z.xl ∨ z.xh = z.xh) (Or.inl rfl : z.yl = z.yl ∨ z.yl = z.yh)
      apply inZone_iff.mpr
      dsimp
      omega
    · change 2*p.1-p.2+t ≤ 2*z.xh-z.yl+t
      omega
  · rcases h with ⟨hx1,hx2,hy1,hy2,hd1,hd2,hs⟩
    refine ⟨(z.xh,z.yh), ?_, ?_⟩
    · apply mem_vertices_corner (z := z) (ds := []) (Or.inr rfl : z.xh = z.xl ∨ z.xh = z.xh) (Or.inr rfl : z.yh = z.yl ∨ z.yh = z.yh)
      apply inZone_iff.mpr
      dsimp
      omega
    · change 2*p.1-p.2+t ≤ 2*z.xh-z.yh+t
      omega
  · rcases h with ⟨hx1,hx2,hy1,hy2,hd1,hd2,hs⟩
    refine ⟨(z.xl,z.xl-z.dl), ?_, ?_⟩
    · apply mem_vertices_of_x (z := z) (ds := []) (x := z.xl) (d := z.dl) (Or.inl rfl : z.xl = z.xl ∨ z.xl = z.xh) (Or.inl rfl : z.dl = z.dl ∨ z.dl = z.dh ∨ z.dl ∈ ([] : List Int))
      apply inZone_iff.mpr
      dsimp
      omega
    · change 2*p.1-p.2+t ≤ 2*z.xl-(z.xl-z.dl)+t
      omega
  · rcases h with ⟨hx1,hx2,hy1,hy2,hd1,hd2,hs⟩
    refine ⟨(z.xl,z.xl-z.dh), ?_, ?_⟩
    · apply mem_vertices_of_x (z := z) (ds := []) (x := z.xl) (d := z.dh) (Or.inl rfl : z.xl = z.xl ∨ z.xl = z.xh) (Or.inr (Or.inl rfl) : z.dh = z.dl ∨ z.dh = z.dh ∨ z.dh ∈ ([] : List Int))
      apply inZone_iff.mpr
      dsimp
      omega
    · change 2*p.1-p.2+t ≤ 2*z.xl-(z.xl-z.dh)+t
      omega
  · rcases h with ⟨hx1,hx2,hy1,hy2,hd1,hd2,hs⟩
    refine ⟨(z.xh,z.xh-z.dl), ?_, ?_⟩
    · apply mem_vertices_of_x (z := z) (ds := []) (x := z.xh) (d := z.dl) (Or.inr rfl : z.xh = z.xl ∨ z.xh = z.xh) (Or.inl rfl : z.dl = z.dl ∨ z.dl = z.dh ∨ z.dl ∈ ([] : List Int))
      apply inZone_iff.mpr
      dsimp
      omega
    · change 2*p.1-p.2+t ≤ 2*z.xh-(z.xh-z.dl)+t
      omega
  · rcases h with ⟨hx1,hx2,hy1,hy2,hd1,hd2,hs⟩
    refine ⟨(z.xh,z.xh-z.dh), ?_, ?_⟩
    · apply mem_vertices_of_x (z := z) (ds := []) (x := z.xh) (d := z.dh) (Or.inr rfl : z.xh = z.xl ∨ z.xh = z.xh) (Or.inr (Or.inl rfl) : z.dh = z.dl ∨ z.dh = z.dh ∨ z.dh ∈ ([] : List Int))
      apply inZone_iff.mpr
      dsimp
      omega
    · change 2*p.1-p.2+t ≤ 2*z.xh-(z.xh-z.dh)+t
      omega
  · rcases h with ⟨hx1,hx2,hy1,hy2,hd1,hd2,hs⟩
    refine ⟨(z.yl+z.dl,z.yl), ?_, ?_⟩
    · apply mem_vertices_of_y (z := z) (ds := []) (y := z.yl) (d := z.dl) (Or.inl rfl : z.yl = z.yl ∨ z.yl = z.yh) (Or.inl rfl : z.dl = z.dl ∨ z.dl = z.dh ∨ z.dl ∈ ([] : List Int))
      apply inZone_iff.mpr
      dsimp
      omega
    · change 2*p.1-p.2+t ≤ 2*(z.yl+z.dl)-z.yl+t
      omega
  · rcases h with ⟨hx1,hx2,hy1,hy2,hd1,hd2,hs⟩
    refine ⟨(z.yl+z.dh,z.yl), ?_, ?_⟩
    · apply mem_vertices_of_y (z := z) (ds := []) (y := z.yl) (d := z.dh) (Or.inl rfl : z.yl = z.yl ∨ z.yl = z.yh) (Or.inr (Or.inl rfl) : z.dh = z.dl ∨ z.dh = z.dh ∨ z.dh ∈ ([] : List Int))
      apply inZone_iff.mpr
      dsimp
      omega
    · change 2*p.1-p.2+t ≤ 2*(z.yl+z.dh)-z.yl+t
      omega
  · rcases h with ⟨hx1,hx2,hy1,hy2,hd1,hd2,hs⟩
    refine ⟨(z.yh+z.dl,z.yh), ?_, ?_⟩
    · apply mem_vertices_of_y (z := z) (ds := []) (y := z.yh) (d := z.dl) (Or.inr rfl : z.yh = z.yl ∨ z.yh = z.yh) (Or.inl rfl : z.dl = z.dl ∨ z.dl = z.dh ∨ z.dl ∈ ([] : List Int))
      apply inZone_iff.mpr
      dsimp
      omega
    · change 2*p.1-p.2+t ≤ 2*(z.yh+z.dl)-z.yh+t
      omega
  · rcases h with ⟨hx1,hx2,hy1,hy2,hd1,hd2,hs⟩
    refine ⟨(z.yh+z.dh,z.yh), ?_, ?_⟩
    · apply mem_vertices_of_y (z := z) (ds := []) (y := z.yh) (d := z.dh) (Or.inr rfl : z.yh = z.yl ∨ z.yh = z.yh) (Or.inr (Or.inl rfl) : z.dh = z.dl ∨ z.dh = z.dh ∨ z.dh ∈ ([] : List Int))
      apply inZone_iff.mpr
      dsimp
      omega
    · change 2*p.1-p.2+t ≤ 2*(z.yh+z.dh)-z.yh+t
      omega

theorem dm_dom_lo (t : Int) {z : Zone} {p : Point} (hp : inZone z p = true) :
    ∃ v, v ∈ vertices z [] ∧ dm.score t v ≤ dm.score t p := by
  simp [inZone] at hp
  let C (x y : Int) : Prop := z.xl ≤ x ∧ x ≤ z.xh ∧ z.yl ≤ y ∧ y ≤ z.yh ∧
    z.dl ≤ x-y ∧ x-y ≤ z.dh ∧ 2*x-y+t ≤ 2*p.1-p.2+t
  have hsel : C z.xl z.yl ∨ C z.xl z.yh ∨ C z.xh z.yl ∨ C z.xh z.yh ∨
      C z.xl (z.xl-z.dl) ∨ C z.xl (z.xl-z.dh) ∨
      C z.xh (z.xh-z.dl) ∨ C z.xh (z.xh-z.dh) ∨
      C (z.yl+z.dl) z.yl ∨ C (z.yl+z.dh) z.yl ∨
      C (z.yh+z.dl) z.yh ∨ C (z.yh+z.dh) z.yh := by
    dsimp [C]
    omega
  rcases hsel with h | h | h | h | h | h | h | h | h | h | h | h
  · rcases h with ⟨hx1,hx2,hy1,hy2,hd1,hd2,hs⟩
    refine ⟨(z.xl,z.yl), ?_, ?_⟩
    · apply mem_vertices_corner (z := z) (ds := []) (Or.inl rfl : z.xl = z.xl ∨ z.xl = z.xh) (Or.inl rfl : z.yl = z.yl ∨ z.yl = z.yh)
      apply inZone_iff.mpr
      dsimp
      omega
    · change 2*z.xl-z.yl+t ≤ 2*p.1-p.2+t
      omega
  · rcases h with ⟨hx1,hx2,hy1,hy2,hd1,hd2,hs⟩
    refine ⟨(z.xl,z.yh), ?_, ?_⟩
    · apply mem_vertices_corner (z := z) (ds := []) (Or.inl rfl : z.xl = z.xl ∨ z.xl = z.xh) (Or.inr rfl : z.yh = z.yl ∨ z.yh = z.yh)
      apply inZone_iff.mpr
      dsimp
      omega
    · change 2*z.xl-z.yh+t ≤ 2*p.1-p.2+t
      omega
  · rcases h with ⟨hx1,hx2,hy1,hy2,hd1,hd2,hs⟩
    refine ⟨(z.xh,z.yl), ?_, ?_⟩
    · apply mem_vertices_corner (z := z) (ds := []) (Or.inr rfl : z.xh = z.xl ∨ z.xh = z.xh) (Or.inl rfl : z.yl = z.yl ∨ z.yl = z.yh)
      apply inZone_iff.mpr
      dsimp
      omega
    · change 2*z.xh-z.yl+t ≤ 2*p.1-p.2+t
      omega
  · rcases h with ⟨hx1,hx2,hy1,hy2,hd1,hd2,hs⟩
    refine ⟨(z.xh,z.yh), ?_, ?_⟩
    · apply mem_vertices_corner (z := z) (ds := []) (Or.inr rfl : z.xh = z.xl ∨ z.xh = z.xh) (Or.inr rfl : z.yh = z.yl ∨ z.yh = z.yh)
      apply inZone_iff.mpr
      dsimp
      omega
    · change 2*z.xh-z.yh+t ≤ 2*p.1-p.2+t
      omega
  · rcases h with ⟨hx1,hx2,hy1,hy2,hd1,hd2,hs⟩
    refine ⟨(z.xl,z.xl-z.dl), ?_, ?_⟩
    · apply mem_vertices_of_x (z := z) (ds := []) (x := z.xl) (d := z.dl) (Or.inl rfl : z.xl = z.xl ∨ z.xl = z.xh) (Or.inl rfl : z.dl = z.dl ∨ z.dl = z.dh ∨ z.dl ∈ ([] : List Int))
      apply inZone_iff.mpr
      dsimp
      omega
    · change 2*z.xl-(z.xl-z.dl)+t ≤ 2*p.1-p.2+t
      omega
  · rcases h with ⟨hx1,hx2,hy1,hy2,hd1,hd2,hs⟩
    refine ⟨(z.xl,z.xl-z.dh), ?_, ?_⟩
    · apply mem_vertices_of_x (z := z) (ds := []) (x := z.xl) (d := z.dh) (Or.inl rfl : z.xl = z.xl ∨ z.xl = z.xh) (Or.inr (Or.inl rfl) : z.dh = z.dl ∨ z.dh = z.dh ∨ z.dh ∈ ([] : List Int))
      apply inZone_iff.mpr
      dsimp
      omega
    · change 2*z.xl-(z.xl-z.dh)+t ≤ 2*p.1-p.2+t
      omega
  · rcases h with ⟨hx1,hx2,hy1,hy2,hd1,hd2,hs⟩
    refine ⟨(z.xh,z.xh-z.dl), ?_, ?_⟩
    · apply mem_vertices_of_x (z := z) (ds := []) (x := z.xh) (d := z.dl) (Or.inr rfl : z.xh = z.xl ∨ z.xh = z.xh) (Or.inl rfl : z.dl = z.dl ∨ z.dl = z.dh ∨ z.dl ∈ ([] : List Int))
      apply inZone_iff.mpr
      dsimp
      omega
    · change 2*z.xh-(z.xh-z.dl)+t ≤ 2*p.1-p.2+t
      omega
  · rcases h with ⟨hx1,hx2,hy1,hy2,hd1,hd2,hs⟩
    refine ⟨(z.xh,z.xh-z.dh), ?_, ?_⟩
    · apply mem_vertices_of_x (z := z) (ds := []) (x := z.xh) (d := z.dh) (Or.inr rfl : z.xh = z.xl ∨ z.xh = z.xh) (Or.inr (Or.inl rfl) : z.dh = z.dl ∨ z.dh = z.dh ∨ z.dh ∈ ([] : List Int))
      apply inZone_iff.mpr
      dsimp
      omega
    · change 2*z.xh-(z.xh-z.dh)+t ≤ 2*p.1-p.2+t
      omega
  · rcases h with ⟨hx1,hx2,hy1,hy2,hd1,hd2,hs⟩
    refine ⟨(z.yl+z.dl,z.yl), ?_, ?_⟩
    · apply mem_vertices_of_y (z := z) (ds := []) (y := z.yl) (d := z.dl) (Or.inl rfl : z.yl = z.yl ∨ z.yl = z.yh) (Or.inl rfl : z.dl = z.dl ∨ z.dl = z.dh ∨ z.dl ∈ ([] : List Int))
      apply inZone_iff.mpr
      dsimp
      omega
    · change 2*(z.yl+z.dl)-z.yl+t ≤ 2*p.1-p.2+t
      omega
  · rcases h with ⟨hx1,hx2,hy1,hy2,hd1,hd2,hs⟩
    refine ⟨(z.yl+z.dh,z.yl), ?_, ?_⟩
    · apply mem_vertices_of_y (z := z) (ds := []) (y := z.yl) (d := z.dh) (Or.inl rfl : z.yl = z.yl ∨ z.yl = z.yh) (Or.inr (Or.inl rfl) : z.dh = z.dl ∨ z.dh = z.dh ∨ z.dh ∈ ([] : List Int))
      apply inZone_iff.mpr
      dsimp
      omega
    · change 2*(z.yl+z.dh)-z.yl+t ≤ 2*p.1-p.2+t
      omega
  · rcases h with ⟨hx1,hx2,hy1,hy2,hd1,hd2,hs⟩
    refine ⟨(z.yh+z.dl,z.yh), ?_, ?_⟩
    · apply mem_vertices_of_y (z := z) (ds := []) (y := z.yh) (d := z.dl) (Or.inr rfl : z.yh = z.yl ∨ z.yh = z.yh) (Or.inl rfl : z.dl = z.dl ∨ z.dl = z.dh ∨ z.dl ∈ ([] : List Int))
      apply inZone_iff.mpr
      dsimp
      omega
    · change 2*(z.yh+z.dl)-z.yh+t ≤ 2*p.1-p.2+t
      omega
  · rcases h with ⟨hx1,hx2,hy1,hy2,hd1,hd2,hs⟩
    refine ⟨(z.yh+z.dh,z.yh), ?_, ?_⟩
    · apply mem_vertices_of_y (z := z) (ds := []) (y := z.yh) (d := z.dh) (Or.inr rfl : z.yh = z.yl ∨ z.yh = z.yh) (Or.inr (Or.inl rfl) : z.dh = z.dl ∨ z.dh = z.dh ∨ z.dh ∈ ([] : List Int))
      apply inZone_iff.mpr
      dsimp
      omega
    · change 2*(z.yh+z.dh)-z.yh+t ≤ 2*p.1-p.2+t
      omega

theorem minimalHull_dm : minimalHull dm := minimalHull_of_dom dm_dom_hi dm_dom_lo

theorem coverage_dm : ∀ t z p, inZone z p = true → ∀ lo hi, estimate dm t z = some (lo,hi) →
    lo ≤ dm.score t p ∧ dm.score t p ≤ hi := coverage_of_dom dm_dom_hi dm_dom_lo
theorem biasFold_dm : ∀ z t l h, estimate dm 0 z = some (l,h) → estimate dm t z = some (l+t,h+t) := by
  intro z t l h he0
  classical
  by_cases hne : ∃ p, inZone z p = true
  · have hc0 : dm.cuts 0 = [] := by rfl
    have hct : dm.cuts t = [] := by rfl
    have hvt : ∀ v ∈ vertices z (dm.cuts t), dm.score t v = dm.score 0 v + t := by
      intro v hv
      change 2*v.1 - v.2 + t = (2*v.1 - v.2 + 0) + t
      omega
    have hmap0 : (vertices z (dm.cuts 0)).map (dm.score 0) ≠ [] := by
      rcases hne with ⟨p, hp⟩
      rcases exists_vertex_of_inZone (z := z) (ds := dm.cuts 0) hp with ⟨v, hv⟩
      intro hn
      have : dm.score 0 v ∈ (vertices z (dm.cuts 0)).map (dm.score 0) := List.mem_map.mpr ⟨v, hv, rfl⟩
      simp [hn] at this
    rcases hmin0 : ((vertices z (dm.cuts 0)).map (dm.score 0)).min? with _ | lo0
    · have hnil : ((vertices z (dm.cuts 0)).map (dm.score 0)) = [] := List.min?_eq_none_iff.mp hmin0
      exact False.elim (hmap0 hnil)
    · rcases hmax0 : ((vertices z (dm.cuts 0)).map (dm.score 0)).max? with _ | hi0
      · have hnil : ((vertices z (dm.cuts 0)).map (dm.score 0)) = [] := List.max?_eq_none_iff.mp hmax0
        exact False.elim (hmap0 hnil)
      · have he0'' : bounds ((vertices z (dm.cuts 0)).map (dm.score 0)) = some (lo0,hi0) := by
          simp [bounds, hmin0, hmax0]
        have hl0 : l = lo0 := by
          have hx : estimate dm 0 z = some (lo0,hi0) := by simpa [estimate] using he0''
          rw [he0] at hx
          cases hx
          rfl
        have hh0 : h = hi0 := by
          have hx : estimate dm 0 z = some (lo0,hi0) := by simpa [estimate] using he0''
          rw [he0] at hx
          cases hx
          rfl
        have hmint : ((vertices z (dm.cuts t)).map (dm.score t)).min? = some (lo0+t) := by
          apply List.min?_eq_some_iff.mpr
          constructor
          · have hm := (List.min?_eq_some_iff.mp hmin0).1
            rcases List.mem_map.mp hm with ⟨v, hv, rfl⟩
            refine List.mem_map.mpr ⟨v, ?_, ?_⟩
            · simpa [hc0, hct] using hv
            · exact hvt v (by simpa [hc0, hct] using hv)
          · intro e he
            rcases List.mem_map.mp he with ⟨v, hv, rfl⟩
            have hv0 : v ∈ vertices z (dm.cuts 0) := by simpa [hc0, hct] using hv
            have hb := bounds_of_mem he0'' (List.mem_map.mpr ⟨v, hv0, rfl⟩)
            rw [hvt v hv]
            omega
        have hmaxt : ((vertices z (dm.cuts t)).map (dm.score t)).max? = some (hi0+t) := by
          apply List.max?_eq_some_iff.mpr
          constructor
          · have hm := (List.max?_eq_some_iff.mp hmax0).1
            rcases List.mem_map.mp hm with ⟨v, hv, rfl⟩
            refine List.mem_map.mpr ⟨v, ?_, ?_⟩
            · simpa [hc0, hct] using hv
            · exact hvt v (by simpa [hc0, hct] using hv)
          · intro e he
            rcases List.mem_map.mp he with ⟨v, hv, rfl⟩
            have hv0 : v ∈ vertices z (dm.cuts 0) := by simpa [hc0, hct] using hv
            have hb := bounds_of_mem he0'' (List.mem_map.mpr ⟨v, hv0, rfl⟩)
            rw [hvt v hv]
            omega
        have hest : estimate dm t z = some (lo0+t, hi0+t) := by
          simp [estimate, bounds, hmint, hmaxt]
        rw [← hl0, ← hh0] at hest
        exact hest
  · have he0none : estimate dm 0 z = none := (deadEdge_general dm 0 z).2 (by
      intro p
      by_cases hp : inZone z p = true
      · exact False.elim (hne ⟨p, hp⟩)
      · simpa using hp)
    rw [he0none] at he0
    contradiction

end TinylambOpt.ResearchProofs.JitFrontiers.RelationalGuards.DoubleMinusLemmas
