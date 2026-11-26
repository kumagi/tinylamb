import TinylambOpt.ResearchGoals.JitFrontiers.RelationalGuards

set_option maxHeartbeats 0
set_option maxRecDepth 10000


set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitFrontiers.RelationalGuards.Common_PositiveExcess_FoldCompare_PositiveExcess_FoldCompare

open TinylambOpt.ResearchGoals.JitFrontiers.RelationalGuards
open TinylambOpt.ResearchGoals.JitFrontiers.CheckedRewrites (fits)

theorem bounds_eq_none {vs : List Int} : bounds vs = none ↔ vs = [] := by
  cases vs with
  | nil => simp [bounds]
  | cons a as => simp [bounds]

theorem bounds_eq_some_iff {vs : List Int} {lo hi : Int} :
    bounds vs = some (lo, hi) ↔ vs.min? = some lo ∧ vs.max? = some hi := by
  constructor
  · intro h
    cases hmin : vs.min? with
    | none => simp [bounds, hmin] at h
    | some a =>
      cases hmax : vs.max? with
      | none => simp [bounds, hmin, hmax] at h
      | some b =>
        have hh : (a, b) = (lo, hi) := by simpa [bounds, hmin, hmax] using h
        have hlo : a = lo := congrArg Prod.fst hh
        have hhi : b = hi := congrArg Prod.snd hh
        exact ⟨by simp [hlo], by simp [hhi]⟩
  · rintro ⟨hmin, hmax⟩
    simp [bounds, hmin, hmax]

theorem bounds_of_mem {vs : List Int} {lo hi v : Int}
    (h : bounds vs = some (lo, hi)) (hv : v ∈ vs) : lo ≤ v ∧ v ≤ hi := by
  rcases (bounds_eq_some_iff.mp h) with ⟨hmin, hmax⟩
  exact ⟨(List.min?_eq_some_iff.mp hmin).2 v hv, (List.max?_eq_some_iff.mp hmax).2 v hv⟩

theorem bounds_min_mem {vs : List Int} {lo hi : Int}
    (h : bounds vs = some (lo, hi)) : lo ∈ vs := by
  rcases (bounds_eq_some_iff.mp h) with ⟨hmin, hmax⟩
  exact (List.min?_eq_some_iff.mp hmin).1

theorem bounds_max_mem {vs : List Int} {lo hi : Int}
    (h : bounds vs = some (lo, hi)) : hi ∈ vs := by
  rcases (bounds_eq_some_iff.mp h) with ⟨hmin, hmax⟩
  exact (List.max?_eq_some_iff.mp hmax).1

theorem inZone_iff {z : Zone} {p : Point} :
    inZone z p = true ↔ z.xl ≤ p.1 ∧ p.1 ≤ z.xh ∧ z.yl ≤ p.2 ∧ p.2 ≤ z.yh ∧ z.dl ≤ p.1-p.2 ∧ p.1-p.2 ≤ z.dh := by
  constructor <;> intro h <;> simp [inZone] at h ⊢ <;> omega

theorem mem_interval {a b i : Int} : i ∈ interval a b ↔ a ≤ i ∧ i ≤ b := by
  constructor
  · intro h
    simp [interval] at h
    rcases h with ⟨n, hn, rfl⟩
    constructor <;> omega
  · intro h
    simp [interval]
    refine ⟨(i - a).toNat, ?_, ?_⟩
    · rw [Int.toNat_of_nonneg (by omega : 0 ≤ i - a)]
      omega
    · rw [Int.toNat_of_nonneg (by omega : 0 ≤ i - a)]
      omega

theorem point_mem_points {z : Zone} {p : Point} (hp : inZone z p = true) : p ∈ points z := by
  simp only [points, List.mem_filter, List.mem_flatMap, List.mem_map]
  have h := inZone_iff.mp hp
  refine ⟨⟨p.1, mem_interval.mpr ⟨h.1, h.2.1⟩, p.2,
    mem_interval.mpr ⟨h.2.2.1, h.2.2.2.1⟩, rfl⟩, hp⟩

theorem vertex_inZone {z : Zone} {ds : List Int} {p : Point}
    (hp : p ∈ vertices z ds) : inZone z p = true := by
  rw [vertices] at hp
  exact (List.mem_filter.mp hp).2

theorem mem_vertices_of_x {z : Zone} {ds : List Int} {x d : Int}
    (hx : x = z.xl ∨ x = z.xh) (hd : d = z.dl ∨ d = z.dh ∨ d ∈ ds)
    (hz : inZone z (x, x-d) = true) : (x, x-d) ∈ vertices z ds := by
  rw [vertices]
  apply List.mem_filter.mpr
  constructor
  · rcases hx with rfl | rfl
    · rcases hd with rfl | rfl | hmem
      · simp
      · simp
      · simp [hmem]
    · rcases hd with rfl | rfl | hmem
      · simp
      · simp
      · simp [hmem]
  · exact hz

theorem mem_vertices_of_y {z : Zone} {ds : List Int} {y d : Int}
    (hy : y = z.yl ∨ y = z.yh) (hd : d = z.dl ∨ d = z.dh ∨ d ∈ ds)
    (hz : inZone z (y+d, y) = true) : (y+d, y) ∈ vertices z ds := by
  rw [vertices]
  apply List.mem_filter.mpr
  constructor
  · rcases hy with rfl | rfl
    · rcases hd with rfl | rfl | hmem
      · simp
      · simp
      · simp [hmem]
    · rcases hd with rfl | rfl | hmem
      · simp
      · simp
      · simp [hmem]
  · exact hz

theorem mem_vertices_corner {z : Zone} {ds : List Int} {x y : Int}
    (hx : x = z.xl ∨ x = z.xh) (hy : y = z.yl ∨ y = z.yh)
    (hz : inZone z (x,y) = true) : (x,y) ∈ vertices z ds := by
  rw [vertices]
  apply List.mem_filter.mpr
  constructor
  · rcases hx with rfl | rfl <;> rcases hy with rfl | rfl <;> simp
  · exact hz

theorem exists_vertex_of_inZone {z : Zone} {ds : List Int} {p : Point}
    (hp : inZone z p = true) : ∃ v, v ∈ vertices z ds := by
  rcases inZone_iff.mp hp with ⟨hxl, hxh, hyl, hyh, hdlp, hdhp⟩
  by_cases hA : z.dl < z.xl - z.yh
  · refine ⟨(z.xl, z.yh), ?_⟩
    apply mem_vertices_corner (z := z) (ds := ds) (x := z.xl) (y := z.yh) (Or.inl rfl) (Or.inr rfl)
    apply inZone_iff.mpr
    dsimp
    omega
  · have hB : z.dl ≥ z.xl - z.yh := by omega
    by_cases hB1 : z.xl - z.dl ≤ z.yl
    · refine ⟨(z.yl + z.dl, z.yl), ?_⟩
      apply mem_vertices_of_y (z := z) (ds := ds) (y := z.yl) (d := z.dl) (Or.inl rfl) (Or.inl rfl)
      apply inZone_iff.mpr
      dsimp
      omega
    · refine ⟨(z.xl, z.xl - z.dl), ?_⟩
      apply mem_vertices_of_x (z := z) (ds := ds) (x := z.xl) (d := z.dl) (Or.inl rfl) (Or.inl rfl)
      apply inZone_iff.mpr
      dsimp
      omega

theorem deadEdge_general (f : Transfer) : deadEdge f := by
  intro t z
  constructor
  · intro he p
    have hnil : (vertices z (f.cuts t)).map (f.score t) = [] := by
      apply bounds_eq_none.mp
      simpa [estimate] using he
    have hvnil : vertices z (f.cuts t) = [] := by
      simpa using hnil
    by_cases hpz : inZone z p = true
    · rcases exists_vertex_of_inZone (z := z) (ds := f.cuts t) (p := p) hpz with ⟨v, hv⟩
      simp [hvnil] at hv
    · simpa using hpz
  · intro h
    have hvnil : vertices z (f.cuts t) = [] := by
      apply List.eq_nil_iff_forall_not_mem.mpr
      intro v hv
      have hz := vertex_inZone hv
      have hh := h v
      simp [hz] at hh
    simp [estimate, hvnil, bounds]

theorem bounds_eq_of_same_minmax {vs ws : List Int} {lo hi : Int}
    (hv : bounds vs = some (lo, hi))
    (hmemvw : ∀ v, v ∈ vs → v ∈ ws)
    (hmemwv : ∀ v, v ∈ ws → v ∈ vs) :
    bounds ws = some (lo, hi) := by
  rcases (bounds_eq_some_iff.mp hv) with ⟨hmin, hmax⟩
  apply bounds_eq_some_iff.mpr
  constructor
  · apply List.min?_eq_some_iff.mpr
    constructor
    · exact hmemvw _ ((List.min?_eq_some_iff.mp hmin).1)
    · intro b hb
      exact (List.min?_eq_some_iff.mp hmin).2 b (hmemwv b hb)
  · apply List.max?_eq_some_iff.mpr
    constructor
    · exact hmemvw _ ((List.max?_eq_some_iff.mp hmax).1)
    · intro b hb
      exact (List.max?_eq_some_iff.mp hmax).2 b (hmemwv b hb)

theorem extrema_covers {f : Transfer} {t : Int} {z : Zone} {p : Point}
    (hp : inZone z p = true) {lo hi : Int}
    (he : extrema f t z = some (lo, hi)) : lo ≤ f.score t p ∧ f.score t p ≤ hi := by
  apply bounds_of_mem he
  exact List.mem_map.mpr ⟨p, point_mem_points hp, rfl⟩

theorem covers_of_estimate_eq {f : Transfer} {t : Int} {z : Zone} {p : Point}
    (hp : inZone z p = true) {lo hi : Int}
    (he : estimate f t z = some (lo, hi))
    (hEq : estimate f t z = extrema f t z) :
    lo ≤ f.score t p ∧ f.score t p ≤ hi := by
  apply extrema_covers hp
  rw [← hEq]
  exact he

theorem omitOverflow_of_covers {f : Transfer}
    (hc : ∀ t z p, inZone z p = true → ∀ lo hi, estimate f t z = some (lo,hi) →
      lo ≤ f.score t p ∧ f.score t p ≤ hi) : omitOverflow f := by
  intro t z w lo hi p he hlo hhi hp
  have h := hc t z p hp lo hi he
  simp [fits] at hlo hhi ⊢
  omega

theorem foldCompare_of_covers {f : Transfer}
    (hc : ∀ t z p, inZone z p = true → ∀ lo hi, estimate f t z = some (lo,hi) →
      lo ≤ f.score t p ∧ f.score t p ≤ hi) : foldCompare f := by
  intro t z lo hi lit p he hhi hp
  have h := hc t z p hp lo hi he
  omega

theorem phiHull_of_covers {f : Transfer}
    (hc : ∀ t z p, inZone z p = true → ∀ lo hi, estimate f t z = some (lo,hi) →
      lo ≤ f.score t p ∧ f.score t p ≤ hi) : phiHull f := by
  intro t a b l h x y he hp
  have hpj : inZone (join a b) (x,y) = true := by
    apply inZone_iff.mpr
    have hp' := inZone_iff.mp hp
    constructor
    · exact Int.le_trans (Int.min_le_left a.xl b.xl) hp'.1
    constructor
    · exact Int.le_trans hp'.2.1 (Int.le_max_left a.xh b.xh)
    constructor
    · exact Int.le_trans (Int.min_le_left a.yl b.yl) hp'.2.2.1
    constructor
    · exact Int.le_trans hp'.2.2.2.1 (Int.le_max_left a.yh b.yh)
    constructor
    · exact Int.le_trans (Int.min_le_left a.dl b.dl) hp'.2.2.2.2.1
    · exact Int.le_trans hp'.2.2.2.2.2 (Int.le_max_left a.dh b.dh)
  exact hc t (join a b) (x,y) hpj l h he

theorem removeLower_of_covers_nonneg {f : Transfer}
    (hc : ∀ t z p, inZone z p = true → ∀ lo hi, estimate f t z = some (lo,hi) →
      lo ≤ f.score t p ∧ f.score t p ≤ hi)
    (hnn : ∀ t p, 0 ≤ f.score t p) : removeLower f := by
  intro z t l h w p he hfit hp
  have hc' := hc t z p hp l h he
  have hn := hnn t p
  simp [fits] at hfit ⊢
  omega

theorem guardNecessity_general (f : Transfer) : guardNecessity f := by
  intro z t l h w he hbad
  have hlmem : l ∈ (vertices z (f.cuts t)).map (f.score t) := by
    simpa [estimate] using bounds_min_mem (vs := (vertices z (f.cuts t)).map (f.score t)) he
  have hhmem : h ∈ (vertices z (f.cuts t)).map (f.score t) := by
    simpa [estimate] using bounds_max_mem (vs := (vertices z (f.cuts t)).map (f.score t)) he
  rcases List.mem_map.mp hlmem with ⟨vl, hvl, hl_eq⟩
  rcases List.mem_map.mp hhmem with ⟨vh, hvh, hh_eq⟩
  have hvin := vertex_inZone (z := z) (ds := f.cuts t) (p := vl) hvl
  have hwin := vertex_inZone (z := z) (ds := f.cuts t) (p := vh) hvh
  simp at hbad
  by_cases hfl : fits w l = true
  · have hhf : fits w h = false := hbad.resolve_left (by simp [hfl])
    refine ⟨vh, hwin, ?_⟩
    simpa [hh_eq] using hhf
  · have hlf : fits w l = false := by cases hfit : fits w l <;> simp_all
    refine ⟨vl, hvin, ?_⟩
    simpa [hl_eq] using hlf

theorem inZone_split_fst {z cut p} (hp : inZone (split z cut).1 p = true) : inZone z p = true := by
  rcases inZone_iff.mp hp with ⟨hxl,hxh,hyl,hyh,hdl,hdh⟩
  apply inZone_iff.mpr
  simp [split] at hxl hxh hyl hyh hdl hdh ⊢
  omega

theorem inZone_split_snd {z cut p} (hp : inZone (split z cut).2 p = true) : inZone z p = true := by
  rcases inZone_iff.mp hp with ⟨hxl,hxh,hyl,hyh,hdl,hdh⟩
  apply inZone_iff.mpr
  simp [split] at hxl hxh hyl hyh hdl hdh ⊢
  omega

theorem mem_vertices_x_split {z ds cut x d}
    (hx : x = z.xl ∨ x = z.xh) (hd : d = z.dl ∨ d = z.dh ∨ d ∈ ds)
    (hz : inZone z (x, x-d) = true) :
    (x, x-d) ∈ vertices (split z cut).1 ds ∨ (x, x-d) ∈ vertices (split z cut).2 ds := by
  have hdz : z.dl ≤ d ∧ d ≤ z.dh := by
    rcases inZone_iff.mp hz with ⟨hxl,hxh,hyl,hyh,hdl,hdh⟩
    omega
  by_cases hdc : d ≤ cut
  · left
    apply mem_vertices_of_x (z := (split z cut).1) (x := x) (d := d) hx
    · rcases hd with rfl | rfl | hmem
      · left; rfl
      · right; left; dsimp [split]; omega
      · right; right; exact hmem
    · apply inZone_iff.mpr
      rcases inZone_iff.mp hz with ⟨hxl,hxh,hyl,hyh,hdl,hdh⟩
      dsimp [split]
      omega
  · right
    apply mem_vertices_of_x (z := (split z cut).2) (x := x) (d := d) hx
    · rcases hd with rfl | rfl | hmem
      · left; dsimp [split]; omega
      · right; left; rfl
      · right; right; exact hmem
    · apply inZone_iff.mpr
      rcases inZone_iff.mp hz with ⟨hxl,hxh,hyl,hyh,hdl,hdh⟩
      dsimp [split]
      omega

theorem mem_vertices_y_split {z ds cut y d}
    (hy : y = z.yl ∨ y = z.yh) (hd : d = z.dl ∨ d = z.dh ∨ d ∈ ds)
    (hz : inZone z (y+d, y) = true) :
    (y+d, y) ∈ vertices (split z cut).1 ds ∨ (y+d, y) ∈ vertices (split z cut).2 ds := by
  have hdz : z.dl ≤ d ∧ d ≤ z.dh := by
    rcases inZone_iff.mp hz with ⟨hxl,hxh,hyl,hyh,hdl,hdh⟩
    omega
  by_cases hdc : d ≤ cut
  · left
    apply mem_vertices_of_y (z := (split z cut).1) (y := y) (d := d) hy
    · rcases hd with rfl | rfl | hmem
      · left; rfl
      · right; left; dsimp [split]; omega
      · right; right; exact hmem
    · apply inZone_iff.mpr
      rcases inZone_iff.mp hz with ⟨hxl,hxh,hyl,hyh,hdl,hdh⟩
      dsimp [split]
      omega
  · right
    apply mem_vertices_of_y (z := (split z cut).2) (y := y) (d := d) hy
    · rcases hd with rfl | rfl | hmem
      · left; dsimp [split]; omega
      · right; left; rfl
      · right; right; exact hmem
    · apply inZone_iff.mpr
      rcases inZone_iff.mp hz with ⟨hxl,hxh,hyl,hyh,hdl,hdh⟩
      dsimp [split]
      omega

theorem mem_vertices_corner_split {z ds cut x y}
    (hx : x = z.xl ∨ x = z.xh) (hy : y = z.yl ∨ y = z.yh)
    (hz : inZone z (x,y) = true) :
    (x,y) ∈ vertices (split z cut).1 ds ∨ (x,y) ∈ vertices (split z cut).2 ds := by
  have hdz : z.dl ≤ x-y ∧ x-y ≤ z.dh := by
    rcases inZone_iff.mp hz with ⟨hxl,hxh,hyl,hyh,hdl,hdh⟩
    omega
  by_cases hdc : x-y ≤ cut
  · left
    apply mem_vertices_corner (z := (split z cut).1) hx hy
    apply inZone_iff.mpr
    rcases inZone_iff.mp hz with ⟨hxl,hxh,hyl,hyh,hdl,hdh⟩
    dsimp [split]
    omega
  · right
    apply mem_vertices_corner (z := (split z cut).2) hx hy
    apply inZone_iff.mpr
    rcases inZone_iff.mp hz with ⟨hxl,hxh,hyl,hyh,hdl,hdh⟩
    dsimp [split]
    omega

theorem vertices_split_cover {z ds cut v} (hv : v ∈ vertices z ds) :
    v ∈ vertices (split z cut).1 ds ∨ v ∈ vertices (split z cut).2 ds := by
  rw [vertices] at hv
  rcases List.mem_filter.mp hv with ⟨hraw, hzone⟩
  simp only [List.mem_append, List.mem_flatMap, List.mem_map] at hraw
  rcases hraw with (hraw | hraw) | hraw
  · rcases hraw with ⟨x, hxmem, hrest⟩
    rcases hrest with ⟨y, hymem, hxy⟩
    simp at hxmem hymem
    subst v
    exact mem_vertices_corner_split hxmem hymem hzone
  · rcases hraw with ⟨x, hxmem, hrest⟩
    rcases hrest with ⟨d, hdmem, hxd⟩
    simp at hxmem hdmem
    subst v
    exact mem_vertices_x_split hxmem hdmem hzone
  · rcases hraw with ⟨y, hymem, hrest⟩
    rcases hrest with ⟨d, hdmem, hyd⟩
    simp at hymem hdmem
    subst v
    exact mem_vertices_y_split hymem hdmem hzone

theorem bounds_eq_minmax {vs : List Int} {lo hi : Int}
    (hlo_mem : lo ∈ vs) (hlo_le : ∀ e ∈ vs, lo ≤ e)
    (hhi_mem : hi ∈ vs) (hle_hi : ∀ e ∈ vs, e ≤ hi) :
    bounds vs = some (lo, hi) := by
  apply bounds_eq_some_iff.mpr
  exact ⟨List.min?_eq_some_iff.mpr ⟨hlo_mem, hlo_le⟩,
         List.max?_eq_some_iff.mpr ⟨hhi_mem, hle_hi⟩⟩






theorem vertices_mono_ds {z : Zone} {ds ds' : List Int} (hsub : ∀ d, d ∈ ds → d ∈ ds')
    {v : Point} (hv : v ∈ vertices z ds) : v ∈ vertices z ds' := by
  rw [vertices] at hv ⊢
  rcases List.mem_filter.mp hv with ⟨hraw, hzone⟩
  apply List.mem_filter.mpr
  constructor
  · simp only [List.mem_append, List.mem_flatMap, List.mem_map] at hraw ⊢
    rcases hraw with (hraw | hraw) | hraw
    · left; left; exact hraw
    · left; right
      rcases hraw with ⟨x, hx, d, hd, hxd⟩
      exact ⟨x, hx, d, by simp at hd; rcases hd with rfl | rfl | hdmem; simp; simp; simp [hsub d hdmem], hxd⟩
    · right
      rcases hraw with ⟨y, hy, d, hd, hyd⟩
      exact ⟨y, hy, d, by simp at hd; rcases hd with rfl | rfl | hdmem; simp; simp; simp [hsub d hdmem], hyd⟩
  · exact hzone

theorem diff_vertex {z : Zone} {ds : List Int} {d : Int}
    (hxl : z.xl ≤ z.xh) (hyl : z.yl ≤ z.yh)
    (hd : max z.dl (z.xl-z.yh) ≤ d ∧ d ≤ min z.dh (z.xh-z.yl))
    (hdmem : d = z.dl ∨ d = z.dh ∨ d ∈ ds) :
    ∃ v, v ∈ vertices z ds ∧ inZone z v = true ∧ v.1 - v.2 = d := by
  have hdl : z.dl ≤ d := Int.le_trans (Int.le_max_left _ _) hd.1
  have hdh : d ≤ z.dh := Int.le_trans hd.2 (Int.min_le_left _ _)
  have hxlow : z.xl - z.yh ≤ d := Int.le_trans (Int.le_max_right _ _) hd.1
  have hxhigh : d ≤ z.xh - z.yl := Int.le_trans hd.2 (Int.min_le_right _ _)
  by_cases hcase : z.xl - z.yh ≤ d
  · by_cases hyhc : z.xl - d ≤ z.yh
    · by_cases hylc : z.xl - d ≤ z.yl
      · refine ⟨(z.yl + d, z.yl), ?_, ?_, ?_⟩
        · apply mem_vertices_of_y (z := z) (ds := ds) (y := z.yl) (d := d) (Or.inl rfl)
          rcases hdmem with rfl | rfl | hdmem
          · left; rfl
          · right; left; rfl
          · right; right; exact hdmem
          apply inZone_iff.mpr; dsimp; omega
        · apply inZone_iff.mpr; dsimp; omega
        · dsimp; omega
      · refine ⟨(z.xl, z.xl - d), ?_, ?_, ?_⟩
        · apply mem_vertices_of_x (z := z) (ds := ds) (x := z.xl) (d := d) (Or.inl rfl)
          rcases hdmem with rfl | rfl | hdmem
          · left; rfl
          · right; left; rfl
          · right; right; exact hdmem
          apply inZone_iff.mpr; dsimp; omega
        · apply inZone_iff.mpr; dsimp; omega
        · dsimp; omega
    · refine ⟨(z.xl, z.yh), ?_, ?_, ?_⟩
      · apply mem_vertices_corner (z := z) (ds := ds) (x := z.xl) (y := z.yh) (Or.inl rfl) (Or.inr rfl)
        apply inZone_iff.mpr; dsimp; omega
      · apply inZone_iff.mpr; dsimp; omega
      · dsimp; omega
  · have : d < z.xl - z.yh := by omega
    -- this case is impossible given hd.1; derive contradiction
    exfalso
    omega


end TinylambOpt.ResearchProofs.JitFrontiers.RelationalGuards.Common_PositiveExcess_FoldCompare_PositiveExcess_FoldCompare

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitFrontiers.RelationalGuards.CorrelatedSubLemmas_PositiveExcess_FoldCompare_PositiveExcess_FoldCompare

open TinylambOpt.ResearchGoals.JitFrontiers.RelationalGuards
open TinylambOpt.ResearchProofs.JitFrontiers.RelationalGuards.Common_PositiveExcess_FoldCompare_PositiveExcess_FoldCompare

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

end TinylambOpt.ResearchProofs.JitFrontiers.RelationalGuards.CorrelatedSubLemmas_PositiveExcess_FoldCompare_PositiveExcess_FoldCompare

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitFrontiers.RelationalGuards.PositiveExcessLemmas_PositiveExcess_FoldCompare_PositiveExcess_FoldCompare

open TinylambOpt.ResearchGoals.JitFrontiers.RelationalGuards
open TinylambOpt.ResearchProofs.JitFrontiers.RelationalGuards.Common_PositiveExcess_FoldCompare_PositiveExcess_FoldCompare
open TinylambOpt.ResearchProofs.JitFrontiers.RelationalGuards.CorrelatedSubLemmas_PositiveExcess_FoldCompare_PositiveExcess_FoldCompare

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

end TinylambOpt.ResearchProofs.JitFrontiers.RelationalGuards.PositiveExcessLemmas_PositiveExcess_FoldCompare_PositiveExcess_FoldCompare
namespace TinylambOpt.ResearchProofs.JitFrontiers.RelationalGuards.Goal_PositiveExcess_FoldCompare

open TinylambOpt.ResearchGoals.JitFrontiers.RelationalGuards
open TinylambOpt.ResearchGoals.JitFrontiers.RelationalGuards.PositiveExcess
open TinylambOpt.ResearchProofs.JitFrontiers.RelationalGuards.Common_PositiveExcess_FoldCompare_PositiveExcess_FoldCompare
open TinylambOpt.ResearchGoals.JitFrontiers.CheckedRewrites (fits limit)
open TinylambOpt.ResearchProofs.JitFrontiers.RelationalGuards.CorrelatedSubLemmas_PositiveExcess_FoldCompare_PositiveExcess_FoldCompare
open TinylambOpt.ResearchProofs.JitFrontiers.RelationalGuards.PositiveExcessLemmas_PositiveExcess_FoldCompare_PositiveExcess_FoldCompare

theorem sound : PositiveExcess.FoldCompare.Goal := by
  exact foldCompare_of_covers coverage_pe

end TinylambOpt.ResearchProofs.JitFrontiers.RelationalGuards.Goal_PositiveExcess_FoldCompare
