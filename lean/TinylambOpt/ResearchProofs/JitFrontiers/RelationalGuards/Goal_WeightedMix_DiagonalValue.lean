import TinylambOpt.ResearchGoals.JitFrontiers.RelationalGuards

/-!
A JIT can use certified operand and difference bounds to choose guards, fold
comparisons, or eliminate unreachable paths for active rows.

Proved

- `WeightedMix.DiagonalValue` — `∀ t x, expr.score t (x,x)=x`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000


set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitFrontiers.RelationalGuards.Common_WeightedMix_DiagonalValue

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




end TinylambOpt.ResearchProofs.JitFrontiers.RelationalGuards.Common_WeightedMix_DiagonalValue

namespace TinylambOpt.ResearchProofs.JitFrontiers.RelationalGuards.Goal_WeightedMix_DiagonalValue

open TinylambOpt.ResearchGoals.JitFrontiers.RelationalGuards
open TinylambOpt.ResearchGoals.JitFrontiers.RelationalGuards.WeightedMix
open TinylambOpt.ResearchProofs.JitFrontiers.RelationalGuards.Common_WeightedMix_DiagonalValue
open TinylambOpt.ResearchGoals.JitFrontiers.CheckedRewrites (fits limit)

theorem sound : WeightedMix.DiagonalValue.Goal := by
  intro t x
  simp [WeightedMix.expr, Int.sub_mul]
  omega

end TinylambOpt.ResearchProofs.JitFrontiers.RelationalGuards.Goal_WeightedMix_DiagonalValue
