import TinylambOpt.ResearchGoals.JitFrontiers.RelationalGuards

/-!
A JIT can use certified operand and difference bounds to choose guards, fold
comparisons, or eliminate unreachable paths for active rows.

Proved

- `DoubleMinus.OmitOverflow` — Use only extrema to omit an overflow intrinsic on active
  rows.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000


set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitFrontiers.RelationalGuards.Common_DoubleMinus_OmitOverflow_DoubleMinus_OmitOverflow

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

theorem bounds_pair_of_list {vs : List Int} {lo hi : Int}
    (h : bounds vs = some (lo, hi)) : bounds [lo, hi] = some (lo, hi) := by
  rcases (bounds_eq_some_iff.mp h) with ⟨hmin, hmax⟩
  have hlmem := (List.min?_eq_some_iff.mp hmin).1
  have hhmem := (List.max?_eq_some_iff.mp hmax).1
  have hlhi : lo ≤ hi := (List.min?_eq_some_iff.mp hmin).2 hi hhmem
  apply bounds_eq_minmax
  · simp
  · intro e he; simp at he; rcases he with rfl | rfl <;> omega
  · simp
  · intro e he; simp at he; rcases he with rfl | rfl <;> omega

theorem bounds_estimates_concat (f : Transfer) (t : Int) (z : Zone) (cut : Int) :
    bounds (([estimate f t (split z cut).1, estimate f t (split z cut).2].filterMap id).flatMap
      (fun (l,h) => [l,h])) =
    bounds ((vertices (split z cut).1 (f.cuts t)).map (f.score t) ++
            (vertices (split z cut).2 (f.cuts t)).map (f.score t)) := by
  let va := (vertices (split z cut).1 (f.cuts t)).map (f.score t)
  let vb := (vertices (split z cut).2 (f.cuts t)).map (f.score t)
  have hea : estimate f t (split z cut).1 = bounds va := by rfl
  have heb : estimate f t (split z cut).2 = bounds vb := by rfl
  change bounds (([estimate f t (split z cut).1, estimate f t (split z cut).2].filterMap id).flatMap
      (fun (l,h) => [l,h])) = bounds (va ++ vb)
  by_cases hva : va = []
  · by_cases hvb : vb = []
    · simp [va, vb, hva, hvb, hea, heb, bounds]
    · rcases hbv : bounds vb with _ | ⟨lb, hbnd⟩
      · have : vb = [] := bounds_eq_none.mp hbv
        exact False.elim (hvb this)
      · have hpair : bounds [lb, hbnd] = bounds vb := (bounds_pair_of_list hbv).trans hbv.symm
        have hea0 : estimate f t (split z cut).1 = none := by rw [hea, hva]; simp [bounds]
        calc
          bounds (([estimate f t (split z cut).1, estimate f t (split z cut).2].filterMap id).flatMap
              (fun (l,h) => [l,h])) = bounds [lb, hbnd] := by simp [hea0, heb, hbv]
          _ = bounds vb := hpair
          _ = bounds (va ++ vb) := by simp [hva]
  · by_cases hvb : vb = []
    · rcases hav : bounds va with _ | ⟨la, habnd⟩
      · have : va = [] := bounds_eq_none.mp hav
        exact False.elim (hva this)
      · have hpair : bounds [la, habnd] = bounds va := (bounds_pair_of_list hav).trans hav.symm
        have heb0 : estimate f t (split z cut).2 = none := by rw [heb, hvb]; simp [bounds]
        calc
          bounds (([estimate f t (split z cut).1, estimate f t (split z cut).2].filterMap id).flatMap
              (fun (l,h) => [l,h])) = bounds [la, habnd] := by simp [hea, heb0, hav]
          _ = bounds va := hpair
          _ = bounds (va ++ vb) := by simp [hvb]
    · rcases hav : bounds va with _ | ⟨la, habnd⟩
      · have : va = [] := bounds_eq_none.mp hav
        exact False.elim (hva this)
      · rcases hbv : bounds vb with _ | ⟨lb, hbnd⟩
        · have : vb = [] := bounds_eq_none.mp hbv
          exact False.elim (hvb this)
        · have hlama : la ∈ va := (List.min?_eq_some_iff.mp (bounds_eq_some_iff.mp hav).1).1
          have hama : habnd ∈ va := (List.max?_eq_some_iff.mp (bounds_eq_some_iff.mp hav).2).1
          have hlbm : lb ∈ vb := (List.min?_eq_some_iff.mp (bounds_eq_some_iff.mp hbv).1).1
          have hbmm : hbnd ∈ vb := (List.max?_eq_some_iff.mp (bounds_eq_some_iff.mp hbv).2).1
          have hla_le_ha : la ≤ habnd := (bounds_of_mem hav hlama).2
          have hlb_le_hb : lb ≤ hbnd := (bounds_of_mem hbv hlbm).2
          have hmin : min la lb ∈ va ++ vb := by
            by_cases h : la ≤ lb
            · rw [Int.min_eq_left h]; simp [hlama]
            · have : lb ≤ la := by omega
              rw [Int.min_eq_right this]; simp [hlbm]
          have hmax : max habnd hbnd ∈ va ++ vb := by
            by_cases h : habnd ≤ hbnd
            · rw [Int.max_eq_right h]; simp [hbmm]
            · have : hbnd ≤ habnd := by omega
              rw [Int.max_eq_left this]; simp [hama]
          have hlo : ∀ e ∈ va ++ vb, min la lb ≤ e := by
            intro e he
            simp at he
            rcases he with he | he
            · exact Int.le_trans (Int.min_le_left _ _) ((List.min?_eq_some_iff.mp (bounds_eq_some_iff.mp hav).1).2 e he)
            · exact Int.le_trans (Int.min_le_right _ _) ((List.min?_eq_some_iff.mp (bounds_eq_some_iff.mp hbv).1).2 e he)
          have hhi : ∀ e ∈ va ++ vb, e ≤ max habnd hbnd := by
            intro e he
            simp at he
            rcases he with he | he
            · exact Int.le_trans ((List.max?_eq_some_iff.mp (bounds_eq_some_iff.mp hav).2).2 e he) (Int.le_max_left _ _)
            · exact Int.le_trans ((List.max?_eq_some_iff.mp (bounds_eq_some_iff.mp hbv).2).2 e he) (Int.le_max_right _ _)
          have hjoin : bounds (va ++ vb) = some (min la lb, max habnd hbnd) :=
            bounds_eq_minmax hmin hlo hmax hhi
          have hfour : bounds ([la, habnd] ++ [lb, hbnd]) = some (min la lb, max habnd hbnd) := by
            apply bounds_eq_minmax
            · by_cases h : la ≤ lb
              · rw [Int.min_eq_left h]; simp
              · have h' : lb ≤ la := by omega
                rw [Int.min_eq_right h']; simp
            · intro e he; simp at he; rcases he with rfl | rfl | rfl | rfl <;> omega
            · by_cases h : habnd ≤ hbnd
              · rw [Int.max_eq_right h]; simp
              · have h' : hbnd ≤ habnd := by omega
                rw [Int.max_eq_left h']; simp
            · intro e he; simp at he; rcases he with rfl | rfl | rfl | rfl <;> omega
          calc
            bounds (([estimate f t (split z cut).1, estimate f t (split z cut).2].filterMap id).flatMap
              (fun (l,h) => [l,h]))
                = bounds ([la, habnd] ++ [lb, hbnd]) := by simp [hea, heb, hav, hbv]
            _ = some (min la lb, max habnd hbnd) := hfour
            _ = bounds (va ++ vb) := hjoin.symm

/-- If every point of a zone is sandwiched by two vertex scores, the vertex
estimate covers every row of the zone. -/
theorem coverage_of_dom {f : Transfer}
    (hle : ∀ t z p, inZone z p = true → ∃ v, v ∈ vertices z (f.cuts t) ∧ f.score t p ≤ f.score t v)
    (hge : ∀ t z p, inZone z p = true → ∃ v, v ∈ vertices z (f.cuts t) ∧ f.score t v ≤ f.score t p) :
    ∀ t z p, inZone z p = true → ∀ lo hi, estimate f t z = some (lo,hi) →
      lo ≤ f.score t p ∧ f.score t p ≤ hi := by
  intro t z p hp lo hi he
  have hb (v : Int) (hv : v ∈ (vertices z (f.cuts t)).map (f.score t)) : lo ≤ v ∧ v ≤ hi :=
    bounds_of_mem (by simpa [estimate] using he) hv
  rcases hle t z p hp with ⟨v, hv, hvle⟩
  rcases hge t z p hp with ⟨w, hw, hwle⟩
  constructor
  · exact Int.le_trans (hb _ (List.mem_map.mpr ⟨w, hw, rfl⟩)).1 hwle
  · exact Int.le_trans hvle (hb _ (List.mem_map.mpr ⟨v, hv, rfl⟩)).2

/-- Vertex domination implies the vertex estimate equals the exhaustive
point extrema (minimal hull). -/
theorem minimalHull_of_dom {f : Transfer}
    (hle : ∀ t z p, inZone z p = true → ∃ v, v ∈ vertices z (f.cuts t) ∧ f.score t p ≤ f.score t v)
    (hge : ∀ t z p, inZone z p = true → ∃ v, v ∈ vertices z (f.cuts t) ∧ f.score t v ≤ f.score t p) :
    minimalHull f := by
  intro t z
  classical
  by_cases hne : ∃ p, inZone z p = true
  · have hvn : vertices z (f.cuts t) ≠ [] := by
      rcases hne with ⟨p, hp⟩
      rcases exists_vertex_of_inZone (z := z) (ds := f.cuts t) hp with ⟨v, hv⟩
      intro hnil
      simp [hnil] at hv
    rcases hm : (vertices z (f.cuts t)).map (f.score t) |>.min? with _ | lo
    · have hlistnil : (vertices z (f.cuts t)).map (f.score t) = [] := List.min?_eq_none_iff.mp hm
      have hvnil : vertices z (f.cuts t) = [] := by simpa using hlistnil
      exact False.elim (hvn hvnil)
    · rcases hM : (vertices z (f.cuts t)).map (f.score t) |>.max? with _ | hi
      · have : (vertices z (f.cuts t)).map (f.score t) = [] := List.max?_eq_none_iff.mp hM
        have hvnil : vertices z (f.cuts t) = [] := by simpa using this
        exact False.elim (hvn hvnil)
      · have hest : estimate f t z = some (lo,hi) := by
          simp [estimate, bounds, hm, hM]
        have hcov := coverage_of_dom hle hge t z
        have hminmem : lo ∈ (points z).map (f.score t) := by
          have hvm : lo ∈ (vertices z (f.cuts t)).map (f.score t) := (List.min?_eq_some_iff.mp hm).1
          rcases List.mem_map.mp hvm with ⟨v, hv, rfl⟩
          exact List.mem_map.mpr ⟨v, point_mem_points (vertex_inZone hv), rfl⟩
        have hmaxmem : hi ∈ (points z).map (f.score t) := by
          have hvm : hi ∈ (vertices z (f.cuts t)).map (f.score t) := (List.max?_eq_some_iff.mp hM).1
          rcases List.mem_map.mp hvm with ⟨v, hv, rfl⟩
          exact List.mem_map.mpr ⟨v, point_mem_points (vertex_inZone hv), rfl⟩
        have hext : bounds ((points z).map (f.score t)) = some (lo,hi) := by
          apply bounds_eq_minmax
          · exact hminmem
          · intro e he
            rcases List.mem_map.mp he with ⟨p, hp, rfl⟩
            exact (hcov p (List.mem_filter.mp hp).2 lo hi hest).1
          · exact hmaxmem
          · intro e he
            rcases List.mem_map.mp he with ⟨p, hp, rfl⟩
            exact (hcov p (List.mem_filter.mp hp).2 lo hi hest).2
        simpa [estimate, extrema, hest, hext]
  · have hest : estimate f t z = none := (deadEdge_general f t z).2 (by
      intro p
      by_cases hp : inZone z p = true
      · exact False.elim (hne ⟨p, hp⟩)
      · simpa using hp)
    have hpnil : points z = [] := by
      apply List.eq_nil_iff_forall_not_mem.mpr
      intro p hp
      exact hne ⟨p, (List.mem_filter.mp hp).2⟩
    change estimate f t z = extrema f t z
    rw [hest]
    simp [extrema, hpnil, bounds]

/-- BranchSplit follows from minimal hull plus the generic vertex cover of
the two split zones. -/
theorem branchSplit_of_minimalHull {f : Transfer}
    (hMH : ∀ t z, estimate f t z = extrema f t z) : branchSplit f := by
  intro z t cut
  let a : Zone := (split z cut).1
  let b : Zone := (split z cut).2
  let sc := f.score t
  let ds := f.cuts t
  let vz := (vertices z ds).map sc
  let va := (vertices a ds).map sc
  let vb := (vertices b ds).map sc
  have hzcover : ∀ v, v ∈ vertices z ds → v ∈ vertices a ds ∨ v ∈ vertices b ds := by
    intro v hv
    exact vertices_split_cover (z := z) (ds := ds) (cut := cut) hv
  have hzaZone : ∀ v, v ∈ vertices a ds → inZone z v := by
    intro v hv
    exact inZone_split_fst (vertex_inZone hv)
  have hzbZone : ∀ v, v ∈ vertices b ds → inZone z v := by
    intro v hv
    exact inZone_split_snd (vertex_inZone hv)
  have hcov : ∀ p, inZone z p = true → ∀ lo hi, estimate f t z = some (lo,hi) →
      lo ≤ sc p ∧ sc p ≤ hi := by
    intro p hp lo hi he
    exact covers_of_estimate_eq hp he (hMH t z)
  by_cases hzempty : ∀ p, inZone z p = false
  · have hez : estimate f t z = none := (deadEdge_general f t z).2 hzempty
    have hea : estimate f t a = none := (deadEdge_general f t a).2 (by
      intro p
      by_cases hp : inZone a p = true
      · have hzp := hzempty p
        have hzt : inZone z p = true := inZone_split_fst (z := z) (cut := cut) (p := p) hp
        rw [hzt] at hzp
        contradiction
      · simpa using hp)
    have heb : estimate f t b = none := (deadEdge_general f t b).2 (by
      intro p
      by_cases hp : inZone b p = true
      · have hzp := hzempty p
        have hzt : inZone z p = true := inZone_split_snd (z := z) (cut := cut) (p := p) hp
        rw [hzt] at hzp
        contradiction
      · simpa using hp)
    change estimate f t z = bounds (([estimate f t a, estimate f t b].filterMap id).flatMap
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
    have hpnil : (points z).map sc ≠ [] := by
      intro hn
      have : sc p0 ∈ (points z).map sc := List.mem_map.mpr ⟨p0, point_mem_points hp0, rfl⟩
      simp [hn] at this
    rcases hext : extrema f t z with _ | ⟨lo,hi⟩
    · have hn : (points z).map sc = [] := bounds_eq_none.mp (by simpa [extrema] using hext)
      exact False.elim (hpnil hn)
    · have hez : estimate f t z = some (lo,hi) := by
        rw [hMH t z]
        exact hext
      have hminmem : lo ∈ vz := by
        simpa [vz, estimate] using bounds_min_mem (vs := vz) (by simpa [vz, estimate] using hez)
      have hmaxmem : hi ∈ vz := by
        simpa [vz, estimate] using bounds_max_mem (vs := vz) (by simpa [vz, estimate] using hez)
      have hzv : bounds vz = some (lo,hi) := by simpa [vz, estimate] using hez
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
            exact (hcov v (hzaZone v hva) lo hi hez).1
          · rcases List.mem_map.mp he with ⟨v, hvb, rfl⟩
            exact (hcov v (hzbZone v hvb) lo hi hez).1
        · rcases List.mem_map.mp hmaxmem with ⟨v, hv, hvhi⟩
          rcases hzcover v hv with hva | hvb
          · exact List.mem_append.mpr (Or.inl (List.mem_map.mpr ⟨v, hva, hvhi⟩))
          · exact List.mem_append.mpr (Or.inr (List.mem_map.mpr ⟨v, hvb, hvhi⟩))
        · intro e he
          simp at he
          rcases he with he | he
          · rcases List.mem_map.mp he with ⟨v, hva, rfl⟩
            exact (hcov v (hzaZone v hva) lo hi hez).2
          · rcases List.mem_map.mp he with ⟨v, hvb, rfl⟩
            exact (hcov v (hzbZone v hvb) lo hi hez).2
      calc
        estimate f t z = bounds vz := by rfl
        _ = some (lo,hi) := hzv
        _ = bounds (va ++ vb) := havb.symm
        _ = bounds (([estimate f t a, estimate f t b].filterMap id).flatMap
            (fun (l,h) => [l,h])) := (bounds_estimates_concat f t z cut).symm

end TinylambOpt.ResearchProofs.JitFrontiers.RelationalGuards.Common_DoubleMinus_OmitOverflow_DoubleMinus_OmitOverflow

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitFrontiers.RelationalGuards.DoubleMinusLemmas_DoubleMinus_OmitOverflow_DoubleMinus_OmitOverflow

open TinylambOpt.ResearchGoals.JitFrontiers.RelationalGuards
open TinylambOpt.ResearchProofs.JitFrontiers.RelationalGuards.Common_DoubleMinus_OmitOverflow_DoubleMinus_OmitOverflow

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

end TinylambOpt.ResearchProofs.JitFrontiers.RelationalGuards.DoubleMinusLemmas_DoubleMinus_OmitOverflow_DoubleMinus_OmitOverflow
namespace TinylambOpt.ResearchProofs.JitFrontiers.RelationalGuards.Goal_DoubleMinus_OmitOverflow

open TinylambOpt.ResearchGoals.JitFrontiers.RelationalGuards
open TinylambOpt.ResearchGoals.JitFrontiers.RelationalGuards.DoubleMinus
open TinylambOpt.ResearchProofs.JitFrontiers.RelationalGuards.Common_DoubleMinus_OmitOverflow_DoubleMinus_OmitOverflow
open TinylambOpt.ResearchGoals.JitFrontiers.CheckedRewrites (fits limit)
open TinylambOpt.ResearchProofs.JitFrontiers.RelationalGuards.DoubleMinusLemmas_DoubleMinus_OmitOverflow_DoubleMinus_OmitOverflow

theorem sound : DoubleMinus.OmitOverflow.Goal := by
  exact omitOverflow_of_covers coverage_dm

end TinylambOpt.ResearchProofs.JitFrontiers.RelationalGuards.Goal_DoubleMinus_OmitOverflow
