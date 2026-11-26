import TinylambOpt.ResearchGoals.JitFrontiers.RelationalGuards

set_option maxHeartbeats 0
set_option maxRecDepth 10000


set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitFrontiers.RelationalGuards.Common_WeightedMix_PhiHull

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
  · exfalso
    omega


end TinylambOpt.ResearchProofs.JitFrontiers.RelationalGuards.Common_WeightedMix_PhiHull

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitFrontiers.RelationalGuards.CorrelatedSubLemmas_WeightedMix_PhiHull

open TinylambOpt.ResearchGoals.JitFrontiers.RelationalGuards
open TinylambOpt.ResearchProofs.JitFrontiers.RelationalGuards.Common_WeightedMix_PhiHull

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

end TinylambOpt.ResearchProofs.JitFrontiers.RelationalGuards.CorrelatedSubLemmas_WeightedMix_PhiHull

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.JitFrontiers.RelationalGuards.WeightedMixLemmas_WeightedMix_PhiHull

open TinylambOpt.ResearchGoals.JitFrontiers.RelationalGuards
open TinylambOpt.ResearchProofs.JitFrontiers.RelationalGuards.Common_WeightedMix_PhiHull
open TinylambOpt.ResearchProofs.JitFrontiers.RelationalGuards.CorrelatedSubLemmas_WeightedMix_PhiHull

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

end TinylambOpt.ResearchProofs.JitFrontiers.RelationalGuards.WeightedMixLemmas_WeightedMix_PhiHull
namespace TinylambOpt.ResearchProofs.JitFrontiers.RelationalGuards.Goal_WeightedMix_PhiHull

open TinylambOpt.ResearchGoals.JitFrontiers.RelationalGuards
open TinylambOpt.ResearchGoals.JitFrontiers.RelationalGuards.WeightedMix
open TinylambOpt.ResearchProofs.JitFrontiers.RelationalGuards.Common_WeightedMix_PhiHull
open TinylambOpt.ResearchGoals.JitFrontiers.CheckedRewrites (fits limit)
open TinylambOpt.ResearchProofs.JitFrontiers.RelationalGuards.CorrelatedSubLemmas_WeightedMix_PhiHull
open TinylambOpt.ResearchProofs.JitFrontiers.RelationalGuards.WeightedMixLemmas_WeightedMix_PhiHull

theorem sound : WeightedMix.PhiHull.Goal := by
  change phiHull wm
  exact phiHull_of_covers coverage_wm

end TinylambOpt.ResearchProofs.JitFrontiers.RelationalGuards.Goal_WeightedMix_PhiHull
