import TinylambOpt.ResearchGoals.Atlas.RangePlans

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_PriceCorridor_PrefixIndex

open TinylambOpt.ResearchGoals.Atlas.RangePlans
open TinylambOpt.ResearchGoals.Atlas.RangePlans.PriceCorridor

private def boxIn (q : Box) (v : Point) : Bool :=
  decide (q.x0 ≤ v.x ∧ v.x < q.x1 ∧ q.y0 ≤ v.y ∧ v.y < q.y1)

private theorem natAbs_le_of_bounds (d : Int) (hlo : -3 ≤ d) (hhi : d ≤ 3) :
    d.natAbs ≤ 3 := by
  by_cases h : 0 ≤ d
  · have hcast : ((d.natAbs : Nat) : Int) ≤ 3 := by
      rw [Int.natAbs_of_nonneg h]
      omega
    exact_mod_cast hcast
  · have hcast : ((d.natAbs : Nat) : Int) ≤ 3 := by
      rw [← Int.natAbs_neg d, Int.natAbs_of_nonneg (by omega : 0 ≤ -d)]
      omega
    exact_mod_cast hcast

private theorem bounds_of_natAbs_le (d : Int) (h : d.natAbs ≤ 3) : -3 ≤ d ∧ d ≤ 3 := by
  have hcast : ((d.natAbs : Nat) : Int) ≤ 3 := by exact_mod_cast h
  rcases Int.natAbs_eq d with hd | hd <;> omega

private theorem sum_filter_eq_sum_if (rs : Rows) (f : Point × Int → Bool) :
    ((rs.filter f).map Prod.snd).sum = (rs.map (fun r => if f r then r.2 else 0)).sum := by
  induction rs with
  | nil => rfl
  | cons r rs ih =>
    by_cases h : f r = true <;> simp [h, ih]

private theorem prefixMass_cons (r : Point × Int) (rs : Rows) (x y : Int) :
    prefixMass (r :: rs) x y =
      (if r.1.x < x && r.1.y < y then r.2 else 0) + prefixMass rs x y := by
  by_cases h : r.1.x < x && r.1.y < y <;> simp [prefixMass, h]

private theorem prefixMass_box (rs : Rows) (x0 x1 y0 y1 : Int) (hx : x0 < x1) (hy : y0 < y1) :
    prefixMass rs x1 y1 - prefixMass rs x0 y1 - prefixMass rs x1 y0 + prefixMass rs x0 y0 =
      ((rs.filter (fun r => boxIn (box x0 x1 y0 y1) r.1)).map Prod.snd).sum := by
  rw [sum_filter_eq_sum_if]
  simp only [boxIn, box]
  induction rs with
  | nil => simp [prefixMass]
  | cons r rs ih =>
    rw [prefixMass_cons, prefixMass_cons, prefixMass_cons, prefixMass_cons]
    simp only [List.map_cons, List.sum_cons]
    by_cases hx1 : r.1.x < x1 <;> by_cases hx0 : r.1.x < x0 <;>
      by_cases hy1 : r.1.y < y1 <;> by_cases hy0 : r.1.y < y0 <;>
      by_cases hxloP : x0 ≤ r.1.x <;> by_cases hyloP : y0 ≤ r.1.y <;>
        simp [hx1, hx0, hy1, hy0, hxloP, hyloP] <;>
        first | exact ih | omega

theorem sound_PriceCorridor_PrefixIndex : PrefixIndex.Goal := by
  intro b p rs hb
  unfold indexed scan
  rw [show shape.stencil b p = [box (p.x - 3) (p.x + 4) (p.y - 2) (p.y + 6)] by rfl]
  simp only [List.map_cons, List.map_nil, List.sum_cons, List.sum_nil, Int.add_zero]
  change corners (box (p.x - 3) (p.x + 4) (p.y - 2) (p.y + 6)) (prefixMass rs) =
    ((rs.filter (fun r => shape.qualifies p r.1)).map Prod.snd).sum
  unfold corners
  simp only [box]
  rw [ite_eq_left (by
    simp only [Bool.and_eq_true]
    constructor <;> apply decide_eq_true <;> omega)]
  rw [prefixMass_box rs (p.x - 3) (p.x + 4) (p.y - 2) (p.y + 6) (by omega) (by omega)]
  simp only [Int.one_mul]
  congr 1
  apply congrArg (List.map Prod.snd)
  apply List.filter_congr
  intro r hr
  apply Bool.eq_iff_iff.mpr
  constructor <;> intro h
  · have hh : p.x - 3 ≤ r.1.x ∧ r.1.x < p.x + 4 ∧
        p.y - 2 ≤ r.1.y ∧ r.1.y < p.y + 6 := by
      have h' : decide (p.x - 3 ≤ r.1.x ∧ r.1.x < p.x + 4 ∧
          p.y - 2 ≤ r.1.y ∧ r.1.y < p.y + 6) = true := by
        change boxIn (box (p.x - 3) (p.x + 4) (p.y - 2) (p.y + 6)) r.1 = true at h
        exact h
      exact of_decide_eq_true h'
    simp [shape]
    constructor
    · constructor
      · exact natAbs_le_of_bounds (r.1.x - p.x) (by omega) (by omega)
      · omega
    · omega
  · simp [shape] at h
    change decide (p.x - 3 ≤ r.1.x ∧ r.1.x < p.x + 4 ∧
        p.y - 2 ≤ r.1.y ∧ r.1.y < p.y + 6) = true
    apply decide_eq_true
    rcases bounds_of_natAbs_le (r.1.x - p.x) h.1.1 with ⟨hblo, hbhi⟩
    omega

end TinylambOpt.ResearchProofs.Atlas.Goal_PriceCorridor_PrefixIndex
