import TinylambOpt.ResearchGoals.Atlas.RangePlans

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_AlignedWeek_UnionAnti

open TinylambOpt.ResearchGoals.Atlas.RangePlans
open TinylambOpt.ResearchGoals.Atlas.RangePlans.AlignedWeek

/-! The AlignedWeek stencil is exactly one half-open rectangle: the bucket
`[7*(p.x/7), 7*(p.x/7)+7) × [10*(p.y/10), 10*(p.y/10)+10)`.  For Euclidean
division by a positive divisor this bucket is precisely the set of points
qualifying with `p` (including negative coordinates).  Thus the compiled
signed corner events count the covering probes at every row, and the count is
zero exactly when `covered` is false. -/

private def boxIn (q : Box) (v : Point) : Bool :=
  decide (q.x0 ≤ v.x ∧ v.x < q.x1 ∧ q.y0 ≤ v.y ∧ v.y < q.y1)

private theorem bucket7 (a v : Int) :
    (7 * (a / 7) ≤ v ∧ v < 7 * (a / 7) + 7) ↔ v / 7 = a / 7 := by
  constructor
  · intro h
    have hvl : v / 7 * 7 ≤ v := Int.ediv_mul_le v (by omega)
    have hvu : v < v / 7 * 7 + 7 := by
      have hmod_nonneg : 0 ≤ v % 7 := Int.emod_nonneg v (by omega)
      have hmod_lt : v % 7 < 7 := Int.emod_lt_of_pos v (by omega)
      have hdecomp := Int.ediv_mul_add_emod v 7
      omega
    by_cases hlt : v / 7 < a / 7
    · omega
    · by_cases hgt : a / 7 < v / 7
      · omega
      · omega
  · intro h
    have hvl : v / 7 * 7 ≤ v := Int.ediv_mul_le v (by omega)
    have hvu : v < v / 7 * 7 + 7 := by
      have hmod_nonneg : 0 ≤ v % 7 := Int.emod_nonneg v (by omega)
      have hmod_lt : v % 7 < 7 := Int.emod_lt_of_pos v (by omega)
      have hdecomp := Int.ediv_mul_add_emod v 7
      omega
    rw [h] at hvl hvu
    constructor <;> omega

private theorem bucket10 (a v : Int) :
    (10 * (a / 10) ≤ v ∧ v < 10 * (a / 10) + 10) ↔ v / 10 = a / 10 := by
  constructor
  · intro h
    have hvl : v / 10 * 10 ≤ v := Int.ediv_mul_le v (by omega)
    have hvu : v < v / 10 * 10 + 10 := by
      have hmod_nonneg : 0 ≤ v % 10 := Int.emod_nonneg v (by omega)
      have hmod_lt : v % 10 < 10 := Int.emod_lt_of_pos v (by omega)
      have hdecomp := Int.ediv_mul_add_emod v 10
      omega
    by_cases hlt : v / 10 < a / 10
    · omega
    · by_cases hgt : a / 10 < v / 10
      · omega
      · omega
  · intro h
    have hvl : v / 10 * 10 ≤ v := Int.ediv_mul_le v (by omega)
    have hvu : v < v / 10 * 10 + 10 := by
      have hmod_nonneg : 0 ≤ v % 10 := Int.emod_nonneg v (by omega)
      have hmod_lt : v % 10 < 10 := Int.emod_lt_of_pos v (by omega)
      have hdecomp := Int.ediv_mul_add_emod v 10
      omega
    rw [h] at hvl hvu
    constructor <;> omega

private theorem prefixMass_cons (e : Point × Int) (es : Rows) (x y : Int) :
    prefixMass (e :: es) x y =
      (if e.1.x < x && e.1.y < y then e.2 else 0) + prefixMass es x y := by
  by_cases h : e.1.x < x && e.1.y < y <;> simp [prefixMass, h]

private theorem prefixMass_append (es₁ es₂ : Rows) (x y : Int) :
    prefixMass (es₁ ++ es₂) x y = prefixMass es₁ x y + prefixMass es₂ x y := by
  induction es₁ with
  | nil => simp [prefixMass]
  | cons e es ih =>
      rw [List.cons_append, prefixMass_cons, prefixMass_cons, ih]
      omega

private theorem atPoint_append (es₁ es₂ : Rows) (r : Point) :
    atPoint (es₁ ++ es₂) r = atPoint es₁ r + atPoint es₂ r := by
  simp [atPoint, prefixMass_append]

private theorem atPoint_events (q : Box) (w : Int) (r : Point) :
    atPoint (events q w) r = if boxIn q r then w * q.coeff else 0 := by
  unfold events
  by_cases hb : q.x0 < q.x1 && q.y0 < q.y1
  · have hbox : q.x0 < q.x1 ∧ q.y0 < q.y1 := by simpa using hb
    simp [hb, atPoint, prefixMass]
    by_cases hm : q.x0 ≤ r.x ∧ r.x < q.x1 ∧ q.y0 ≤ r.y ∧ r.y < q.y1
    · have hmd : decide (q.x0 ≤ r.x ∧ r.x < q.x1 ∧ q.y0 ≤ r.y ∧ r.y < q.y1) = true := by
        simp [hm]
      simp only [boxIn, hmd, ite_true]
      by_cases hx0 : q.x0 < r.x + 1 <;> by_cases hx1 : q.x1 < r.x + 1 <;>
        by_cases hy0 : q.y0 < r.y + 1 <;> by_cases hy1 : q.y1 < r.y + 1 <;>
        simp [hx0, hx1, hy0, hy1] <;> omega
    · have hmd : decide (q.x0 ≤ r.x ∧ r.x < q.x1 ∧ q.y0 ≤ r.y ∧ r.y < q.y1) = false := by
        simp [hm]
      simp only [boxIn, hmd]
      have hout : r.x < q.x0 ∨ q.x1 ≤ r.x ∨ r.y < q.y0 ∨ q.y1 ≤ r.y := by omega
      rcases hout with hout | hout | hout | hout <;>
        by_cases hx0 : q.x0 < r.x + 1 <;> by_cases hx1 : q.x1 < r.x + 1 <;>
          by_cases hy0 : q.y0 < r.y + 1 <;> by_cases hy1 : q.y1 < r.y + 1 <;>
          simp [hx0, hx1, hy0, hy1] <;> omega
  · have hb' : ¬ (q.x0 < q.x1 ∧ q.y0 < q.y1) := by simpa using hb
    simp [hb, atPoint, prefixMass]
    simp only [boxIn, decide_eq_true_eq]
    omega

private theorem box_iff_qualifies (p v : Point) :
    boxIn (box (7 * (p.x / 7)) (7 * (p.x / 7) + 7)
      (10 * (p.y / 10)) (10 * (p.y / 10) + 10)) v = shape.qualifies p v := by
  unfold boxIn box shape
  apply Bool.eq_iff_iff.mpr
  simp only [decide_eq_true_eq, Bool.and_eq_true, beq_iff_eq]
  constructor
  · intro h
    constructor
    · exact (bucket7 p.x v.x).mp ⟨h.1, h.2.1⟩
    · exact (bucket10 p.y v.y).mp ⟨h.2.2.1, h.2.2.2⟩
  · intro h
    have hx := (bucket7 p.x v.x).mpr h.1
    have hy := (bucket10 p.y v.y).mpr h.2
    exact ⟨hx.1, hx.2, hy.1, hy.2⟩

private theorem atPoint_compile_units (b : Int) (ps : List Point) (v : Point) :
    atPoint (compile shape b (ps.map (fun p => (p, (1 : Int))))) v =
      (ps.map (fun p => if shape.qualifies p v then (1 : Int) else 0)).sum := by
  unfold compile
  induction ps with
  | nil => simp [atPoint, prefixMass]
  | cons p ps ih =>
      simp only [List.map_cons, List.flatMap_cons]
      rw [atPoint_append, ih]
      have hsingle : (shape.stencil b p).flatMap (fun q => events q (1 : Int)) =
          events (box (7 * (p.x / 7)) (7 * (p.x / 7) + 7)
            (10 * (p.y / 10)) (10 * (p.y / 10) + 10)) 1 := by
        simp [shape]
      rw [hsingle, atPoint_events, box_iff_qualifies]
      simp [box]

private theorem sum_indicator_nonneg (ps : List Point) (f : Point → Bool) :
    0 ≤ (ps.map (fun p => if f p then (1 : Int) else 0)).sum := by
  induction ps with
  | nil => simp
  | cons p ps ih =>
      by_cases h : f p = true <;> simp [h, ih] <;> omega

private theorem sum_indicator_any_zero (ps : List Point) (f : Point → Bool) :
    ((ps.map (fun p => if f p then (1 : Int) else 0)).sum == 0) = !ps.any f := by
  induction ps with
  | nil => simp
  | cons p ps ih =>
      simp only [List.map_cons, List.sum_cons, List.any_cons] at ih ⊢
      by_cases hp : f p = true
      · rw [hp]
        simp only [Bool.true_or, Bool.not_true, ite_true]
        apply Bool.eq_false_of_ne_true
        intro hz
        have heq : 1 + (ps.map (fun p => if f p then (1 : Int) else 0)).sum = 0 :=
          of_decide_eq_true hz
        have hn := sum_indicator_nonneg ps f
        omega
      · have hf : f p = false := Bool.eq_false_of_ne_true hp
        rw [hf]
        simp only [Bool.false_eq_true, ite_false, Bool.false_or, Int.zero_add, ih]

theorem sound_AlignedWeek_UnionAnti : UnionAnti.Goal := by
  intro b ps rs hb
  dsimp only
  apply List.filter_congr
  intro r hr
  rw [atPoint_compile_units b ps r.1]
  rw [sum_indicator_any_zero]
  rfl

end TinylambOpt.ResearchProofs.Atlas.Goal_AlignedWeek_UnionAnti
