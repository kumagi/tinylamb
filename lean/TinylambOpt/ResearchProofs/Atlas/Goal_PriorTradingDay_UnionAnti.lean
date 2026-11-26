import TinylambOpt.ResearchGoals.Atlas.RangePlans

/-!
An ordered access path needs to preserve the rows and boundaries selected by the SQL
range predicate.

Proved

- `PriorTradingDay.UnionAnti` — `∀ b (ps : List Point) (rs : Rows), (∀ r ∈ rs, bounded
  b r.1) → let es := compile shape b (ps.map (fun p => (p,1))) rs.filter (fun r =>
  !covered shape ps r.1) = rs.filter (fun r => atPoint es r.1 == 0)`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_PriorTradingDay_UnionAnti

open TinylambOpt.ResearchGoals.Atlas.RangePlans
open TinylambOpt.ResearchGoals.Atlas.RangePlans.PriorTradingDay

private def boxIn (q : Box) (v : Point) : Bool :=
  decide (q.x0 ≤ v.x ∧ v.x < q.x1 ∧ q.y0 ≤ v.y ∧ v.y < q.y1)

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

/-- A batch of signed corner events counts exactly the rows in the half-open box. -/
private theorem atPoint_events (q : Box) (w : Int) (r : Point) :
    atPoint (events q w) r = if boxIn q r then w * q.coeff else 0 := by
  unfold events
  by_cases hb : q.x0 < q.x1 && q.y0 < q.y1
  · have _hbox : q.x0 < q.x1 ∧ q.y0 < q.y1 := by simpa using hb
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
  · have _hb' : ¬ (q.x0 < q.x1 ∧ q.y0 < q.y1) := by simpa using hb
    simp [hb, atPoint, prefixMass]
    simp only [boxIn, decide_eq_true_eq]
    omega

/-- The two stencil boxes are disjoint in x, so a single probe contributes 1 iff
    the bounded build row qualifies, and never 2. -/
private theorem atPoint_single (b : Int) (p v : Point) (hv : bounded b v) :
    atPoint ((shape.stencil b p).flatMap (fun q => events q (1 : Int))) v =
      if shape.qualifies p v then (1 : Int) else 0 := by
  unfold bounded at hv
  let q1 : Box := box (p.x - 3) p.x (-b) (b + 1)
  let q2 : Box := box p.x (p.x + 1) (-b) p.y
  have hst : (shape.stencil b p).flatMap (fun q => events q (1 : Int)) =
      events q1 1 ++ events q2 1 := by
    simp [shape, q1, q2]
  rw [hst, atPoint_append, atPoint_events, atPoint_events]
  simp only [q1, q2, box, Int.mul_one]
  change (if boxIn (box (p.x - 3) p.x (-b) (b + 1)) v then (1 : Int) else 0) +
      (if boxIn (box p.x (p.x + 1) (-b) p.y) v then (1 : Int) else 0) =
    (if shape.qualifies p v then (1 : Int) else 0)
  rw [show box (p.x - 3) p.x (-b) (b + 1) = q1 by rfl,
      show box p.x (p.x + 1) (-b) p.y = q2 by rfl]
  by_cases h1 : boxIn q1 v = true
  · have htq1 : p.x - 3 ≤ v.x ∧ v.x < p.x ∧ -b ≤ v.y ∧ v.y < b + 1 := by
      simpa [boxIn, box, q1] using h1
    have h2f : boxIn q2 v = false := by
      apply Bool.eq_false_of_ne_true
      intro ht
      have ht' : p.x ≤ v.x ∧ v.x < p.x + 1 ∧ -b ≤ v.y ∧ v.y < p.y := by
        simpa [boxIn, box, q2] using ht
      omega
    have hq : shape.qualifies p v = true := by
      simp [shape, htq1.1, htq1.2.1]
    simp [h1, h2f, hq]
  · by_cases h2 : boxIn q2 v = true
    · have htq2 : p.x ≤ v.x ∧ v.x < p.x + 1 ∧ -b ≤ v.y ∧ v.y < p.y := by
        simpa [boxIn, box, q2] using h2
      have hq : shape.qualifies p v = true := by
        simp [shape]
        right
        constructor
        · have heq : v.x = p.x := by omega
          simp [heq]
        · exact htq2.2.2.2
      simp [h1, h2, hq]
    · have hq : shape.qualifies p v = false := by
        apply Bool.eq_false_of_ne_true
        intro ht
        have ht' : (((p.x - 3 ≤ v.x && v.x < p.x) = true) ||
            ((v.x == p.x && v.y < p.y) = true)) := by
          simpa [shape] using ht
        simp only [Bool.and_eq_true, Bool.or_eq_true, decide_eq_true_eq] at ht'
        rcases ht' with hfirst | hsecond
        · have hyhi : v.y < b + 1 := by omega
          have hb1 : boxIn q1 v = true := by
            simp [boxIn, box, q1, hfirst.1, hfirst.2, hv.2.2.1, hyhi]
          exact h1 hb1
        · have hxlow : p.x ≤ v.x := by
            have heq : v.x = p.x := by simpa using hsecond.1
            omega
          have hxhigh : v.x < p.x + 1 := by
            have heq : v.x = p.x := by simpa using hsecond.1
            omega
          have hb2 : boxIn q2 v = true := by
            simp [boxIn, box, q2, hxlow, hxhigh, hv.2.2.1, hsecond.2]
          exact h2 hb2
      simp [h1, h2, hq]

private theorem atPoint_compile_units (b : Int) (ps : List Point) (v : Point)
    (hv : bounded b v) :
    atPoint (compile shape b (ps.map (fun p => (p, (1 : Int))))) v =
      (ps.map (fun p => if shape.qualifies p v then (1 : Int) else 0)).sum := by
  unfold compile
  induction ps with
  | nil => simp [atPoint, prefixMass]
  | cons p ps ih =>
      simp only [List.map_cons, List.flatMap_cons, List.sum_cons]
      rw [atPoint_append, ih, atPoint_single b p v hv]

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

theorem sound_PriorTradingDay_UnionAnti : UnionAnti.Goal := by
  intro b ps rs hb
  dsimp only
  apply List.filter_congr
  intro r hr
  have hv := hb r hr
  rw [atPoint_compile_units b ps r.1 hv]
  rw [sum_indicator_any_zero]
  rfl

end TinylambOpt.ResearchProofs.Atlas.Goal_PriorTradingDay_UnionAnti
