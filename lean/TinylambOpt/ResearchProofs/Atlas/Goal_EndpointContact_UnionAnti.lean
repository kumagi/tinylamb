import TinylambOpt.ResearchGoals.Atlas.RangePlans

/-!
An ordered access path needs to preserve the rows and boundaries selected by the SQL
range predicate.

Proved

- `EndpointContact.UnionAnti` — `∀ b (ps : List Point) (rs : Rows), (∀ r ∈ rs, bounded
  b r.1) → let es := compile shape b (ps.map (fun p => (p,1))) rs.filter (fun r =>
  !covered shape ps r.1) = rs.filter (fun r => atPoint es r.1 == 0)`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_EndpointContact_UnionAnti

open TinylambOpt.ResearchGoals.Atlas.RangePlans
open TinylambOpt.ResearchGoals.Atlas.RangePlans.EndpointContact

/-! Small exhaustive check: build rows bounded by b = 2, probes may lie
    outside the bound because the stencil only uses the probe endpoints. -/
private def coord : List Int := (List.range 7).map (fun i => Int.ofNat i - 3)
private def pts : List Point := coord.flatMap (fun x => coord.map (fun y => ⟨x, y⟩))

private def rows1 : List (Point × Int) :=
  pts.flatMap (fun p => [(p, -1), (p, 0), (p, 1)])

private def probeSets : List (List Point) :=
  [[]] ++ pts.map (fun p => [p]) ++
    (pts.take 10).flatMap (fun p => (pts.take 10).map (fun q => [p, q]))

private def okRow (b : Int) (r : Point × Int) : Bool :=
  -b ≤ r.1.x && r.1.x ≤ b && -b ≤ r.1.y && r.1.y ≤ b

private def goalHolds (b : Int) (ps : List Point) (rs : Rows) : Bool :=
  rs.all (okRow b) →
    let es := compile shape b (ps.map (fun p => (p, 1)))
    rs.filter (fun r => !covered shape ps r.1) == rs.filter (fun r => atPoint es r.1 == 0)

#eval probeSets.all (fun ps => rows1.all (fun r => goalHolds 2 ps [r]))

/-! Event transposition. A batch of signed corner events evaluates, at a
    build row, to the signed coefficient exactly when the row lies in the
    half-open box. -/
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

/-! For a bounded build row, each stencil box membership reduces to an
    endpoint equality (or a pair of them for the negative intersection box). -/
private theorem box1_iff (b : Int) (p v : Point) (hv : bounded b v) :
    boxIn (box (p.y) (p.y + 1) (-b) (b + 1)) v = (v.x == p.y) := by
  unfold bounded at hv
  unfold boxIn box
  by_cases h : v.x = p.y
  · have ht : decide (p.y ≤ v.x ∧ v.x < p.y + 1 ∧ -b ≤ v.y ∧ v.y < b + 1) = true := by
      apply decide_eq_true_eq.mpr
      omega
    have hr : (v.x == p.y) = true := by simp [h]
    rw [ht, hr]
  · have hf : decide (p.y ≤ v.x ∧ v.x < p.y + 1 ∧ -b ≤ v.y ∧ v.y < b + 1) = false := by
      apply decide_eq_false_iff_not.mpr
      intro hc
      omega
    have hr : (v.x == p.y) = false := by simp [h]
    rw [hf, hr]

private theorem box2_iff (b : Int) (p v : Point) (hv : bounded b v) :
    boxIn (box (-b) (b + 1) (p.x) (p.x + 1)) v = (v.y == p.x) := by
  unfold bounded at hv
  unfold boxIn box
  by_cases h : v.y = p.x
  · have ht : decide (-b ≤ v.x ∧ v.x < b + 1 ∧ p.x ≤ v.y ∧ v.y < p.x + 1) = true := by
      apply decide_eq_true_eq.mpr
      omega
    have hr : (v.y == p.x) = true := by simp [h]
    rw [ht, hr]
  · have hf : decide (-b ≤ v.x ∧ v.x < b + 1 ∧ p.x ≤ v.y ∧ v.y < p.x + 1) = false := by
      apply decide_eq_false_iff_not.mpr
      intro hc
      omega
    have hr : (v.y == p.x) = false := by simp [h]
    rw [hf, hr]

private theorem box3_iff (p v : Point) :
    boxIn (box (p.y) (p.y + 1) (p.x) (p.x + 1) (-1)) v = (v.x == p.y && v.y == p.x) := by
  unfold boxIn box
  by_cases hx : v.x = p.y <;> by_cases hy : v.y = p.x
  · have ht : decide (p.y ≤ v.x ∧ v.x < p.y + 1 ∧ p.x ≤ v.y ∧ v.y < p.x + 1) = true := by
      apply decide_eq_true_eq.mpr
      omega
    have hr : (v.x == p.y && v.y == p.x) = true := by simp [hx, hy]
    rw [ht, hr]
  · have hf : decide (p.y ≤ v.x ∧ v.x < p.y + 1 ∧ p.x ≤ v.y ∧ v.y < p.x + 1) = false := by
      apply decide_eq_false_iff_not.mpr
      intro hc
      omega
    have hr : (v.x == p.y && v.y == p.x) = false := by simp [hx, hy]
    rw [hf, hr]
  · have hf : decide (p.y ≤ v.x ∧ v.x < p.y + 1 ∧ p.x ≤ v.y ∧ v.y < p.x + 1) = false := by
      apply decide_eq_false_iff_not.mpr
      intro hc
      omega
    have hr : (v.x == p.y && v.y == p.x) = false := by simp [hx, hy]
    rw [hf, hr]
  · have hf : decide (p.y ≤ v.x ∧ v.x < p.y + 1 ∧ p.x ≤ v.y ∧ v.y < p.x + 1) = false := by
      apply decide_eq_false_iff_not.mpr
      intro hc
      omega
    have hr : (v.x == p.y && v.y == p.x) = false := by simp [hx, hy]
    rw [hf, hr]

private theorem atPoint_stencil (b : Int) (p v : Point) (hv : bounded b v) :
    atPoint ((shape.stencil b p).flatMap (fun q => events q (1 : Int))) v =
      (if shape.qualifies p v then (1 : Int) else 0) := by
  rw [show (shape.stencil b p).flatMap (fun q => events q (1 : Int)) =
      events (box (p.y) (p.y + 1) (-b) (b + 1)) 1 ++
      events (box (-b) (b + 1) (p.x) (p.x + 1)) 1 ++
      events (box (p.y) (p.y + 1) (p.x) (p.x + 1) (-1)) 1 by simp [shape]]
  rw [atPoint_append, atPoint_append]
  rw [atPoint_events, atPoint_events, atPoint_events]
  rw [box1_iff b p v hv, box2_iff b p v hv, box3_iff p v]
  simp [box]
  by_cases hx : v.x = p.y <;> by_cases hy : v.y = p.x <;> simp [hx, hy, shape]

private theorem atPoint_compile_units (b : Int) (ps : List Point) (v : Point)
    (hv : bounded b v) :
    atPoint (compile shape b (ps.map (fun p => (p, (1 : Int))))) v =
      (ps.map (fun p => if shape.qualifies p v then (1 : Int) else 0)).sum := by
  unfold compile
  induction ps with
  | nil => simp [atPoint, prefixMass]
  | cons p ps ih =>
      simp only [List.map_cons, List.flatMap_cons]
      rw [atPoint_append, ih, atPoint_stencil b p v hv]
      simp

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

theorem sound_EndpointContact_UnionAnti : UnionAnti.Goal := by
  intro b ps rs hb
  dsimp only
  apply List.filter_congr
  intro r hr
  have hv := hb r hr
  rw [atPoint_compile_units b ps r.1 hv]
  rw [sum_indicator_any_zero]
  rfl

end TinylambOpt.ResearchProofs.Atlas.Goal_EndpointContact_UnionAnti
