import TinylambOpt.ResearchGoals.Atlas.RangePlans

/-!
An ordered access path needs to preserve the rows and boundaries selected by the SQL
range predicate.

Proved

- `PriceCorridor.UnionAnti` — `∀ b (ps : List Point) (rs : Rows), (∀ r ∈ rs, bounded b
  r.1) → let es := compile shape b (ps.map (fun p => (p,1))) rs.filter (fun r =>
  !covered shape ps r.1) = rs.filter (fun r => atPoint es r.1 == 0)`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_PriceCorridor_UnionAnti

open TinylambOpt.ResearchGoals.Atlas.RangePlans
open TinylambOpt.ResearchGoals.Atlas.RangePlans.PriceCorridor

/-! Scratch exhaustive check on small inputs (build rows bounded by b = 2,
    probes also outside that bound because this stencil ignores b). -/
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

/-! Event-transposition lemmas. A signed corner event batch evaluates, at a
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

/-! The corridor stencil ignores the bound: its box membership is exactly the
    predicate, since natAbs ≤ 3 is the closed interval [p.x-3, p.x+3]. -/
private theorem box_iff_qualifies (p v : Point) :
    boxIn (box (p.x - 3) (p.x + 4) (p.y - 2) (p.y + 6)) v = shape.qualifies p v := by
  apply Bool.eq_iff_iff.mpr
  unfold boxIn box shape
  simp only
  simp only [decide_eq_true_eq, Bool.and_eq_true]
  constructor <;> intro h <;> omega

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
          events (box (p.x - 3) (p.x + 4) (p.y - 2) (p.y + 6)) 1 := by
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

theorem sound_PriceCorridor_UnionAnti : UnionAnti.Goal := by
  intro b ps rs hb
  dsimp only
  apply List.filter_congr
  intro r hr
  rw [atPoint_compile_units b ps r.1]
  rw [sum_indicator_any_zero]
  rfl

end TinylambOpt.ResearchProofs.Atlas.Goal_PriceCorridor_UnionAnti
