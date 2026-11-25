import TinylambOpt.ResearchGoals.Atlas.RangePlans

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_RingNeighborhood_UnionAnti

open TinylambOpt.ResearchGoals.Atlas.RangePlans
open TinylambOpt.ResearchGoals.Atlas.RangePlans.RingNeighborhood

/-! Truth check on small inputs: build rows bounded by `b = 2`, probes drawn
    from a grid that includes points outside the bound. -/
private def coord : List Int := (List.range 11).map (fun i => Int.ofNat i - 5)
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

private theorem atPoint_flatMap_events (ss : List Box) (w : Int) (r : Point) :
    atPoint (ss.flatMap (events · w)) r =
      (ss.map (fun q => if boxIn q r then w * q.coeff else 0)).sum := by
  induction ss with
  | nil => simp [atPoint, prefixMass]
  | cons q qs ih =>
      simp only [List.flatMap_cons]
      rw [atPoint_append, atPoint_events, ih]
      simp

/-! For the ring shape the signed stencil evaluates to exactly the
    indicator of `qualifies`: the outer box contributes +1 for `d ≤ 5` and
    the inner (negative) box contributes -1 for `d ≤ 1`. -/
private theorem stencil_id (b : Int) (p v : Point) :
    ((shape.stencil b p).map (fun q => if boxIn q v then q.coeff else 0)).sum =
      if shape.qualifies p v then (1 : Int) else 0 := by
  rw [show shape.stencil b p =
      [box (p.x - 5) (p.x + 6) (p.y - 5) (p.y + 6),
       box (p.x - 1) (p.x + 2) (p.y - 1) (p.y + 2) (-1)] by rfl]
  simp
  change (if boxIn (box (p.x - 5) (p.x + 6) (p.y - 5) (p.y + 6)) v then (1 : Int) else 0) +
      (if boxIn (box (p.x - 1) (p.x + 2) (p.y - 1) (p.y + 2) (-1)) v then (-1 : Int) else 0) =
      if shape.qualifies p v then (1 : Int) else 0
  let d := max (v.x - p.x).natAbs (v.y - p.y).natAbs
  have houter : boxIn (box (p.x - 5) (p.x + 6) (p.y - 5) (p.y + 6)) v = true ↔ d ≤ 5 := by
    dsimp [d, boxIn, box]
    simp only [decide_eq_true_eq]
    omega
  have hinner : boxIn (box (p.x - 1) (p.x + 2) (p.y - 1) (p.y + 2) (-1)) v = true ↔ d ≤ 1 := by
    dsimp [d, boxIn, box]
    simp only [decide_eq_true_eq]
    omega
  by_cases hd1 : d ≤ 1
  · have ho : boxIn (box (p.x - 5) (p.x + 6) (p.y - 5) (p.y + 6)) v = true :=
      houter.mpr (by omega)
    have hi : boxIn (box (p.x - 1) (p.x + 2) (p.y - 1) (p.y + 2) (-1)) v = true :=
      hinner.mpr hd1
    have hq : shape.qualifies p v = false := by
      simp [shape]
      omega
    simp [ho, hi, hq]
  · by_cases hd5 : d ≤ 5
    · have ho : boxIn (box (p.x - 5) (p.x + 6) (p.y - 5) (p.y + 6)) v = true :=
        houter.mpr hd5
      have hi : boxIn (box (p.x - 1) (p.x + 2) (p.y - 1) (p.y + 2) (-1)) v = false := by
        apply Bool.eq_false_of_ne_true
        intro h
        exact hd1 (hinner.mp h)
      have hq : shape.qualifies p v = true := by
        simp [shape]
        omega
      simp [ho, hi, hq]
    · have ho : boxIn (box (p.x - 5) (p.x + 6) (p.y - 5) (p.y + 6)) v = false := by
        apply Bool.eq_false_of_ne_true
        intro h
        exact hd5 (houter.mp h)
      have hi : boxIn (box (p.x - 1) (p.x + 2) (p.y - 1) (p.y + 2) (-1)) v = false := by
        apply Bool.eq_false_of_ne_true
        intro h
        exact hd1 (hinner.mp h)
      have hq : shape.qualifies p v = false := by
        simp [shape]
        omega
      simp [ho, hi, hq]

private theorem atPoint_compile_units (b : Int) (ps : List Point) (v : Point) :
    atPoint (compile shape b (ps.map (fun p => (p, (1 : Int))))) v =
      (ps.map (fun p => if shape.qualifies p v then (1 : Int) else 0)).sum := by
  unfold compile
  induction ps with
  | nil => simp [atPoint, prefixMass]
  | cons p ps ih =>
      simp only [List.map_cons, List.flatMap_cons]
      rw [atPoint_append, ih, atPoint_flatMap_events]
      simp
      rw [stencil_id b p v]

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

theorem sound_RingNeighborhood_UnionAnti : RingNeighborhood.UnionAnti.Goal := by
  intro b ps rs hb
  dsimp only
  apply List.filter_congr
  intro r hr
  rw [atPoint_compile_units b ps r.1]
  rw [sum_indicator_any_zero]
  rfl

end TinylambOpt.ResearchProofs.Atlas.Goal_RingNeighborhood_UnionAnti
