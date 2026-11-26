import TinylambOpt.ResearchGoals.Atlas.RangePlans

/-!
An ordered access path needs to preserve the rows and boundaries selected by the SQL
range predicate.

Proved

- `LexSuccessor.UnionAnti` — `∀ b (ps : List Point) (rs : Rows), (∀ r ∈ rs, bounded b
  r.1) → let es := compile shape b (ps.map (fun p => (p,1))) rs.filter (fun r =>
  !covered shape ps r.1) = rs.filter (fun r => atPoint es r.1 == 0)`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_LexSuccessor_UnionAnti

open TinylambOpt.ResearchGoals.Atlas.RangePlans
open TinylambOpt.ResearchGoals.Atlas.RangePlans.LexSuccessor

/-! The compiled probe events form a signed tiling.  For a bounded build row the
    two stencil boxes are exactly the two arms of the lexicographic successor
    test and are disjoint, so their weights sum to the test's indicator.  The
    compiled sum therefore counts the covering probes and is zero iff none
    cover, even with duplicate probes. -/

private def inBox (q : Box) (r : Point) : Bool :=
  q.x0 ≤ r.x && r.x < q.x1 && q.y0 ≤ r.y && r.y < q.y1

private theorem inBox_iff (q : Box) (r : Point) :
    inBox q r = true ↔ q.x0 ≤ r.x ∧ r.x < q.x1 ∧ q.y0 ≤ r.y ∧ r.y < q.y1 := by
  unfold inBox
  simp only [Bool.and_eq_true]
  constructor
  · intro h
    exact ⟨of_decide_eq_true h.1.1.1, of_decide_eq_true h.1.1.2,
      of_decide_eq_true h.1.2, of_decide_eq_true h.2⟩
  · intro h
    exact And.intro (And.intro (And.intro (decide_eq_true h.1) (decide_eq_true h.2.1))
      (decide_eq_true h.2.2.1)) (decide_eq_true h.2.2.2)

private theorem prefixMass_cons (r : Point × Int) (x y : Int) (rs : Rows) :
    prefixMass (r :: rs) x y = (if (r.1.x < x && r.1.y < y) = true then r.2 else 0) + prefixMass rs x y := by
  unfold prefixMass
  simp only [List.filter_cons]
  by_cases h : (r.1.x < x && r.1.y < y) = true
  · rw [ite_eq_left h]; simp [h]
  · rw [ite_eq_right h]; simp [h]

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
    atPoint (events q w) r = if inBox q r then w * q.coeff else 0 := by
  unfold events
  generalize hz : w * q.coeff = z
  by_cases hb : q.x0 < q.x1 && q.y0 < q.y1
  · have hbprop : q.x0 < q.x1 ∧ q.y0 < q.y1 := by
      simpa only [Bool.and_eq_true, decide_eq_true_eq] using hb
    simp [hb, atPoint, prefixMass]
    by_cases hx0 : q.x0 < r.x + 1 <;> by_cases hx1 : q.x1 < r.x + 1 <;>
      by_cases hy0 : q.y0 < r.y + 1 <;> by_cases hy1 : q.y1 < r.y + 1 <;>
      simp [hx0, hx1, hy0, hy1]
    all_goals (try simp only [inBox_iff]) <;> (try intro hi) <;> (try split) <;>
      (try (rename_i hbox; rcases hbox with ⟨hxlo,hxhi,hylo,hyhi⟩)) <;>
      (try (rcases hi with ⟨hxlo,hxhi,hylo,hyhi⟩)) <;>
      (try (
        have hnp : ¬ (q.x0 ≤ r.x ∧ r.x < q.x1 ∧ q.y0 ≤ r.y ∧ r.y < q.y1) := by
          intro hp
          have ht : inBox q r = true := (inBox_iff q r).mpr hp
          rw [hi] at ht
          contradiction
        simp only [not_and] at hnp)) <;>
      (try rw [Int.add_neg_cancel_left]) <;> omega
  · have hbprop : ¬ (q.x0 < q.x1 ∧ q.y0 < q.y1) := by
      intro hh
      apply hb
      simp only [Bool.and_eq_true, decide_eq_true_eq]
      exact hh
    simp [hb, atPoint, prefixMass]
    (try simp only [inBox_iff])
    omega

private theorem atPoint_flatMap_events (ss : List Box) (w : Int) (r : Point) :
    atPoint (ss.flatMap (events · w)) r =
      (ss.map (fun q => if inBox q r then w * q.coeff else 0)).sum := by
  induction ss with
  | nil => simp [atPoint, prefixMass]
  | cons q qs ih =>
      simp only [List.flatMap_cons]
      rw [atPoint_append, atPoint_events, ih]
      simp

private def stencilWeight (s : Shape) (b : Int) (p : Point) (r : Point) : Int :=
  ((s.stencil b p).map (fun q => if inBox q r then q.coeff else 0)).sum

private theorem stencilWeight_scale (st : List Box) (w : Int) (r : Point) :
    (st.map (fun q => if inBox q r then w * q.coeff else 0)).sum =
      w * ((st.map (fun q => if inBox q r then q.coeff else 0)).sum) := by
  induction st with
  | nil => simp
  | cons q qs ih =>
      by_cases h : inBox q r = true
      · simp only [h, ite_true, List.map_cons, List.sum_cons, ih]
        rw [Int.mul_add]
      · simp [h, ih]

private theorem atPoint_compile (s : Shape) (b : Int) (ps : Rows) (r : Point) :
    atPoint (compile s b ps) r =
      (ps.map (fun (p,w) => w * stencilWeight s b p r)).sum := by
  induction ps with
  | nil => simp [compile, atPoint, prefixMass]
  | cons pw ps ih =>
      rcases pw with ⟨p,w⟩
      simp only [compile, List.flatMap_cons, List.map_cons, List.sum_cons] at ih ⊢
      rw [atPoint_append, atPoint_flatMap_events, stencilWeight_scale, ih]
      rfl

private theorem sum_indicator_nonneg (ps : List Point) (f : Point → Bool) :
    0 ≤ (ps.map (fun p => if f p then (1 : Int) else 0)).sum := by
  induction ps with
  | nil => simp
  | cons p ps ih =>
      simp only [List.map_cons, List.sum_cons]
      split <;> omega

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

private theorem unionAnti_sound (s : Shape) (b : Int) (ps : List Point) (rs : Rows)
    (hb : ∀ r ∈ rs, bounded b r.1)
    (hst : ∀ p v, bounded b v → stencilWeight s b p v = if s.qualifies p v then (1 : Int) else 0) :
    let es := compile s b (ps.map (fun p => (p,1)))
    rs.filter (fun r => !covered s ps r.1) = rs.filter (fun r => atPoint es r.1 == 0) := by
  apply List.filter_congr
  intro r hr
  have hbr := hb r hr
  have hc : atPoint (compile s b (ps.map (fun p => (p,1)))) r.1 =
      (ps.map (fun p => stencilWeight s b p r.1)).sum := by
    rw [atPoint_compile, List.map_map]
    apply congrArg List.sum
    apply List.map_congr_left
    intro p hp
    simp
  rw [hc]
  have hw : (ps.map (fun p => stencilWeight s b p r.1)) =
      (ps.map (fun p => if s.qualifies p r.1 then (1 : Int) else 0)) := by
    apply List.map_congr_left
    intro p hp
    exact hst p r.1 hbr
  rw [hw, sum_indicator_any_zero]
  cases hcov : covered s ps r.1
  · have hfalse : ps.any (fun p => s.qualifies p r.1) = false := by
      simpa only [covered] using hcov
    simp [hfalse]
  · have htrue : ps.any (fun p => s.qualifies p r.1) = true := by
      simpa only [covered] using hcov
    simp [htrue]

private theorem stencil_id (b : Int) (p v : Point) (hv : bounded b v) :
    ((shape.stencil b p).map (fun q => if inBox q v then q.coeff else 0)).sum =
      if shape.qualifies p v then (1 : Int) else 0 := by
  simp [shape, inBox, box, bounded] at hv ⊢
  omega

theorem sound_LexSuccessor_UnionAnti : UnionAnti.Goal := by
  intro b ps rs hb
  exact unionAnti_sound shape b ps rs hb (fun p v hv => stencil_id b p v hv)

end TinylambOpt.ResearchProofs.Atlas.Goal_LexSuccessor_UnionAnti
