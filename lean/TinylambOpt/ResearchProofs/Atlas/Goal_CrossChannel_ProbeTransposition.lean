import TinylambOpt.ResearchGoals.Atlas.RangePlans

/-!
An ordered access path needs to preserve the rows and boundaries selected by the SQL
range predicate.

Proved

- `CrossChannel.ProbeTransposition` — `∀ b (ps rs : Rows), (∀ r ∈ rs, bounded b r.1) →
  (ps.map (fun (p,w) => w * scan shape p rs)).sum = (rs.map (fun (r,w) => w * atPoint
  (compile shape b ps) r)).sum`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_CrossChannel_ProbeTransposition

open TinylambOpt.ResearchGoals.Atlas.RangePlans
open TinylambOpt.ResearchGoals.Atlas.RangePlans.CrossChannel

/-! Finite-list and rectangle algebra shared by the scan side and the event side. -/

private def inBox (q : Box) (v : Point) : Bool :=
  decide (q.x0 ≤ v.x ∧ v.x < q.x1 ∧ q.y0 ≤ v.y ∧ v.y < q.y1)

private theorem sum_filter_eq_sum_if (rs : Rows) (f : Point × Int → Bool)
    (g : Point × Int → Int) :
    ((rs.filter f).map g).sum = (rs.map (fun r => if f r then g r else 0)).sum := by
  induction rs with
  | nil => rfl
  | cons r rs ih =>
      by_cases h : f r = true <;> simp [h, ih]

private theorem prefixMass_cons (r : Point × Int) (rs : Rows) (x y : Int) :
    prefixMass (r :: rs) x y =
      (if (r.1.x < x && r.1.y < y) = true then r.2 else 0) + prefixMass rs x y := by
  unfold prefixMass
  simp only [List.filter_cons]
  by_cases h : (r.1.x < x && r.1.y < y) = true
  · rw [if_pos h]
    simp [h]
  · rw [if_neg h]
    simp [h]

private theorem prefixMass_append (es₁ es₂ : Rows) (x y : Int) :
    prefixMass (es₁ ++ es₂) x y = prefixMass es₁ x y + prefixMass es₂ x y := by
  induction es₁ with
  | nil => simp [prefixMass]
  | cons e es ih =>
      rw [List.cons_append, prefixMass_cons, prefixMass_cons, ih]
      omega

private theorem sum_map_zero {β : Type} (bs : List β) :
    (bs.map (fun _ => (0 : Int))).sum = 0 := by
  induction bs with
  | nil => rfl
  | cons b bs ih => simp [ih]

private theorem sum_map_add {β : Type} (bs : List β) (f g : β → Int) :
    (bs.map (fun b => f b + g b)).sum = (bs.map f).sum + (bs.map g).sum := by
  induction bs with
  | nil => simp
  | cons b bs ih => simp [ih]; omega

private theorem sum_map_mul_left {β : Type} (bs : List β) (f : β → Int) (w : Int) :
    (bs.map (fun b => w * f b)).sum = w * (bs.map f).sum := by
  induction bs with
  | nil => simp
  | cons b bs ih => simp [ih, Int.mul_add]

private theorem sum_swap {α β : Type} (as : List α) (bs : List β) (f : α → β → Int) :
    (as.map (fun a => (bs.map (fun b => f a b)).sum)).sum =
      (bs.map (fun b => (as.map (fun a => f a b)).sum)).sum := by
  induction as with
  | nil => simp [sum_map_zero]
  | cons a as ih =>
      simp only [List.map_cons, List.sum_cons]
      rw [ih, ← sum_map_add]

/-! Transposing one probe rectangle into boundary events. -/

private theorem atPoint_append (es₁ es₂ : Rows) (r : Point) :
    atPoint (es₁ ++ es₂) r = atPoint es₁ r + atPoint es₂ r := by
  simp [atPoint, prefixMass_append]

private theorem atPoint_events (q : Box) (w : Int) (r : Point) :
    atPoint (events q w) r = if inBox q r then w * q.coeff else 0 := by
  unfold events
  by_cases hb : (q.x0 < q.x1 && q.y0 < q.y1) = true
  · simp [hb, atPoint, prefixMass]
    by_cases hx0 : q.x0 < r.x + 1 <;> by_cases hx1 : q.x1 < r.x + 1 <;>
      by_cases hy0 : q.y0 < r.y + 1 <;> by_cases hy1 : q.y1 < r.y + 1 <;>
      by_cases hxl : q.x0 ≤ r.x <;> by_cases hyl : q.y0 ≤ r.y <;>
      simp [inBox, decide_eq_true_eq, hx0, hx1, hy0, hy1, hxl, hyl] <;> grind
  · have hb' : (q.x0 < q.x1 && q.y0 < q.y1) = false :=
      Bool.eq_false_of_ne_true hb
    simp [hb', atPoint, prefixMass]
    have hn : inBox q r = false := by
      unfold inBox
      apply Bool.eq_false_of_ne_true
      intro hbin
      simp only [decide_eq_true_eq] at hbin
      rcases hbin with ⟨hxlo, hxhi, hylo, hyhi⟩
      have : q.x0 < q.x1 ∧ q.y0 < q.y1 := by omega
      exact hb (by simp [this])
    simp [hn]

private theorem atPoint_flatMap_events (ss : List Box) (w : Int) (r : Point) :
    atPoint (ss.flatMap (events · w)) r =
      (ss.map (fun q => if inBox q r then w * q.coeff else 0)).sum := by
  induction ss with
  | nil => simp [atPoint, prefixMass]
  | cons q qs ih =>
      simp only [List.flatMap_cons]
      rw [atPoint_append, atPoint_events, ih]
      simp

private theorem compile_cons (s : Shape) (b : Int) (p : Point) (w : Int)
    (ps : Rows) :
    compile s b ((p, w) :: ps) =
      (s.stencil b p).flatMap (events · w) ++ compile s b ps := by
  simp [compile]

private def stencilWeight (s : Shape) (b : Int) (p v : Point) : Int :=
  ((s.stencil b p).map (fun q => if inBox q v then q.coeff else 0)).sum

private theorem stencilWeight_scale_list (ss : List Box) (v : Point) (w : Int) :
    (ss.map (fun q => if inBox q v then w * q.coeff else 0)).sum =
      w * (ss.map (fun q => if inBox q v then q.coeff else 0)).sum := by
  induction ss with
  | nil => simp
  | cons q qs ih =>
      by_cases h : inBox q v = true
      · simp only [List.map_cons, List.sum_cons, h, if_true, ih, Int.mul_add]
        try ac_rfl
      · simp [h, ih]

private theorem stencilWeight_scale (s : Shape) (b : Int) (p v : Point) (w : Int) :
    ((s.stencil b p).map (fun q => if inBox q v then w * q.coeff else 0)).sum =
      w * stencilWeight s b p v := by
  exact stencilWeight_scale_list (s.stencil b p) v w

private theorem atPoint_compile (s : Shape) (b : Int) (ps : Rows) (r : Point) :
    atPoint (compile s b ps) r =
      (ps.map (fun (q, w) => w * stencilWeight s b q r)).sum := by
  induction ps with
  | nil => simp [compile, atPoint, prefixMass]
  | cons pw ps ih =>
      rcases pw with ⟨p, w⟩
      rw [compile_cons, atPoint_append, atPoint_flatMap_events, ih]
      rw [stencilWeight_scale]
      simp

/-! The CrossChannel stencil is the indicator of its qualifying relation on bounded rows. -/

private theorem stencil_id (b : Int) (p v : Point) (hv : bounded b v) :
    stencilWeight shape b p v = if shape.qualifies p v then (1 : Int) else 0 := by
  simp [shape, box, stencilWeight, inBox, bounded] at hv ⊢
  omega

private theorem scan_eq_sum_indicators (s : Shape) (p : Point) (rs : Rows) :
    scan s p rs = (rs.map (fun r => r.2 * (if s.qualifies p r.1 then (1 : Int) else 0))).sum := by
  unfold scan
  rw [sum_filter_eq_sum_if]
  apply congrArg List.sum
  apply List.map_congr_left
  intro r hr
  by_cases h : s.qualifies p r.1 = true <;> simp [h]

private theorem probeTransposition_of_stencil (s : Shape)
    (hst : ∀ b p v, bounded b v →
      stencilWeight s b p v = if s.qualifies p v then (1 : Int) else 0) :
    ∀ b (ps rs : Rows), (∀ r ∈ rs, bounded b r.1) →
      (ps.map (fun (p, w) => w * scan s p rs)).sum =
        (rs.map (fun (r, w) => w * atPoint (compile s b ps) r)).sum := by
  intro b ps rs hb
  induction ps with
  | nil =>
      simp [compile, atPoint, prefixMass, Int.mul_zero]
      exact (sum_map_zero rs).symm
  | cons pr ps ih =>
      rcases pr with ⟨p, w⟩
      have hsplit : ∀ (r : Point), atPoint (compile s b ((p, w) :: ps)) r =
          atPoint (compile s b [(p, w)]) r + atPoint (compile s b ps) r := by
        intro r
        rw [compile_cons, atPoint_append]
        congr 1 <;> simp [compile]
      change w * scan s p rs + (ps.map (fun (p, w) => w * scan s p rs)).sum =
        (rs.map (fun (r, v) => v * atPoint (compile s b ((p, w) :: ps)) r)).sum
      have hm : rs.map (fun (r, v) => v * atPoint (compile s b ((p, w) :: ps)) r) =
          rs.map (fun (r, v) => v * atPoint (compile s b [(p, w)]) r +
            v * atPoint (compile s b ps) r) := by
        apply List.map_congr_left
        intro rr hrr
        rcases rr with ⟨r, v⟩
        simp only [hsplit r, Int.mul_add]
      rw [hm, sum_map_add]
      rw [ih]
      have hone : w * scan s p rs =
          (rs.map (fun (r, v) => v * atPoint (compile s b [(p, w)]) r)).sum := by
        have hat : ∀ r ∈ rs, atPoint (compile s b [(p, w)]) r.1 =
            w * (if s.qualifies p r.1 then (1 : Int) else 0) := by
          intro r hr
          have hrp := hb r hr
          rw [atPoint_compile]
          simp only [List.map_cons, List.map_nil, List.sum_cons, List.sum_nil, Int.add_zero]
          rw [hst b p r.1 hrp]
        have hat2 : rs.map (fun (r, v) => v * atPoint (compile s b [(p, w)]) r) =
            rs.map (fun (r, v) => v * (w * (if s.qualifies p r then (1 : Int) else 0))) := by
          apply List.map_congr_left
          intro rr hrr
          have hrp := hb rr hrr
          rcases rr with ⟨r, v⟩
          change v * atPoint (compile s b [(p, w)]) r = v * (w * (if s.qualifies p r then (1 : Int) else 0))
          rw [atPoint_compile]
          simp only [List.map_cons, List.map_nil, List.sum_cons, List.sum_nil, Int.add_zero]
          rw [hst b p r hrp]
        rw [hat2]
        have hcomm : rs.map (fun (r, v) => v * (w * (if s.qualifies p r then (1 : Int) else 0))) =
            rs.map (fun row => w * (row.2 * (if s.qualifies p row.1 then (1 : Int) else 0))) := by
          apply List.map_congr_left
          intro rr hrr
          rcases rr with ⟨r, v⟩
          ac_rfl
        rw [hcomm]
        rw [scan_eq_sum_indicators]
        exact (sum_map_mul_left rs (fun r => r.2 * (if s.qualifies p r.1 then (1 : Int) else 0)) w).symm
      rw [hone]

theorem sound_CrossChannel_ProbeTransposition :
    TinylambOpt.ResearchGoals.Atlas.RangePlans.CrossChannel.ProbeTransposition.Goal := by
  exact probeTransposition_of_stencil shape stencil_id

end TinylambOpt.ResearchProofs.Atlas.Goal_CrossChannel_ProbeTransposition
