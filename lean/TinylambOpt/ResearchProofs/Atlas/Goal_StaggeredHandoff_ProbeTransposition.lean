import TinylambOpt.ResearchGoals.Atlas.RangePlans

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_StaggeredHandoff_ProbeTransposition

open TinylambOpt.ResearchGoals.Atlas.RangePlans
open TinylambOpt.ResearchGoals.Atlas.RangePlans.StaggeredHandoff

/-! Finite-list algebra shared by the scan side and the event side. -/

private def boxIn (q : Box) (v : Point) : Bool :=
  decide (q.x0 ≤ v.x ∧ v.x < q.x1 ∧ q.y0 ≤ v.y ∧ v.y < q.y1)

private theorem prefixMass_cons (r : Point × Int) (rs : Rows) (x y : Int) :
    prefixMass (r :: rs) x y =
      (if r.1.x < x && r.1.y < y then r.2 else 0) + prefixMass rs x y := by
  by_cases h : r.1.x < x && r.1.y < y <;> simp [prefixMass, h]

private theorem prefixMass_append (rs ts : Rows) (x y : Int) :
    prefixMass (rs ++ ts) x y = prefixMass rs x y + prefixMass ts x y := by
  induction rs with
  | nil => simp [prefixMass]
  | cons r rs ih =>
      rw [List.cons_append, prefixMass_cons, prefixMass_cons, ih]
      omega

private theorem sum_filter_eq_sum_if (rs : Rows) (f : Point × Int → Bool) :
    ((rs.filter f).map Prod.snd).sum =
      (rs.map (fun r => if f r then r.2 else 0)).sum := by
  induction rs with
  | nil => rfl
  | cons r rs ih =>
      by_cases h : f r = true <;> simp [h, ih]

private theorem sum_congr {α : Type} (xs : List α) (f g : α → Int)
    (h : ∀ x ∈ xs, f x = g x) : (xs.map f).sum = (xs.map g).sum :=
  congrArg List.sum (List.map_congr_left h)

private theorem sum_map_mul_const {α : Type} (xs : List α) (f : α → Int) (w : Int) :
    (xs.map (fun x => w * f x)).sum = w * (xs.map f).sum := by
  induction xs with
  | nil => simp
  | cons x xs ih => simp [ih, Int.mul_add]

private theorem sum_map_add {α : Type} (xs : List α) (f g : α → Int) :
    (xs.map (fun x => f x + g x)).sum = (xs.map f).sum + (xs.map g).sum := by
  induction xs with
  | nil => simp
  | cons x xs ih => simp [ih] <;> omega

private theorem sum_map_zero {β : Type} (bs : List β) :
    (bs.map (fun _ => (0 : Int))).sum = 0 := by
  induction bs with
  | nil => rfl
  | cons b bs ih => simp [ih]

private theorem sum_swap {α β : Type} (as : List α) (bs : List β) (f : α → β → Int) :
    (as.map (fun a => (bs.map (fun b => f a b)).sum)).sum =
      (bs.map (fun b => (as.map (fun a => f a b)).sum)).sum := by
  induction as with
  | nil => simp [sum_map_zero]
  | cons a as ih =>
      simp only [List.map_cons, List.sum_cons]
      rw [ih, ← sum_map_add]

/-! Events of one rectangle transpose into its indicator. -/

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

/-! The single-box stencil of StaggeredHandoff. -/

private def sb (b : Int) (p : Point) : Box := box (p.x + 1) p.y (p.y + 1) (b + 1)

private theorem atPoint_stencil_events (b : Int) (p : Point) (w : Int) (r : Point) :
    atPoint ((shape.stencil b p).flatMap (events · w)) r =
      if boxIn (sb b p) r then w else 0 := by
  have hs : (shape.stencil b p).flatMap (events · w) = events (sb b p) w := by
    simp [shape, sb]
  rw [hs, atPoint_events]
  simp [sb, box]
  rfl

private theorem atPoint_compile_staggered (b : Int) (ps : Rows) (r : Point) :
    atPoint (compile shape b ps) r =
      (ps.map (fun pr => if boxIn (sb b pr.1) r then pr.2 else 0)).sum := by
  unfold compile
  induction ps with
  | nil => simp [atPoint, prefixMass]
  | cons pr ps ih =>
      rw [List.flatMap_cons, atPoint_append, atPoint_stencil_events, ih]
      simp

private theorem qualifies_eq_boxIn (b : Int) (p v : Point) (hv : bounded b v) :
    shape.qualifies p v = boxIn (sb b p) v := by
  have hv' : -b ≤ v.x ∧ v.x ≤ b ∧ -b ≤ v.y ∧ v.y ≤ b := by
    simpa [bounded] using hv
  by_cases hq : shape.qualifies p v
  · have hq' : p.x < v.x ∧ v.x < p.y ∧ p.y < v.y := by
      simpa [shape, and_assoc] using hq
    have hbb : boxIn (sb b p) v = true := by
      change decide (p.x + 1 ≤ v.x ∧ v.x < p.y ∧ p.y + 1 ≤ v.y ∧ v.y < b + 1) = true
      exact decide_eq_true (by omega)
    simp [hq, hbb]
  · have hq' : ¬ (p.x < v.x ∧ v.x < p.y ∧ p.y < v.y) := by
      intro h
      apply hq
      simpa [shape, and_assoc] using h
    have hbb : boxIn (sb b p) v = false := by
      change decide (p.x + 1 ≤ v.x ∧ v.x < p.y ∧ p.y + 1 ≤ v.y ∧ v.y < b + 1) = false
      exact decide_eq_false_iff_not.mpr (by
        intro hbox
        apply hq'
        omega)
    simp [hq, hbb]

private theorem scan_eq_boxsum (b : Int) (p : Point) (rs : Rows)
    (hb : ∀ r ∈ rs, bounded b r.1) :
    scan shape p rs =
      (rs.map (fun r => if boxIn (sb b p) r.1 then r.2 else 0)).sum := by
  rw [scan, sum_filter_eq_sum_if]
  apply sum_congr
  intro r hr
  rw [qualifies_eq_boxIn b p r.1 (hb r hr)]

theorem sound_StaggeredHandoff_ProbeTransposition :
    TinylambOpt.ResearchGoals.Atlas.RangePlans.StaggeredHandoff.ProbeTransposition.Goal := by
  intro b ps rs hb
  have hleft : ps.map (fun (p, w) => w * scan shape p rs) =
      ps.map (fun pr => pr.2 *
        (rs.map (fun rr => if boxIn (sb b pr.1) rr.1 then rr.2 else 0)).sum) := by
    apply List.map_congr_left
    rintro ⟨p, w⟩ _
    dsimp
    rw [scan_eq_boxsum b p rs hb]
  have hright : rs.map (fun (r, w) => w * atPoint (compile shape b ps) r) =
      rs.map (fun rr => rr.2 *
        (ps.map (fun pr => if boxIn (sb b pr.1) rr.1 then pr.2 else 0)).sum) := by
    apply List.map_congr_left
    rintro ⟨r, w⟩ _
    dsimp
    rw [atPoint_compile_staggered]
  rw [hleft, hright]
  have hs1 : (ps.map (fun pr => pr.2 *
      (rs.map (fun rr => if boxIn (sb b pr.1) rr.1 then rr.2 else 0)).sum)).sum =
      (ps.map (fun pr => (rs.map (fun rr =>
        if boxIn (sb b pr.1) rr.1 then pr.2 * rr.2 else 0)).sum)).sum := by
    apply congrArg List.sum
    apply List.map_congr_left
    intro pr _
    calc
      pr.2 * (rs.map (fun rr => if boxIn (sb b pr.1) rr.1 then rr.2 else 0)).sum =
          (rs.map (fun rr => pr.2 * (if boxIn (sb b pr.1) rr.1 then rr.2 else 0))).sum :=
        (sum_map_mul_const rs (fun rr => if boxIn (sb b pr.1) rr.1 then rr.2 else 0) pr.2).symm
      _ = (rs.map (fun rr => if boxIn (sb b pr.1) rr.1 then pr.2 * rr.2 else 0)).sum := by
        apply congrArg List.sum
        apply List.map_congr_left
        intro rr _
        by_cases h : boxIn (sb b pr.1) rr.1 <;> simp [h]
  have hswap : (ps.map (fun pr => (rs.map (fun rr =>
      if boxIn (sb b pr.1) rr.1 then pr.2 * rr.2 else 0)).sum)).sum =
      (rs.map (fun rr => (ps.map (fun pr =>
        if boxIn (sb b pr.1) rr.1 then pr.2 * rr.2 else 0)).sum)).sum := by
    exact sum_swap ps rs (fun pr rr => if boxIn (sb b pr.1) rr.1 then pr.2 * rr.2 else 0)
  rw [hs1, hswap]
  apply sum_congr
  intro rr _
  calc
    (ps.map (fun pr => if boxIn (sb b pr.1) rr.1 then pr.2 * rr.2 else 0)).sum =
        (ps.map (fun pr => rr.2 * (if boxIn (sb b pr.1) rr.1 then pr.2 else 0))).sum := by
      apply congrArg List.sum
      apply List.map_congr_left
      intro pr _
      by_cases h : boxIn (sb b pr.1) rr.1 <;> simp [h, Int.mul_comm]
    _ = rr.2 * (ps.map (fun pr => if boxIn (sb b pr.1) rr.1 then pr.2 else 0)).sum :=
      sum_map_mul_const ps (fun pr => if boxIn (sb b pr.1) rr.1 then pr.2 else 0) rr.2

end TinylambOpt.ResearchProofs.Atlas.Goal_StaggeredHandoff_ProbeTransposition
