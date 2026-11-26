import TinylambOpt.ResearchGoals.Atlas.RangePlans

/-!
An ordered access path needs to preserve the rows and boundaries selected by the SQL
range predicate.

Proved

- `ExactlyOneKey.PageVerdict` — `∀ b p g, valid g → (∀ r ∈ g.rows, bounded b r.1) →
  pageScan shape b p g = scan shape p g.rows`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_ExactlyOneKey_PageVerdict

open TinylambOpt.ResearchGoals.Atlas.RangePlans
open TinylambOpt.ResearchGoals.Atlas.RangePlans.ExactlyOneKey

private def inBox (q : Box) (v : Point) : Bool :=
  decide (q.x0 ≤ v.x ∧ v.x < q.x1 ∧ q.y0 ≤ v.y ∧ v.y < q.y1)

private theorem sum_filter_eq_sum_if (rs : Rows) (f : Point × Int → Bool)
    (g : Point × Int → Int) :
    ((rs.filter f).map g).sum = (rs.map (fun r => if f r then g r else 0)).sum := by
  induction rs with
  | nil => rfl
  | cons r rs ih =>
      by_cases h : f r = true <;> simp [h, ih]

private def stencilWeight (s : Shape) (b : Int) (p v : Point) : Int :=
  ((s.stencil b p).map (fun q => if inBox q v then q.coeff else 0)).sum

private theorem mul_sum_map_snd (rs : Rows) (c : Int) :
    (rs.map (fun r => c * r.2)).sum = c * (rs.map Prod.snd).sum := by
  induction rs with
  | nil => simp
  | cons r rs ih => simp [ih, Int.mul_add]

private theorem stencil_id (b : Int) (p v : Point) (hv : bounded b v) :
    stencilWeight shape b p v = if shape.qualifies p v then (1 : Int) else 0 := by
  simp [shape, box, stencilWeight, inBox, bounded] at hv ⊢
  by_cases hx : v.x = p.x <;> by_cases hy : v.y = p.y
  all_goals
    have hxb : (v.x == p.x) = decide (v.x = p.x) := by rfl
    have hyb : (v.y == p.y) = decide (v.y = p.y) := by rfl
    simp only [hxb, hyb, hx, hy, ite_true]
    repeat' split <;> simp_all <;> omega

/-- A uniform verdict for a box is either "disjoint from the page" (weight 0)
    or "covers the whole page" (weight `q.coeff`).  For any row inside the page
    the box membership is therefore exactly the verdict's weight. -/
private theorem uniform_coeff_eq (g : Page) (q : Box) (r : Point)
    (hr : g.lower.x ≤ r.x ∧ r.x ≤ g.upper.x ∧ g.lower.y ≤ r.y ∧ r.y ≤ g.upper.y) :
    match uniform g q with
    | some c => c = if inBox q r then q.coeff else 0
    | none => True := by
  unfold uniform
  by_cases hdis : (q.x1 ≤ q.x0 || q.y1 ≤ q.y0 || g.upper.x < q.x0 || q.x1 ≤ g.lower.x ||
      g.upper.y < q.y0 || q.y1 ≤ g.lower.y) = true
  · simp [hdis]
    by_cases hin : inBox q r = true
    · unfold inBox at hin
      simp only [decide_eq_true_eq] at hin
      simp only [Bool.or_eq_true, decide_eq_true_eq] at hdis
      omega
    · simp [hin]
  · by_cases hfull : (q.x0 ≤ g.lower.x && g.upper.x < q.x1 && q.y0 ≤ g.lower.y &&
      g.upper.y < q.y1) = true
    · simp [hdis, hfull]
      have hin : inBox q r = true := by
        unfold inBox
        simp only [decide_eq_true_eq]
        simp only [Bool.and_eq_true, decide_eq_true_eq] at hfull
        rcases hr with ⟨hlx, hux, hly, huy⟩
        omega
      simp [hin]
    · simp [hdis, hfull]

private theorem mapM_uniform_coeff (qs : List Box) (g : Page) (r : Point)
    (hr : g.lower.x ≤ r.x ∧ r.x ≤ g.upper.x ∧ g.lower.y ≤ r.y ∧ r.y ≤ g.upper.y) :
    ∀ c : List Int, (qs.mapM (uniform g)) = some c →
      c.sum = (qs.map (fun q => if inBox q r then q.coeff else 0)).sum := by
  induction qs with
  | nil =>
      intro c hc
      simp at hc
      subst c
      simp
  | cons q qs ih =>
      intro c hc
      simp only [List.mapM_cons] at hc
      by_cases hu : ∃ d, uniform g q = some d
      · obtain ⟨d, hd⟩ := hu
        rw [hd] at hc
        cases hqs : qs.mapM (uniform g) with
        | none => simp [hqs] at hc
        | some ws =>
            simp [hqs] at hc
            subst c
            have hcq := uniform_coeff_eq g q r hr
            rw [hd] at hcq
            simp only [List.sum_cons]
            rw [hcq]
            rw [ih ws hqs]
            simp
      · have hnone : uniform g q = none := by
          cases h : uniform g q with
          | none => rfl
          | some d => exact (hu ⟨d, h⟩).elim
        simp [hnone] at hc

theorem sound_ExactlyOneKey_PageVerdict : ExactlyOneKey.PageVerdict.Goal := by
  intro b p g hv hb
  unfold pageScan
  cases hM : (ExactlyOneKey.shape.stencil b p).mapM (uniform g) with
  | none => rfl
  | some ws =>
      have hws : ∀ r ∈ g.rows, ws.sum = stencilWeight shape b p r.1 := by
        intro r hr
        rcases hv r hr with ⟨hlx, hux, hly, huy⟩
        exact mapM_uniform_coeff (ExactlyOneKey.shape.stencil b p) g r.1
          ⟨hlx, hux, hly, huy⟩ ws hM
      unfold scan
      rw [sum_filter_eq_sum_if]
      have hmap : g.rows.map (fun r => if shape.qualifies p r.1 then r.2 else 0) =
          g.rows.map (fun r => ws.sum * r.2) := by
        apply List.map_congr_left
        intro r hr
        have hq := stencil_id b p r.1 (hb r hr)
        have hw := hws r hr
        by_cases hqual : shape.qualifies p r.1 = true
        · simp [hqual] at hq ⊢
          rw [hw, hq]
          simp
        · simp [hqual] at hq ⊢
          rw [hw, hq]
          simp
      rw [hmap]
      exact (mul_sum_map_snd g.rows ws.sum).symm

end TinylambOpt.ResearchProofs.Atlas.Goal_ExactlyOneKey_PageVerdict
