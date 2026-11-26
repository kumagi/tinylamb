import TinylambOpt.ResearchGoals.Atlas.RangePlans

/-!
An ordered access path needs to preserve the rows and boundaries selected by the SQL
range predicate.

Proved

- `EndpointContact.PageVerdict` — `∀ b p g, valid g → (∀ r ∈ g.rows, bounded b r.1) →
  pageScan shape b p g = scan shape p g.rows`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_EndpointContact_PageVerdict

open TinylambOpt.ResearchGoals.Atlas.RangePlans
open TinylambOpt.ResearchGoals.Atlas.RangePlans.EndpointContact

private def boxIn (q : Box) (v : Point) : Bool :=
  decide (q.x0 ≤ v.x ∧ v.x < q.x1 ∧ q.y0 ≤ v.y ∧ v.y < q.y1)

private def stencilWeight (b : Int) (p v : Point) : Int :=
  ((EndpointContact.shape.stencil b p).map (fun q => if boxIn q v then q.coeff else 0)).sum

private theorem sum_filter_eq_sum_if (rs : Rows) (f : Point × Int → Bool) :
    ((rs.filter f).map Prod.snd).sum =
      (rs.map (fun r => if f r then r.2 else 0)).sum := by
  induction rs with
  | nil => rfl
  | cons r rs ih =>
    by_cases h : f r = true <;> simp [h, ih]

private theorem stencil_id (b : Int) (p v : Point) (hv : bounded b v) :
    stencilWeight b p v = if EndpointContact.shape.qualifies p v then (1 : Int) else 0 := by
  simp [stencilWeight, EndpointContact.shape, box, boxIn, bounded] at hv ⊢
  omega

private theorem scan_eq_sum_weight (b : Int) (p : Point) (rs : Rows)
    (hb : ∀ r ∈ rs, bounded b r.1) :
    scan EndpointContact.shape p rs =
      (rs.map (fun r => stencilWeight b p r.1 * r.2)).sum := by
  rw [scan, sum_filter_eq_sum_if]
  apply congrArg List.sum
  apply List.map_congr_left
  intro r hr
  rw [stencil_id b p r.1 (hb r hr)]
  by_cases hc : EndpointContact.shape.qualifies p r.1 <;> simp [hc]

private theorem sum_map_snd_const (rs : Rows) (c : Int) :
    (rs.map (fun r => c * r.2)).sum = c * (rs.map Prod.snd).sum := by
  induction rs with
  | nil => simp
  | cons r rs ih => simp [ih, Int.mul_add]

private theorem uniform_cases (g : Page) (q : Box) (c : Int) (hvalid : valid g)
    (hg : uniform g q = some c) :
    (c = 0 ∧ ∀ r ∈ g.rows, boxIn q r.1 = false) ∨
      (q.x0 ≤ g.lower.x ∧ g.upper.x < q.x1 ∧
        q.y0 ≤ g.lower.y ∧ g.upper.y < q.y1 ∧ c = q.coeff ∧
        ∀ r ∈ g.rows, boxIn q r.1 = true) := by
  unfold uniform at hg
  by_cases hz : q.x1 ≤ q.x0 || q.y1 ≤ q.y0 || g.upper.x < q.x0 || q.x1 ≤ g.lower.x ||
      g.upper.y < q.y0 || q.y1 ≤ g.lower.y
  · rw [if_pos hz] at hg
    have hc : c = 0 := by simpa [eq_comm] using hg
    refine Or.inl ⟨hc, ?_⟩
    intro r hr
    have hv := hvalid r hr
    have hz' : q.x1 ≤ q.x0 ∨ q.y1 ≤ q.y0 ∨ g.upper.x < q.x0 ∨
        q.x1 ≤ g.lower.x ∨ g.upper.y < q.y0 ∨ q.y1 ≤ g.lower.y := by
      simp only [Bool.or_eq_true, decide_eq_true_eq, or_assoc] at hz
      exact hz
    rcases hz' with hz | hz | hz | hz | hz | hz <;>
      simp [boxIn, decide_eq_true_eq] <;> omega
  · rw [if_neg hz] at hg
    by_cases hf : q.x0 ≤ g.lower.x && g.upper.x < q.x1 &&
        q.y0 ≤ g.lower.y && g.upper.y < q.y1
    · rw [if_pos hf] at hg
      have hc : c = q.coeff := by simpa [eq_comm] using hg
      have hf' : q.x0 ≤ g.lower.x ∧ g.upper.x < q.x1 ∧
          q.y0 ≤ g.lower.y ∧ g.upper.y < q.y1 := by
        simp only [Bool.and_eq_true, decide_eq_true_eq, and_assoc] at hf
        exact hf
      rcases hf' with ⟨hx0, hx1, hy0, hy1⟩
      refine Or.inr ⟨hx0, hx1, hy0, hy1, hc, ?_⟩
      intro r hr
      have hv := hvalid r hr
      simp [boxIn, decide_eq_true_eq]
      omega
    · simp [hf] at hg

theorem sound_EndpointContact_PageVerdict : EndpointContact.PageVerdict.Goal := by
  intro b p g hv hb
  let q1 : Box := box (p.y) (p.y + 1) (-b) (b + 1)
  let q2 : Box := box (-b) (b + 1) (p.x) (p.x + 1)
  let q3 : Box := box (p.y) (p.y + 1) (p.x) (p.x + 1) (-1)
  have hstencil : EndpointContact.shape.stencil b p = [q1, q2, q3] := by
    rfl
  unfold pageScan
  rw [hstencil]
  cases hu1 : uniform g q1 with
  | none => simp [hu1]
  | some c1 =>
    cases hu2 : uniform g q2 with
    | none => simp [hu1, hu2]
    | some c2 =>
      cases hu3 : uniform g q3 with
      | none => simp [hu1, hu2, hu3]
      | some c3 =>
        simp [hu1, hu2, hu3]
        rw [scan_eq_sum_weight b p g.rows hb]
        have hq1 := uniform_cases g q1 c1 hv hu1
        have hq2 := uniform_cases g q2 c2 hv hu2
        have hq3 := uniform_cases g q3 c3 hv hu3
        have hbox : ∀ r ∈ g.rows,
            (if boxIn q1 r.1 then q1.coeff else 0) = c1 ∧
            (if boxIn q2 r.1 then q2.coeff else 0) = c2 ∧
            (if boxIn q3 r.1 then q3.coeff else 0) = c3 := by
          intro r hr
          constructor
          · rcases hq1 with h | h
            · simp [h.1, h.2 r hr]
            · simp [h.2.2.2.2.1, h.2.2.2.2.2 r hr]
          constructor
          · rcases hq2 with h | h
            · simp [h.1, h.2 r hr]
            · simp [h.2.2.2.2.1, h.2.2.2.2.2 r hr]
          · rcases hq3 with h | h
            · simp [h.1, h.2 r hr]
            · simp [h.2.2.2.2.1, h.2.2.2.2.2 r hr]
        have hsum : (g.rows.map (fun r => stencilWeight b p r.1 * r.2)).sum =
            (g.rows.map (fun r => (c1 + c2 + c3) * r.2)).sum := by
          apply congrArg List.sum
          apply List.map_congr_left
          intro r hr
          have hrbox := hbox r hr
          have hsw : stencilWeight b p r.1 = c1 + c2 + c3 := by
            unfold stencilWeight
            rw [hstencil]
            simp [hrbox.1, hrbox.2.1, hrbox.2.2]
            omega
          rw [hsw]
        rw [hsum, sum_map_snd_const]
        have hassoc : c1 + (c2 + c3) = c1 + c2 + c3 := by omega
        rw [hassoc]

end TinylambOpt.ResearchProofs.Atlas.Goal_EndpointContact_PageVerdict
