import TinylambOpt.ResearchGoals.Atlas.RangePlans

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_NestedVersion_OuterCertificate

open TinylambOpt.ResearchGoals.Atlas.RangePlans
open TinylambOpt.ResearchGoals.Atlas.RangePlans.NestedVersion

/-- Rewrite a filtered sum as a sum with an `if` term, so the two scans can be
compared pointwise on the original row list. -/
private theorem sum_filter_eq_sum_if (rs : Rows) (f : Point × Int → Bool)
    (g : Point × Int → Int) :
    ((rs.filter f).map g).sum = (rs.map (fun r => if f r then g r else 0)).sum := by
  induction rs with
  | nil => rfl
  | cons r rs ih =>
      by_cases h : f r <;> simp [h, ih]

private theorem scan_eq_sum_if (s : Shape) (p : Point) (rs : Rows) :
    scan s p rs = (rs.map (fun r => if s.qualifies p r.1 then r.2 else 0)).sum := by
  unfold scan
  exact sum_filter_eq_sum_if rs (fun r => s.qualifies p r.1) Prod.snd

/-- Qualification for `q` implies qualification for `p`, and with nonnegative
weights the pointwise terms are ordered, hence so are the sums. -/
private theorem scan_le_scan_of_subset (p q : Point) : ∀ rs : Rows,
    (∀ r ∈ rs, shape.qualifies q r.1 = true → shape.qualifies p r.1 = true) →
    (∀ r ∈ rs, 0 ≤ r.2) →
    scan shape q rs ≤ scan shape p rs := by
  intro rs
  induction rs with
  | nil =>
      intro hmono hnn
      simp [scan]
  | cons r rs ih =>
      intro hmono hnn
      have hr : 0 ≤ r.2 := hnn r (by simp)
      have hmono' : ∀ r' ∈ rs, shape.qualifies q r'.1 = true → shape.qualifies p r'.1 = true :=
        fun r' hm => hmono r' (by simp [hm])
      have hnn' : ∀ r' ∈ rs, 0 ≤ r'.2 := fun r' hm => hnn r' (by simp [hm])
      have ih' := ih hmono' hnn'
      have hterm :
          (if shape.qualifies q r.1 then r.2 else 0) ≤
            (if shape.qualifies p r.1 then r.2 else 0) := by
        by_cases hq : shape.qualifies q r.1
        · have hp : shape.qualifies p r.1 := hmono r (by simp) hq
          simp [hq, hp]
        · by_cases hp : shape.qualifies p r.1
          · simp [hq, hp, hr]
          · simp [hq, hp]
      rw [scan_eq_sum_if shape q rs, scan_eq_sum_if shape p rs] at ih'
      rw [scan_eq_sum_if shape q (r :: rs), scan_eq_sum_if shape p (r :: rs)]
      simp only [List.map_cons, List.sum_cons]
      omega

theorem sound_NestedVersion_OuterCertificate : NestedVersion.OuterCertificate.Goal := by
  intro p q rs hpq hqp hw
  apply scan_le_scan_of_subset p q rs
  · intro r hr hq
    simp [shape] at hq ⊢
    omega
  · intro r hr
    exact hw r hr

end TinylambOpt.ResearchProofs.Atlas.Goal_NestedVersion_OuterCertificate
