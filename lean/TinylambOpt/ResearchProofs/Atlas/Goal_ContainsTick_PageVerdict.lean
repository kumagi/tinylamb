import TinylambOpt.ResearchGoals.Atlas.RangePlans

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_ContainsTick_PageVerdict

open TinylambOpt.ResearchGoals.Atlas.RangePlans
open TinylambOpt.ResearchGoals.Atlas.RangePlans.ContainsTick

theorem sound_ContainsTick_PageVerdict : ContainsTick.PageVerdict.Goal := by
  intro b p g hv hb
  unfold pageScan
  simp [ContainsTick.shape, uniform, box]
  by_cases hbad : ((((p.x + 1 ≤ -b ∨ b ≤ p.x) ∨ g.upper.x < -b) ∨ p.x + 1 ≤ g.lower.x) ∨ g.upper.y < p.x + 1) ∨ b + 1 ≤ g.lower.y
  · simp [hbad]
    unfold scan
    have hall : ∀ r ∈ g.rows, (decide (r.1.x ≤ p.x) && decide (p.x < r.1.y)) = false := by
      intro r hr
      have hrv := hv r hr
      have hrb := hb r hr
      simp [bounded] at hrb
      rcases hbad with hABCDE | hF
      · rcases hABCDE with hABCD | hE
        · rcases hABCD with hABC | hD
          · rcases hABC with hAB | hC
            · rcases hAB with hA | hB
              · have hx : ¬ r.1.x ≤ p.x := by omega
                simp [hx]
              · have hy : ¬ p.x < r.1.y := by omega
                simp [hy]
            · have hx : ¬ r.1.x ≤ p.x := by omega
              simp [hx]
          · have hx : ¬ r.1.x ≤ p.x := by omega
            simp [hx]
        · have hy : ¬ p.x < r.1.y := by omega
          simp [hy]
      · have hy : ¬ p.x < r.1.y := by omega
        simp [hy]
    have filter_all_false : ∀ (l : List (Point × Int)),
        (∀ r ∈ l, (decide (r.1.x ≤ p.x) && decide (p.x < r.1.y)) = false) →
        l.filter (fun r => decide (r.1.x ≤ p.x) && decide (p.x < r.1.y)) = [] := by
      intro l
      induction l with
      | nil => intro h; rfl
      | cons r rs ih =>
        intro h
        have hrfalse : (decide (r.1.x ≤ p.x) && decide (p.x < r.1.y)) = false :=
          h r (List.mem_cons_self : r ∈ r :: rs)
        have htail : ∀ x ∈ rs, (decide (x.1.x ≤ p.x) && decide (p.x < x.1.y)) = false := by
          intro x hx
          exact h x (List.mem_cons_of_mem r hx)
        simp [hrfalse, ih htail]
    simp [filter_all_false g.rows hall]
  · simp [hbad]
    by_cases hfull : ((-b ≤ g.lower.x ∧ g.upper.x < p.x + 1) ∧ p.x + 1 ≤ g.lower.y) ∧ g.upper.y < b + 1
    · simp [hfull]
      unfold scan
      have hall : ∀ r ∈ g.rows, (decide (r.1.x ≤ p.x) && decide (p.x < r.1.y)) = true := by
        intro r hr
        have hrv := hv r hr
        have hrb := hb r hr
        simp [bounded] at hrb
        have hx : r.1.x ≤ p.x := by omega
        have hy : p.x < r.1.y := by omega
        simp [hx, hy]
      have hfilter : g.rows.filter (fun r => decide (r.1.x ≤ p.x) && decide (p.x < r.1.y)) = g.rows :=
        (List.filter_eq_self).2 hall
      simp [hfilter]
    · simp [hfull]

end TinylambOpt.ResearchProofs.Atlas.Goal_ContainsTick_PageVerdict
