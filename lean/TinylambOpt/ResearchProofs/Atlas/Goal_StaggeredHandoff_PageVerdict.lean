import TinylambOpt.ResearchGoals.Atlas.RangePlans

/-!
An ordered access path needs to preserve the rows and boundaries selected by the SQL
range predicate.

Proved

- `StaggeredHandoff.PageVerdict` — `∀ b p g, valid g → (∀ r ∈ g.rows, bounded b r.1) →
  pageScan shape b p g = scan shape p g.rows`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_StaggeredHandoff_PageVerdict

open TinylambOpt.ResearchGoals.Atlas.RangePlans
open TinylambOpt.ResearchGoals.Atlas.RangePlans.StaggeredHandoff

theorem sound_StaggeredHandoff_PageVerdict : StaggeredHandoff.PageVerdict.Goal := by
  intro b p g hv hb
  unfold pageScan
  simp [StaggeredHandoff.shape, uniform, box]
  by_cases hbad : ((((p.y ≤ p.x + 1 ∨ b ≤ p.y) ∨ g.upper.x < p.x + 1) ∨ p.y ≤ g.lower.x) ∨ g.upper.y < p.y + 1) ∨ b + 1 ≤ g.lower.y
  · simp [hbad]
    unfold scan
    have hall : ∀ r ∈ g.rows,
        (decide (p.x < r.1.x) && decide (r.1.x < p.y) && decide (p.y < r.1.y)) = false := by
      intro r hr
      have hrv := hv r hr
      have hrb := hb r hr
      simp [bounded] at hrb
      rcases hbad with hABCDE | hF
      · rcases hABCDE with hABCD | hE
        · rcases hABCD with hABC | hD
          · rcases hABC with hAB | hC
            · rcases hAB with hA | hB
              · by_cases hx : p.x < r.1.x
                · have hyx : ¬ r.1.x < p.y := by omega
                  simp [hx, hyx]
                · simp [hx]
              · have hy : ¬ p.y < r.1.y := by omega
                simp [hy]
            · have hx : ¬ p.x < r.1.x := by omega
              simp [hx]
          · have hx : ¬ r.1.x < p.y := by omega
            simp [hx]
        · have hy : ¬ p.y < r.1.y := by omega
          simp [hy]
      · have hcontra : False := by omega
        exact False.elim hcontra
    have filter_all_false : ∀ (l : List (Point × Int)),
        (∀ r ∈ l,
          (decide (p.x < r.1.x) && decide (r.1.x < p.y) && decide (p.y < r.1.y)) = false) →
        l.filter (fun r => decide (p.x < r.1.x) && decide (r.1.x < p.y) && decide (p.y < r.1.y)) = [] := by
      intro l
      induction l with
      | nil => intro h; rfl
      | cons r rs ih =>
        intro h
        have hrfalse := h r (List.mem_cons_self : r ∈ r :: rs)
        have htail : ∀ x ∈ rs,
            (decide (p.x < x.1.x) && decide (x.1.x < p.y) && decide (p.y < x.1.y)) = false := by
          intro x hx
          exact h x (List.mem_cons_of_mem r hx)
        simp [hrfalse, ih htail]
    simp [filter_all_false g.rows hall]
  · by_cases hfull : ((p.x + 1 ≤ g.lower.x ∧ g.upper.x < p.y) ∧ p.y + 1 ≤ g.lower.y) ∧ g.upper.y < b + 1
    · simp [hbad, hfull]
      unfold scan
      have hall : ∀ r ∈ g.rows,
          (decide (p.x < r.1.x) && decide (r.1.x < p.y) && decide (p.y < r.1.y)) = true := by
        intro r hr
        have hrv := hv r hr
        have hrb := hb r hr
        simp [bounded] at hrb
        have hx1 : p.x < r.1.x := by omega
        have hx2 : r.1.x < p.y := by omega
        have hy : p.y < r.1.y := by omega
        simp [hx1, hx2, hy]
      have hfilter : g.rows.filter
          (fun r => decide (p.x < r.1.x) && decide (r.1.x < p.y) && decide (p.y < r.1.y)) = g.rows :=
        (List.filter_eq_self).2 hall
      simp [hfilter]
    · simp [hbad, hfull]

end TinylambOpt.ResearchProofs.Atlas.Goal_StaggeredHandoff_PageVerdict
