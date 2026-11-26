import TinylambOpt.ResearchGoals.Atlas.RangePlans

/-!
An ordered access path needs to preserve the rows and boundaries selected by the SQL
range predicate.

Proved

- `GapSession.PageVerdict` — `∀ b p g, valid g → (∀ r ∈ g.rows, bounded b r.1) →
  pageScan shape b p g = scan shape p g.rows`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_GapSession_PageVerdict

open TinylambOpt.ResearchGoals.Atlas.RangePlans
open TinylambOpt.ResearchGoals.Atlas.RangePlans.GapSession

theorem sound_GapSession_PageVerdict : GapSession.PageVerdict.Goal := by
  intro b p g hv hb
  unfold pageScan
  simp [GapSession.shape, uniform, box]
  by_cases hbad : ((((p.y + 8 ≤ p.y ∨ p.x + 2 ≤ p.x - 1) ∨ g.upper.x < p.y) ∨
      p.y + 8 ≤ g.lower.x) ∨ g.upper.y < p.x - 1) ∨ p.x + 2 ≤ g.lower.y
  · simp [hbad]
    unfold scan
    have hall : ∀ r ∈ g.rows,
        (decide (p.y ≤ r.1.x) && decide (r.1.x ≤ p.y + 7) &&
          decide (p.x - 1 ≤ r.1.y) && decide (r.1.y ≤ p.x + 1)) = false := by
      intro r hr
      have hrv := hv r hr
      have hrb := hb r hr
      simp [bounded] at hrb
      rcases hbad with hABCDE | hF
      · rcases hABCDE with hABCD | hE
        · rcases hABCD with hABC | hD
          · rcases hABC with hAB | hC
            · rcases hAB with hA | hB
              · have hcontra : False := by omega
                exact False.elim hcontra
              · have hcontra : False := by omega
                exact False.elim hcontra
            · have hx : ¬ p.y ≤ r.1.x := by omega
              simp [hx]
          · have hx : ¬ r.1.x ≤ p.y + 7 := by omega
            simp [hx]
        · have hy : ¬ p.x - 1 ≤ r.1.y := by omega
          simp [hy]
      · have hy : ¬ r.1.y ≤ p.x + 1 := by omega
        simp [hy]
    have filter_all_false : ∀ (l : List (Point × Int)),
        (∀ r ∈ l,
          (decide (p.y ≤ r.1.x) && decide (r.1.x ≤ p.y + 7) &&
            decide (p.x - 1 ≤ r.1.y) && decide (r.1.y ≤ p.x + 1)) = false) →
        l.filter (fun r => decide (p.y ≤ r.1.x) && decide (r.1.x ≤ p.y + 7) &&
            decide (p.x - 1 ≤ r.1.y) && decide (r.1.y ≤ p.x + 1)) = [] := by
      intro l
      induction l with
      | nil => intro h; rfl
      | cons r rs ih =>
        intro h
        have hrfalse := h r (List.mem_cons_self : r ∈ r :: rs)
        have htail : ∀ x ∈ rs,
            (decide (p.y ≤ x.1.x) && decide (x.1.x ≤ p.y + 7) &&
              decide (p.x - 1 ≤ x.1.y) && decide (x.1.y ≤ p.x + 1)) = false := by
          intro x hx
          exact h x (List.mem_cons_of_mem r hx)
        simp [hrfalse, ih htail]
    simp [filter_all_false g.rows hall]
  · by_cases hfull : ((p.y ≤ g.lower.x ∧ g.upper.x < p.y + 8) ∧
        p.x - 1 ≤ g.lower.y) ∧ g.upper.y < p.x + 2
    · simp [hbad, hfull]
      unfold scan
      have hall : ∀ r ∈ g.rows,
          (decide (p.y ≤ r.1.x) && decide (r.1.x ≤ p.y + 7) &&
            decide (p.x - 1 ≤ r.1.y) && decide (r.1.y ≤ p.x + 1)) = true := by
        intro r hr
        have hrv := hv r hr
        have hrb := hb r hr
        simp [bounded] at hrb
        have hx1 : p.y ≤ r.1.x := by omega
        have hx2 : r.1.x ≤ p.y + 7 := by omega
        have hy1 : p.x - 1 ≤ r.1.y := by omega
        have hy2 : r.1.y ≤ p.x + 1 := by omega
        simp [hx1, hx2, hy1, hy2]
      have hfilter : g.rows.filter
          (fun r => decide (p.y ≤ r.1.x) && decide (r.1.x ≤ p.y + 7) &&
            decide (p.x - 1 ≤ r.1.y) && decide (r.1.y ≤ p.x + 1)) = g.rows :=
        (List.filter_eq_self).2 hall
      simp [hfilter]
    · simp [hbad, hfull]

end TinylambOpt.ResearchProofs.Atlas.Goal_GapSession_PageVerdict
