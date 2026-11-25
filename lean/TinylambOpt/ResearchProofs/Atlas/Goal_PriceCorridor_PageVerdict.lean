import TinylambOpt.ResearchGoals.Atlas.RangePlans

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_PriceCorridor_PageVerdict

open TinylambOpt.ResearchGoals.Atlas.RangePlans
open TinylambOpt.ResearchGoals.Atlas.RangePlans.PriceCorridor

theorem sound_PriceCorridor_PageVerdict : PriceCorridor.PageVerdict.Goal := by
  intro b p g hv hb
  unfold pageScan
  simp [PriceCorridor.shape, uniform, box]
  by_cases hbad :
      ((((p.x + 4 ≤ p.x - 3 ∨ p.y + 6 ≤ p.y - 2) ∨ g.upper.x < p.x - 3) ∨
        p.x + 4 ≤ g.lower.x) ∨ g.upper.y < p.y - 2) ∨ p.y + 6 ≤ g.lower.y
  · simp [hbad]
    unfold scan
    have hall : ∀ r ∈ g.rows,
        (decide ((r.1.x - p.x).natAbs ≤ 3) && decide (p.y - 2 ≤ r.1.y) &&
          decide (r.1.y ≤ p.y + 5)) = false := by
      intro r hr
      have hrv := hv r hr
      rcases hrv with ⟨hlx, hux, hly, huy⟩
      rcases hbad with habcde | hF
      · rcases habcde with habcd | hE
        · rcases habcd with habc | hD
          · rcases habc with hAB | hC
            · rcases hAB with hA | hB
              · have hx : ¬ (r.1.x - p.x).natAbs ≤ 3 := by omega
                simp [hx]
              · have hx : ¬ (r.1.x - p.x).natAbs ≤ 3 := by omega
                simp [hx]
            · have hx : ¬ (r.1.x - p.x).natAbs ≤ 3 := by omega
              simp [hx]
          · have hx : ¬ (r.1.x - p.x).natAbs ≤ 3 := by omega
            simp [hx]
        · have hy : ¬ p.y - 2 ≤ r.1.y := by omega
          simp [hy]
      · have hy : ¬ r.1.y ≤ p.y + 5 := by omega
        simp [hy]
    have filter_all_false : ∀ (l : List (Point × Int)),
        (∀ r ∈ l,
          (decide ((r.1.x - p.x).natAbs ≤ 3) && decide (p.y - 2 ≤ r.1.y) &&
            decide (r.1.y ≤ p.y + 5)) = false) →
        l.filter (fun r =>
          decide ((r.1.x - p.x).natAbs ≤ 3) && decide (p.y - 2 ≤ r.1.y) &&
            decide (r.1.y ≤ p.y + 5)) = [] := by
      intro l
      induction l with
      | nil => intro h; rfl
      | cons r rs ih =>
        intro h
        have hrfalse :
            (decide ((r.1.x - p.x).natAbs ≤ 3) && decide (p.y - 2 ≤ r.1.y) &&
              decide (r.1.y ≤ p.y + 5)) = false :=
          h r (List.mem_cons_self : r ∈ r :: rs)
        have htail : ∀ x ∈ rs,
            (decide ((x.1.x - p.x).natAbs ≤ 3) && decide (p.y - 2 ≤ x.1.y) &&
              decide (x.1.y ≤ p.y + 5)) = false := by
          intro x hx
          exact h x (List.mem_cons_of_mem r hx)
        simp [hrfalse, ih htail]
    simp [filter_all_false g.rows hall]
  · simp [hbad]
    by_cases hfull : ((p.x - 3 ≤ g.lower.x ∧ g.upper.x < p.x + 4) ∧ p.y - 2 ≤ g.lower.y) ∧
        g.upper.y < p.y + 6
    · simp [hfull]
      unfold scan
      have hall : ∀ r ∈ g.rows,
          (decide ((r.1.x - p.x).natAbs ≤ 3) && decide (p.y - 2 ≤ r.1.y) &&
            decide (r.1.y ≤ p.y + 5)) = true := by
        intro r hr
        have hrv := hv r hr
        rcases hrv with ⟨hlx, hux, hly, huy⟩
        rcases hfull with ⟨⟨⟨hx0, hx1⟩, hy0⟩, hy1⟩
        have hx : (r.1.x - p.x).natAbs ≤ 3 := by omega
        have hylo : p.y - 2 ≤ r.1.y := by omega
        have hyhi : r.1.y ≤ p.y + 5 := by omega
        simp [hx, hylo, hyhi]
      have hfilter : g.rows.filter (fun r =>
          decide ((r.1.x - p.x).natAbs ≤ 3) && decide (p.y - 2 ≤ r.1.y) &&
            decide (r.1.y ≤ p.y + 5)) = g.rows :=
        (List.filter_eq_self).2 hall
      simp [hfilter]
    · simp [hfull]

end TinylambOpt.ResearchProofs.Atlas.Goal_PriceCorridor_PageVerdict
