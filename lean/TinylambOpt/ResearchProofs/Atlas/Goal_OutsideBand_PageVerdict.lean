import TinylambOpt.ResearchGoals.Atlas.RangePlans

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_OutsideBand_PageVerdict

open TinylambOpt.ResearchGoals.Atlas.RangePlans
open TinylambOpt.ResearchGoals.Atlas.RangePlans.OutsideBand

theorem sound_OutsideBand_PageVerdict : OutsideBand.PageVerdict.Goal := by
  intro b p g hv hb
  have rows_nil_of_outer_bad :
      ((((b + 1 ≤ -b ∨ g.upper.x < -b) ∨ b + 1 ≤ g.lower.x) ∨ g.upper.y < -b) ∨
        b + 1 ≤ g.lower.y) → g.rows = [] := by
    intro hbad
    apply (List.eq_nil_iff_forall_not_mem).2
    intro r hr
    exfalso
    have hrv := hv r hr
    have hrb := hb r hr
    rcases hrv with ⟨hlx, hux, hly, huy⟩
    rcases hrb with ⟨blx, bux, bly, buy⟩
    rcases hbad with (((hA | hB) | hC) | hD) | hE <;> omega
  have filter_false_nil : ∀ (l : List (Point × Int)),
      (∀ r ∈ l, (decide (2 < (r.1.x - p.x).natAbs) ||
        decide (4 < (r.1.y - p.y).natAbs)) = false) →
      l.filter (fun r => decide (2 < (r.1.x - p.x).natAbs) ||
        decide (4 < (r.1.y - p.y).natAbs)) = [] := by
    intro l
    induction l with
    | nil => intro h; rfl
    | cons r rs ih =>
        intro h
        simp [h r (List.mem_cons_self : r ∈ r :: rs),
          ih (fun x hx => h x (List.mem_cons_of_mem r hx))]
  unfold pageScan
  simp [OutsideBand.shape, uniform, box]
  by_cases h0bad : (((b + 1 ≤ -b ∨ g.upper.x < -b) ∨ b + 1 ≤ g.lower.x) ∨ g.upper.y < -b) ∨ b + 1 ≤ g.lower.y
  · have hrows := rows_nil_of_outer_bad h0bad
    by_cases h1bad : (((((p.x + 3 ≤ p.x - 2 ∨ p.y + 5 ≤ p.y - 4) ∨ g.upper.x < p.x - 2) ∨ p.x + 3 ≤ g.lower.x) ∨ g.upper.y < p.y - 4) ∨ p.y + 5 ≤ g.lower.y)
    · simp [h0bad, h1bad, hrows, scan]
    · by_cases h1in : ((p.x - 2 ≤ g.lower.x ∧ g.upper.x < p.x + 3) ∧ p.y - 4 ≤ g.lower.y) ∧ g.upper.y < p.y + 5
      · simp [h0bad, h1bad, h1in, hrows, scan]
      · simp [h0bad, h1bad, h1in]
  · by_cases h0in : ((-b ≤ g.lower.x ∧ g.upper.x < b + 1) ∧ -b ≤ g.lower.y) ∧ g.upper.y < b + 1
    · by_cases h1bad : (((((p.x + 3 ≤ p.x - 2 ∨ p.y + 5 ≤ p.y - 4) ∨ g.upper.x < p.x - 2) ∨ p.x + 3 ≤ g.lower.x) ∨ g.upper.y < p.y - 4) ∨ p.y + 5 ≤ g.lower.y)
      · simp [h0bad, h0in, h1bad]
        unfold scan
        have hall : ∀ r ∈ g.rows,
            (decide (2 < (r.1.x - p.x).natAbs) ||
              decide (4 < (r.1.y - p.y).natAbs)) = true := by
          intro r hr
          have hrv := hv r hr
          rcases hrv with ⟨hlx, hux, hly, huy⟩
          rcases h1bad with (((((hA | hB) | hC) | hD) | hE) | hF)
          · omega
          · omega
          · have hx : 2 < (r.1.x - p.x).natAbs := by omega
            simp [hx]
          · have hx : 2 < (r.1.x - p.x).natAbs := by omega
            simp [hx]
          · have hy : 4 < (r.1.y - p.y).natAbs := by omega
            simp [hy]
          · have hy : 4 < (r.1.y - p.y).natAbs := by omega
            simp [hy]
        have hfilter : g.rows.filter (fun r =>
            decide (2 < (r.1.x - p.x).natAbs) ||
              decide (4 < (r.1.y - p.y).natAbs)) = g.rows :=
          (List.filter_eq_self).2 hall
        simp [hfilter]
      · by_cases h1in : ((p.x - 2 ≤ g.lower.x ∧ g.upper.x < p.x + 3) ∧ p.y - 4 ≤ g.lower.y) ∧ g.upper.y < p.y + 5
        · simp [h0bad, h0in, h1bad, h1in]
          unfold scan
          have hall : ∀ r ∈ g.rows,
              (decide (2 < (r.1.x - p.x).natAbs) ||
                decide (4 < (r.1.y - p.y).natAbs)) = false := by
            intro r hr
            have hrv := hv r hr
            rcases hrv with ⟨hlx, hux, hly, huy⟩
            rcases h1in with ⟨⟨⟨hx0, hx1⟩, hy0⟩, hy1⟩
            have hx : (r.1.x - p.x).natAbs ≤ 2 := by omega
            have hy : (r.1.y - p.y).natAbs ≤ 4 := by omega
            simp [hx, hy]
          simp [filter_false_nil g.rows hall]
        · simp [h0bad, h0in, h1bad, h1in]
    · simp [h0bad, h0in]

end TinylambOpt.ResearchProofs.Atlas.Goal_OutsideBand_PageVerdict
