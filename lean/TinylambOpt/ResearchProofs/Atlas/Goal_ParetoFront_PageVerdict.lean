import TinylambOpt.ResearchGoals.Atlas.RangePlans

/-!
An ordered access path needs to preserve the rows and boundaries selected by the SQL
range predicate.

Proved

- `ParetoFront.PageVerdict` — `∀ b p g, valid g → (∀ r ∈ g.rows, bounded b r.1) →
  pageScan shape b p g = scan shape p g.rows`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_ParetoFront_PageVerdict

open TinylambOpt.ResearchGoals.Atlas.RangePlans
open TinylambOpt.ResearchGoals.Atlas.RangePlans.ParetoFront

theorem sound_ParetoFront_PageVerdict : ParetoFront.PageVerdict.Goal := by
  intro b p g hv hb
  have hselfx : ¬ p.x + 1 ≤ p.x := by omega
  have hselfy : ¬ p.y + 1 ≤ p.y := by omega
  let qual : Point → Bool := fun v =>
    decide (p.x ≤ v.x) && decide (p.y ≤ v.y) && (decide (p.x < v.x) || decide (p.y < v.y))
  have scan_true (h : ∀ r ∈ g.rows, qual r.1 = true) :
      scan shape p g.rows = (g.rows.map Prod.snd).sum := by
    unfold scan
    change ((g.rows.filter (fun r => qual r.1)).map Prod.snd).sum = (g.rows.map Prod.snd).sum
    have hf : g.rows.filter (fun r => qual r.1) = g.rows := (List.filter_eq_self).2 h
    simp [hf]
  have filter_nil_aux : ∀ (l : Rows),
      (∀ r ∈ l, qual r.1 = false) → l.filter (fun r => qual r.1) = [] := by
    intro l
    induction l with
    | nil => intro h; rfl
    | cons r rs ih =>
      intro h
      have hr : qual r.1 = false := h r (by simp)
      have ht : ∀ x ∈ rs, qual x.1 = false := by
        intro x hx
        exact h x (by simp [hx])
      simp [hr, ih ht]
  have scan_false (h : ∀ r ∈ g.rows, qual r.1 = false) :
      scan shape p g.rows = 0 := by
    unfold scan
    change ((g.rows.filter (fun r => qual r.1)).map Prod.snd).sum = 0
    have hf := filter_nil_aux g.rows h
    simp [hf]
  have no_qual_out1
      (hout1 : ((((b + 1 ≤ p.x ∨ b + 1 ≤ p.y) ∨ g.upper.x < p.x) ∨ b + 1 ≤ g.lower.x) ∨ g.upper.y < p.y) ∨ b + 1 ≤ g.lower.y) :
      ∀ r ∈ g.rows, qual r.1 = false := by
    intro r hr
    have hrv := hv r hr
    have hrb := hb r hr
    simp [bounded] at hrb
    rcases hout1 with (((((hA | hB) | hC) | hD) | hE) | hF)
    · have hx : ¬ p.x ≤ r.1.x := by omega
      simp [qual, hx]
    · have hy : ¬ p.y ≤ r.1.y := by omega
      simp [qual, hy]
    · have hx : ¬ p.x ≤ r.1.x := by omega
      simp [qual, hx]
    · exfalso
      omega
    · have hy : ¬ p.y ≤ r.1.y := by omega
      simp [qual, hy]
    · exfalso
      omega
  have no_qual_full2
      (hfull2 : ((p.x ≤ g.lower.x ∧ g.upper.x < p.x + 1) ∧ p.y ≤ g.lower.y) ∧ g.upper.y < p.y + 1) :
      ∀ r ∈ g.rows, qual r.1 = false := by
    intro r hr
    have hrv := hv r hr
    rcases hfull2 with ⟨⟨⟨hflx, hfux⟩, hfly⟩, hfuy⟩
    have hx : r.1.x = p.x := by omega
    have hy : r.1.y = p.y := by omega
    simp [qual, hx, hy]
  have all_qual_full1_out2
      (hfull1 : ((p.x ≤ g.lower.x ∧ g.upper.x < b + 1) ∧ p.y ≤ g.lower.y) ∧ g.upper.y < b + 1)
      (hout2 : ((g.upper.x < p.x ∨ p.x + 1 ≤ g.lower.x) ∨ g.upper.y < p.y) ∨ p.y + 1 ≤ g.lower.y) :
      ∀ r ∈ g.rows, qual r.1 = true := by
    intro r hr
    have hrv := hv r hr
    have hrb := hb r hr
    simp [bounded] at hrb
    rcases hfull1 with ⟨⟨⟨hflx, hfux⟩, hfly⟩, hfuy⟩
    rcases hout2 with (((hC | hD) | hE) | hF)
    · exfalso
      omega
    · have hxlt : p.x < r.1.x := by omega
      have hxle : p.x ≤ r.1.x := by omega
      have hyle : p.y ≤ r.1.y := by omega
      simp [qual, hxle, hyle, hxlt]
    · exfalso
      omega
    · have hylt : p.y < r.1.y := by omega
      have hxle : p.x ≤ r.1.x := by omega
      have hyle : p.y ≤ r.1.y := by omega
      simp [qual, hxle, hyle, hylt]
  unfold pageScan
  simp [ParetoFront.shape, uniform, box, hselfx, hselfy]
  by_cases hout1 : ((((b + 1 ≤ p.x ∨ b + 1 ≤ p.y) ∨ g.upper.x < p.x) ∨ b + 1 ≤ g.lower.x) ∨ g.upper.y < p.y) ∨ b + 1 ≤ g.lower.y
  · simp [hout1]
    by_cases hout2 : ((g.upper.x < p.x ∨ p.x + 1 ≤ g.lower.x) ∨ g.upper.y < p.y) ∨ p.y + 1 ≤ g.lower.y
    · simp [hout2]
      symm
      exact scan_false (no_qual_out1 hout1)
    · simp [hout2]
      by_cases hfull2 : ((p.x ≤ g.lower.x ∧ g.upper.x < p.x + 1) ∧ p.y ≤ g.lower.y) ∧ g.upper.y < p.y + 1
      · simp [hfull2]
        have hnil : g.rows = [] := by
          apply List.eq_nil_iff_forall_not_mem.mpr
          intro r hr
          have hrv := hv r hr
          have hrb := hb r hr
          simp [bounded] at hrb
          rcases hfull2 with ⟨⟨⟨hflx, hfux⟩, hfly⟩, hfuy⟩
          rcases hout1 with (((((hA | hB) | hC) | hD) | hE) | hF) <;> omega
        simp [hnil, scan]
      · simp [hfull2]
  · simp [hout1]
    by_cases hfull1 : ((p.x ≤ g.lower.x ∧ g.upper.x < b + 1) ∧ p.y ≤ g.lower.y) ∧ g.upper.y < b + 1
    · simp [hfull1]
      by_cases hout2 : ((g.upper.x < p.x ∨ p.x + 1 ≤ g.lower.x) ∨ g.upper.y < p.y) ∨ p.y + 1 ≤ g.lower.y
      · simp [hout2]
        symm
        exact scan_true (all_qual_full1_out2 hfull1 hout2)
      · simp [hout2]
        by_cases hfull2 : ((p.x ≤ g.lower.x ∧ g.upper.x < p.x + 1) ∧ p.y ≤ g.lower.y) ∧ g.upper.y < p.y + 1
        · simp [hfull2]
          symm
          exact scan_false (no_qual_full2 hfull2)
        · have hnot : ¬ (g.upper.x < p.x + 1 ∧ g.upper.y < p.y + 1) := by
            intro hh
            apply hfull2
            exact ⟨⟨⟨hfull1.1.1.1, hh.1⟩, hfull1.1.2⟩, hh.2⟩
          simp [hnot]
    · simp [hfull1]

end TinylambOpt.ResearchProofs.Atlas.Goal_ParetoFront_PageVerdict
