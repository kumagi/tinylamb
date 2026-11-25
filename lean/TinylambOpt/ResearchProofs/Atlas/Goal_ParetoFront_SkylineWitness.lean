import TinylambOpt.ResearchGoals.Atlas.RangePlans

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_ParetoFront_SkylineWitness

open TinylambOpt.ResearchGoals.Atlas.RangePlans
open TinylambOpt.ResearchGoals.Atlas.RangePlans.ParetoFront

/-! `qualifies p v` is strict Pareto dominance: both coordinates weakly
improve and at least one strictly.  It is transitive, and it implies strict
growth in the lexicographic order `(x, y)`.  Among the finitely many points
dominated by the probe, take a lexicographic maximum; nothing can dominate it,
so the reduction to the non-dominated ("skyline") subset is sound. -/

private def Le2 (a b : Point) : Prop := a.x ≤ b.x ∧ a.y ≤ b.y
private def Dom (a b : Point) : Prop := Le2 a b ∧ a ≠ b
private def LexLe (a b : Point) : Prop := a.x < b.x ∨ (a.x = b.x ∧ a.y ≤ b.y)

private theorem qualifies_iff_dom (a b : Point) :
    shape.qualifies a b = true ↔ Dom a b := by
  constructor
  · intro h
    have h' : a.x ≤ b.x ∧ a.y ≤ b.y ∧ (a.x < b.x ∨ a.y < b.y) := by
      simpa [shape, and_assoc] using h
    refine ⟨⟨h'.1, h'.2.1⟩, ?_⟩
    intro he
    subst b
    omega
  · intro h
    have hax : a.x ≤ b.x := h.1.1
    have hay : a.y ≤ b.y := h.1.2
    have h' : a.x ≤ b.x ∧ a.y ≤ b.y ∧ (a.x < b.x ∨ a.y < b.y) := by
      refine ⟨hax, hay, ?_⟩
      by_cases hx : a.x < b.x
      · exact Or.inl hx
      · by_cases hy : a.y < b.y
        · exact Or.inr hy
        · exfalso
          apply h.2
          have hxeq : a.x = b.x := by omega
          have hyeq : a.y = b.y := by omega
          cases a
          cases b
          simp_all
    simpa [shape, and_assoc] using h'

private theorem dom_trans {a b c : Point} (hab : Dom a b) (hbc : Dom b c) : Dom a c := by
  rcases hab with ⟨⟨hax, hay⟩, hneab⟩
  rcases hbc with ⟨⟨hbx, hby⟩, _⟩
  constructor
  · exact ⟨by omega, by omega⟩
  · intro hac
    apply hneab
    have hacx : a.x = c.x := by rw [hac]
    have hacy : a.y = c.y := by rw [hac]
    cases a
    cases b
    cases c
    simp_all
    constructor <;> omega

private theorem dom_lex {a b : Point} (h : Dom a b) : LexLe a b := by
  rcases h with ⟨⟨hx, hy⟩, _⟩
  by_cases hlt : a.x < b.x
  · exact Or.inl hlt
  · exact Or.inr ⟨by omega, hy⟩

private theorem lex_refl (a : Point) : LexLe a a := by
  exact Or.inr ⟨rfl, by omega⟩

private theorem lex_total (a b : Point) : LexLe a b ∨ LexLe b a := by
  by_cases h : a.x < b.x
  · exact Or.inl (Or.inl h)
  · by_cases h' : b.x < a.x
    · exact Or.inr (Or.inl h')
    · have hx : a.x = b.x := by omega
      by_cases hy : a.y ≤ b.y
      · exact Or.inl (Or.inr ⟨hx, hy⟩)
      · exact Or.inr (Or.inr ⟨by omega, by omega⟩)

private theorem lex_trans {a b c : Point} (h1 : LexLe a b) (h2 : LexLe b c) : LexLe a c := by
  rcases h1 with h1 | ⟨h1x, h1y⟩
  · rcases h2 with h2 | ⟨h2x, h2y⟩
    · exact Or.inl (by omega)
    · exact Or.inl (by omega)
  · rcases h2 with h2 | ⟨h2x, h2y⟩
    · exact Or.inl (by omega)
    · exact Or.inr ⟨by omega, by omega⟩

private theorem lex_antisymm {a b : Point} (h1 : LexLe a b) (h2 : LexLe b a) : a = b := by
  rcases h1 with h1 | ⟨h1x, h1y⟩
  · rcases h2 with h2 | ⟨h2x, h2y⟩
    · omega
    · omega
  · rcases h2 with h2 | ⟨h2x, h2y⟩
    · omega
    · have hx : a.x = b.x := h1x
      have hy : a.y = b.y := by omega
      cases a
      cases b
      simp_all

private theorem exists_lex_max (P : Point → Prop) [DecidablePred P] :
    ∀ l : List Point, (∃ v, v ∈ l ∧ P v) →
      ∃ m, m ∈ l ∧ P m ∧ ∀ r, r ∈ l → P r → LexLe r m
  | [], h => by
      rcases h with ⟨v, hv, _⟩
      simp at hv
  | a :: l, h => by
      by_cases hPa : P a
      · by_cases htail : ∃ v, v ∈ l ∧ P v
        · rcases exists_lex_max P l htail with ⟨m, hm, hPm, hmax⟩
          rcases lex_total a m with ham | hma
          · refine ⟨m, by simp [hm], hPm, ?_⟩
            intro r hr hPr
            simp at hr
            rcases hr with rfl | hr
            · exact ham
            · exact hmax r hr hPr
          · refine ⟨a, by simp, hPa, ?_⟩
            intro r hr hPr
            simp at hr
            rcases hr with rfl | hr
            · exact lex_refl _
            · exact lex_trans (hmax r hr hPr) hma
        · refine ⟨a, by simp, hPa, ?_⟩
          intro r hr hPr
          simp at hr
          rcases hr with rfl | hr
          · exact lex_refl _
          · exfalso
            exact htail ⟨r, hr, hPr⟩
      · rcases h with ⟨v, hv, hPv⟩
        simp at hv
        rcases hv with rfl | hv
        · exact False.elim (hPa hPv)
        · rcases exists_lex_max P l ⟨v, hv, hPv⟩ with ⟨m, hm, hPm, hmax⟩
          refine ⟨m, by simp [hm], hPm, ?_⟩
          intro r hr hPr
          simp at hr
          rcases hr with rfl | hr
          · exact False.elim (hPa hPr)
          · exact hmax r hr hPr

theorem sound_ParetoFront_SkylineWitness : ParetoFront.SkylineWitness.Goal := by
  classical
  intro p points
  apply Bool.eq_iff_iff.mpr
  constructor
  · intro hleft
    rcases (List.any_eq_true.mp hleft) with ⟨v, hv, hdomv⟩
    have hDv : Dom p v := (qualifies_iff_dom p v).1 hdomv
    rcases exists_lex_max (fun z => Dom p z) points ⟨v, hv, hDv⟩ with
      ⟨m, hm, hDm, hmax⟩
    rw [List.any_eq_true]
    refine ⟨m, ?_, (qualifies_iff_dom p m).2 hDm⟩
    rw [List.mem_filter]
    constructor
    · exact hm
    · have hfalse : points.any (shape.qualifies m) = false := by
        rw [Bool.eq_false_iff]
        intro hany
        rcases (List.any_eq_true.mp hany) with ⟨r, hr, hdomr⟩
        have hDr : Dom m r := (qualifies_iff_dom m r).1 hdomr
        have hDpr : Dom p r := dom_trans hDm hDr
        have heq : m = r := lex_antisymm (dom_lex hDr) (hmax r hr hDpr)
        exact hDr.2 heq
      simp [hfalse]
  · intro hright
    rcases (List.any_eq_true.mp hright) with ⟨m, hmfilter, hdomm⟩
    rw [List.any_eq_true]
    exact ⟨m, (List.mem_filter.mp hmfilter).1, hdomm⟩

end TinylambOpt.ResearchProofs.Atlas.Goal_ParetoFront_SkylineWitness
