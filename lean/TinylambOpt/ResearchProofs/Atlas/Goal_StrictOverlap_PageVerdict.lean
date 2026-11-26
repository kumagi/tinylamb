import TinylambOpt.ResearchGoals.Atlas.RangePlans

/-!
An ordered access path needs to preserve the rows and boundaries selected by the SQL
range predicate.

Refuted by counterexample

- `StrictOverlap.PageVerdict` — `∀ b p g, valid g → (∀ r ∈ g.rows, bounded b r.1) →
  pageScan shape b p g = scan shape p g.rows`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_StrictOverlap_PageVerdict

open TinylambOpt.ResearchGoals.Atlas.RangePlans
open TinylambOpt.ResearchGoals.Atlas.RangePlans.StrictOverlap

/-- probe が [0,2)、build が重み 7 の空期間 [1,1) のとき、ページ判定は
    符号付き矩形 [-2,2)×[1,3) がページ全体を含むとみなして 7 を返すが、
    正の長さの交差を要求する走査は 0 を返す。 -/
theorem disproof_StrictOverlap_PageVerdict : ¬ StrictOverlap.PageVerdict.Goal := by
  intro h
  have hres := h 2 (⟨0, 2⟩ : Point)
    (⟨⟨1, 1⟩, ⟨1, 1⟩, [(⟨1, 1⟩, 7)]⟩ : Page)
    (by
      intro r hr
      simp at hr
      subst r
      native_decide)
    (by
      intro r hr
      simp at hr
      subst r
      unfold bounded
      native_decide)
  have hne :
      pageScan StrictOverlap.shape 2 (⟨0, 2⟩ : Point)
          (⟨⟨1, 1⟩, ⟨1, 1⟩, [(⟨1, 1⟩, 7)]⟩ : Page) ≠
        scan StrictOverlap.shape (⟨0, 2⟩ : Point) ([(⟨1, 1⟩, 7)] : Rows) := by
    native_decide
  exact hne hres

end TinylambOpt.ResearchProofs.Atlas.Goal_StrictOverlap_PageVerdict
