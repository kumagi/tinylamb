import TinylambOpt.ResearchProofs.JitFrontiers.RelationalGuards.Common

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitFrontiers.RelationalGuards.ClampedDiffLemmas

open TinylambOpt.ResearchGoals.JitFrontiers.RelationalGuards
open TinylambOpt.ResearchProofs.JitFrontiers.RelationalGuards.Common
open TinylambOpt.ResearchProofs.JitFrontiers.RelationalGuards.CorrelatedSubLemmas

abbrev cd := TinylambOpt.ResearchGoals.JitFrontiers.RelationalGuards.ClampedDiff.expr

theorem min_mono (t a b : Int) (hab : a ≤ b) : min t a ≤ min t b := by
  exact Int.le_min.mpr ⟨Int.min_le_left _ _, Int.le_trans (Int.min_le_right _ _) hab⟩

theorem clamp_mono (t a b : Int) (hab : a ≤ b) :
    max (-t) (min t a) ≤ max (-t) (min t b) := by
  rw [Int.max_le]
  constructor
  · exact Int.le_max_left _ _
  · exact Int.le_trans (min_mono t a b hab) (Int.le_max_right _ _)

theorem cd_dom_hi (t : Int) (z : Zone) (p : Point) (hp : inZone z p = true) :
    ∃ v, v ∈ vertices z (cd.cuts t) ∧ cd.score t p ≤ cd.score t v := by
  rcases score_range hp with ⟨hlo, hhi⟩
  rcases maxDiff_vertex (z := z) ⟨p, hp⟩ with ⟨v, hv, hvz, hvs⟩
  refine ⟨v, vertices_mono_ds (z := z) (ds := []) (ds' := cd.cuts t) (by intro d hd; simp at hd) hv, ?_⟩
  change max (-t) (min t (p.1-p.2)) ≤ max (-t) (min t (v.1-v.2))
  apply clamp_mono
  omega

theorem cd_dom_lo (t : Int) (z : Zone) (p : Point) (hp : inZone z p = true) :
    ∃ v, v ∈ vertices z (cd.cuts t) ∧ cd.score t v ≤ cd.score t p := by
  rcases score_range hp with ⟨hlo, hhi⟩
  rcases minDiff_vertex (z := z) ⟨p, hp⟩ with ⟨v, hv, hvz, hvs⟩
  refine ⟨v, vertices_mono_ds (z := z) (ds := []) (ds' := cd.cuts t) (by intro d hd; simp at hd) hv, ?_⟩
  change max (-t) (min t (v.1-v.2)) ≤ max (-t) (min t (p.1-p.2))
  apply clamp_mono
  omega

theorem minimalHull_cd : minimalHull cd := minimalHull_of_dom cd_dom_hi cd_dom_lo

theorem coverage_cd : ∀ t z p, inZone z p = true → ∀ lo hi, estimate cd t z = some (lo,hi) →
    lo ≤ cd.score t p ∧ cd.score t p ≤ hi := coverage_of_dom cd_dom_hi cd_dom_lo

end TinylambOpt.ResearchProofs.JitFrontiers.RelationalGuards.ClampedDiffLemmas
