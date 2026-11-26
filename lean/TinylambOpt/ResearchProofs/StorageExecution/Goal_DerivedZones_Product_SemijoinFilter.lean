import TinylambOpt.ResearchGoals.StorageExecution.DerivedZones

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_DerivedZones_Product_SemijoinFilter

open TinylambOpt.ResearchGoals.StorageExecution.DerivedZones
open TinylambOpt.ResearchGoals.StorageExecution.DerivedZones.Product

/-- Every score of a covered page lies in the componentwise product rectangle:
multiplication of naturals is monotone in both arguments. -/
private theorem score_in_bounds (t : Nat) (p : List (Option Row)) (a b : Bounds)
    (hcov : covers p a b) (v : Nat) (hv : v ∈ scores kernel t p) :
    (a.lo * b.lo ≤ v && v ≤ a.hi * b.hi) = true := by
  unfold scores at hv
  rw [List.mem_filterMap] at hv
  rcases hv with ⟨r, hrp, hr⟩
  cases r with
  | none => simp at hr
  | some xy =>
      rcases xy with ⟨x, y⟩
      have hb := hcov (some (x, y)) hrp x y rfl
      simp only [inside] at hb
      rcases hb with ⟨⟨haxl, haxh⟩, ⟨hbyl, hbyh⟩⟩
      simp only [Option.map_some, Option.some.injEq] at hr
      subst v
      dsimp [kernel]
      simp [Nat.mul_le_mul haxl hbyl, Nat.mul_le_mul haxh hbyh]

/-- Restricting the build keys to the page's score interval is a no-op on
scores actually produced by the page, so the semijoin filter is unchanged. -/
theorem sound_SemijoinFilter : SemijoinFilter.Goal := by
  intro t p a b ks hcov
  change (scores kernel t p).filter ks.contains =
    (scores kernel t p).filter
      ((ks.filter (fun k => a.lo * b.lo ≤ k && k ≤ a.hi * b.hi)).contains)
  apply List.filter_congr
  intro v hv
  have hvR := score_in_bounds t p a b hcov v hv
  simp only [List.contains]
  simp [hvR]

end TinylambOpt.ResearchProofs.StorageExecution.Goal_DerivedZones_Product_SemijoinFilter
