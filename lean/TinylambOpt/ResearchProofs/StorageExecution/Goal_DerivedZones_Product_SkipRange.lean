import TinylambOpt.ResearchGoals.StorageExecution.DerivedZones

/-!
Scan pruning can exploit derived bounds only when every matching row remains covered by
the block summary.

Proved

- `Product.SkipRange` — Drop a whole page for a range test, preserving empty and NULL
  pages.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_DerivedZones_Product_SkipRange

open TinylambOpt.ResearchGoals.StorageExecution.DerivedZones
open TinylambOpt.ResearchGoals.StorageExecution.DerivedZones.Product

/-- Every value of `scores` comes from a non-NULL row of the page. -/
private theorem score_mem {t : Nat} {p : List (Option Row)} {v : Nat}
    (hv : v ∈ scores kernel t p) :
    ∃ r ∈ p, ∃ x y, r = some (x, y) ∧ v = x * y := by
  rcases List.mem_filterMap.mp hv with ⟨r, hr, hmap⟩
  rcases Option.map_eq_some_iff.mp hmap with ⟨xy, hrxy, hscore⟩
  rcases xy with ⟨x, y⟩
  exact ⟨r, hr, x, y, hrxy, by simpa [kernel] using hscore.symm⟩

/-- The product of two independent intervals is a sound interval for `x * y`. -/
private theorem soundInterval_product : soundInterval kernel := by
  intro t a b x y hax hby
  simp only [inside] at hax hby ⊢
  exact ⟨Nat.mul_le_mul hax.1 hby.1, Nat.mul_le_mul hax.2 hby.2⟩

/-- A certified scalar interval justifies dropping a page for a range test. -/
private theorem skipRange_of_sound {f : Kernel} (hs : soundInterval f) : skipRange f := by
  intro t p a b lo hi hc hn
  unfold selected
  apply List.filter_eq_nil_iff.mpr
  intro v hv hpass
  have hpred : (lo ≤ v && v < hi) = true := hpass
  simp only [Bool.and_eq_true, decide_eq_true_eq] at hpred
  rcases hpred with ⟨hlo, hhi'⟩
  rcases List.mem_filterMap.mp hv with ⟨r, hr, hmap⟩
  rcases Option.map_eq_some_iff.mp hmap with ⟨xy, hrxy, hscore⟩
  rcases xy with ⟨x, y⟩
  have hcov := hc r hr x y hrxy
  have hb : inside (f.bounds t a b) (f.score t (x, y)) :=
    hs t a b x y hcov.1 hcov.2
  simp only [inside] at hb
  have hl : lo ≤ (f.bounds t a b).hi :=
    Nat.le_trans hlo (by simpa [hscore] using hb.2)
  have hr' : (f.bounds t a b).lo < hi :=
    Nat.lt_of_le_of_lt (by simpa [hscore] using hb.1) hhi'
  have hboth : lo < hi := Nat.lt_of_le_of_lt hlo hhi'
  have hallows : allows (f.bounds t a b) lo hi = true := by
    simp [allows, hl, hr', hboth]
  rw [hallows] at hn
  cases hn

theorem sound_SkipRange : Product.SkipRange.Goal :=
  skipRange_of_sound soundInterval_product

#guard selected kernel 0 [some (1, 8), none, some (8, 1)] 50 100 == []

end TinylambOpt.ResearchProofs.StorageExecution.Goal_DerivedZones_Product_SkipRange
