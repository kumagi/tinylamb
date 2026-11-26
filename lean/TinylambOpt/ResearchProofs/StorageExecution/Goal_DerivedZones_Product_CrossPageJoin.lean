import TinylambOpt.ResearchGoals.StorageExecution.DerivedZones

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_DerivedZones_Product_CrossPageJoin

open TinylambOpt.ResearchGoals.StorageExecution.DerivedZones
open TinylambOpt.ResearchGoals.StorageExecution.DerivedZones.Product

private theorem score_mem {t : Nat} {p : List (Option Row)} {v : Nat}
    (hv : v ∈ scores kernel t p) :
    ∃ r ∈ p, ∃ x y, r = some (x, y) ∧ v = kernel.score t (x, y) := by
  rcases List.mem_filterMap.mp hv with ⟨r, hr, hmap⟩
  rcases Option.map_eq_some_iff.mp hmap with ⟨xy, hrxy, hscore⟩
  rcases xy with ⟨x, y⟩
  exact ⟨r, hr, x, y, hrxy, hscore.symm⟩

private theorem score_bounds {t : Nat} {p : List (Option Row)} {a b : Bounds}
    (hc : covers p a b) {v : Nat} (hv : v ∈ scores kernel t p) :
    inside (kernel.bounds t a b) v := by
  rcases score_mem hv with ⟨r, hr, x, y, hrxy, rfl⟩
  have hcov := hc r hr x y hrxy
  exact ⟨Nat.mul_le_mul hcov.1.1 hcov.2.1, Nat.mul_le_mul hcov.1.2 hcov.2.2⟩

theorem sound_CrossPageJoin : Product.CrossPageJoin.Goal := by
  intro t u p q a b c d hcp hcq hsep x hx hxq
  have hb1 := score_bounds (t := t) (p := p) (a := a) (b := b) hcp hx
  have hb2 := score_bounds (t := u) (p := q) (a := c) (b := d) hcq hxq
  simp only [inside, kernel] at hb1 hb2 hsep
  omega

end TinylambOpt.ResearchProofs.StorageExecution.Goal_DerivedZones_Product_CrossPageJoin
