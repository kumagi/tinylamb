import TinylambOpt.ResearchGoals.StorageExecution.DerivedZones

/-!
Scan pruning can exploit derived bounds only when every matching row remains covered by
the block summary.

Proved

- `CappedPair.SemijoinFilter` — Apply the footer to the build-key set before probing;
  retain left bag order and duplicates.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_DerivedZones_CappedPair_SemijoinFilter

open TinylambOpt.ResearchGoals.StorageExecution.DerivedZones
open TinylambOpt.ResearchGoals.StorageExecution.DerivedZones.CappedPair

private theorem score_mem {f : Kernel} {t : Nat} {p : List (Option Row)} {v : Nat}
    (hv : v ∈ scores f t p) :
    ∃ r ∈ p, ∃ x y, r = some (x,y) ∧ v = f.score t (x,y) := by
  rcases List.mem_filterMap.mp hv with ⟨r, hr, hmap⟩
  rcases Option.map_eq_some_iff.mp hmap with ⟨xy, hrxy, hscore⟩
  rcases xy with ⟨x,y⟩
  exact ⟨r, hr, x, y, hrxy, hscore.symm⟩

private theorem score_bounds {f : Kernel} {t : Nat} {p : List (Option Row)} {a b : Bounds}
    (hs : soundInterval f) (hc : covers p a b) {v : Nat} (hv : v ∈ scores f t p) :
    inside (f.bounds t a b) v := by
  rcases score_mem hv with ⟨r, hr, x, y, hrxy, rfl⟩
  exact hs t a b x y (hc r hr x y hrxy).1 (hc r hr x y hrxy).2

private theorem semijoinFilter_of_sound {f : Kernel} (hs : soundInterval f) : semijoinFilter f := by
  intro t p a b ks hc
  dsimp only
  apply List.filter_congr
  intro v hv
  have hb := score_bounds hs hc hv
  by_cases hmem : v ∈ ks
  · simp [List.contains_eq_mem, hmem, hb.1, hb.2]
  · have hnot : v ∉ ks.filter (fun k => (f.bounds t a b).lo ≤ k && k ≤ (f.bounds t a b).hi) := by
      simp [List.mem_filter, hmem]
    simp [List.contains_eq_mem, hmem, hnot]

private theorem nat_min_le_min {a b c d : Nat} (h1 : a ≤ c) (h2 : b ≤ d) :
    min a b ≤ min c d := by
  apply Nat.le_min.mpr
  constructor
  · exact Nat.le_trans (Nat.min_le_left a b) h1
  · exact Nat.le_trans (Nat.min_le_right a b) h2

private theorem cappedPair_sound : soundInterval kernel := by
  intro t a b x y hax hby
  simp only [kernel, inside]
  exact ⟨nat_min_le_min (Nat.le_refl t) (nat_min_le_min hax.1 hby.1),
    nat_min_le_min (Nat.le_refl t) (nat_min_le_min hax.2 hby.2)⟩

theorem sound_SemijoinFilter : SemijoinFilter.Goal :=
  semijoinFilter_of_sound cappedPair_sound

end TinylambOpt.ResearchProofs.StorageExecution.Goal_DerivedZones_CappedPair_SemijoinFilter
