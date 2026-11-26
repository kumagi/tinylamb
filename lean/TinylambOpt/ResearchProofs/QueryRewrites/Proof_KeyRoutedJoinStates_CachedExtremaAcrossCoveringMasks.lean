import TinylambOpt.ResearchGoals.QueryRewrites.KeyRoutedJoinStates
import TinylambOpt.ResearchProofs.QueryRewrites.FileRangesAndExtrema.PairedExtremaReduction
import TinylambOpt.ResearchProofs.QueryRewrites.FileRangesAndExtrema.QualifiedPageExtremaReuse

/-!
Proof contribution for QueryRewrites_KeyRoutedJoinStates_CachedExtremaAcrossCoveringMasks. The theorem below
records a planning or execution decision that preserves the modeled
SQL result, or an explicit counterexample when the proposed rewrite is
unsound.
-/


namespace TinylambOpt.ResearchProofs.QueryRewrites.KeyRoutedJoinStates.Goal_CachedExtremaAcrossCoveringMasks

open TinylambOpt.ResearchGoals.QueryRewrites.KeyRoutedJoinStates


/-
# CachedExtremaAcrossCoveringMasks

Overlapping cached group slices may be merged for MIN/MAX as long as their union
covers exactly the selected row IDs. Only the set of non-NULL values matters for
extrema: recomputing a minimum from each slice's cached minimum, or a maximum
from each slice's cached maximum, preserves both components of the aggregate
state even when slices overlap or have different order.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

open TinylambOpt.ResearchGoals.QueryRewrites

theorem krs_mem_values_flatten {selected : List Nat} {slices : List (List Nat)}
    {value : Nat → Option Int} (h : ∀ i, i ∈ selected ↔ i ∈ slices.flatten) :
    ∀ v, v ∈ selected.filterMap value ↔
      v ∈ (slices.map (fun s => s.filterMap value)).flatten := by
  intro v
  constructor
  · intro hv
    rcases List.mem_filterMap.mp hv with ⟨i, hiSelected, hvi⟩
    have hiFlat : i ∈ slices.flatten := (h i).mp hiSelected
    rcases List.mem_flatten.mp hiFlat with ⟨s, hsSlices, hiS⟩
    have hvS : v ∈ s.filterMap value := List.mem_filterMap.mpr ⟨i, hiS, hvi⟩
    exact List.mem_flatten.mpr
      ⟨s.filterMap value, List.mem_map.mpr ⟨s, hsSlices, rfl⟩, hvS⟩
  · intro hv
    rcases List.mem_flatten.mp hv with ⟨vs, hvsmap, hvvs⟩
    rcases List.mem_map.mp hvsmap with ⟨s, hsSlices, rfl⟩
    rcases List.mem_filterMap.mp hvvs with ⟨i, hiS, hvi⟩
    have hiFlat : i ∈ slices.flatten := List.mem_flatten.mpr ⟨s, hsSlices, hiS⟩
    have hiSelected : i ∈ selected := (h i).mpr hiFlat
    exact List.mem_filterMap.mpr ⟨i, hiSelected, hvi⟩

theorem krs_filterMap_min_eq_flat_min (slices : List (List Nat)) (value : Nat → Option Int) :
    (slices.filterMap (fun s => (s.filterMap value).min?)).min? =
      (slices.map (fun s => s.filterMap value)).flatten.min? := by
  calc
    (slices.filterMap (fun s => (s.filterMap value).min?)).min? =
        ((slices.map (fun s => ((s.filterMap value).min?, (s.filterMap value).max?))).filterMap
          Prod.fst).min? := by
            congr 1
            induction slices with
            | nil => rfl
            | cons s ss ih =>
                simp only [List.filterMap_cons, List.map_cons]
                cases hmin : (s.filterMap value).min? with
                | none => simp [hmin, ih]
                | some a => simp [hmin, ih]
    _ = (slices.map (fun s => s.filterMap value)).flatten.min? :=
      TinylambOpt.ResearchProofs.QueryRewrites.FileRangesAndExtrema.QualifiedPageExtremaReuse.min_flat_extrema
        slices (fun s => s.filterMap value)

theorem krs_filterMap_max_eq_flat_max (slices : List (List Nat)) (value : Nat → Option Int) :
    (slices.filterMap (fun s => (s.filterMap value).max?)).max? =
      (slices.map (fun s => s.filterMap value)).flatten.max? := by
  calc
    (slices.filterMap (fun s => (s.filterMap value).max?)).max? =
        ((slices.map (fun s => ((s.filterMap value).min?, (s.filterMap value).max?))).filterMap
          Prod.snd).max? := by
            congr 1
            induction slices with
            | nil => rfl
            | cons s ss ih =>
                simp only [List.filterMap_cons, List.map_cons]
                cases hmax : (s.filterMap value).max? with
                | none => simp [hmax, ih]
                | some a => simp [hmax, ih]
    _ = (slices.map (fun s => s.filterMap value)).flatten.max? :=
      TinylambOpt.ResearchProofs.QueryRewrites.FileRangesAndExtrema.QualifiedPageExtremaReuse.max_flat_extrema
        slices (fun s => s.filterMap value)

theorem sound : TinylambOpt.ResearchGoals.QueryRewrites.KeyRoutedJoinStates.CachedExtremaAcrossCoveringMasks.Goal := by
  intro selected slices value hcover
  have hmem := krs_mem_values_flatten (selected := selected) (slices := slices) (value := value) hcover
  apply Prod.ext
  · rw [krs_filterMap_min_eq_flat_min slices value]
    exact TinylambOpt.ResearchProofs.QueryRewrites.FileRangesAndExtrema.PairedExtremaReduction.min?_eq_of_cover
      (xs := selected.filterMap value)
      (ys := (slices.map (fun s => s.filterMap value)).flatten)
      (by intro y hy; exact (hmem y).mpr hy)
      (by
        intro x hx
        exact ⟨x, (hmem x).mp hx, by omega⟩)
  · rw [krs_filterMap_max_eq_flat_max slices value]
    exact TinylambOpt.ResearchProofs.QueryRewrites.FileRangesAndExtrema.PairedExtremaReduction.max?_eq_of_cover
      (xs := selected.filterMap value)
      (ys := (slices.map (fun s => s.filterMap value)).flatten)
      (by intro y hy; exact (hmem y).mpr hy)
      (by
        intro x hx
        exact ⟨x, (hmem x).mp hx, by omega⟩)
end TinylambOpt.ResearchProofs.QueryRewrites.KeyRoutedJoinStates.Goal_CachedExtremaAcrossCoveringMasks
