import TinylambOpt.ResearchGoals.QueryRewrites.KeyRoutedJoinStates

/-!
# CachedExtremaAcrossCoveringMasks
-/

set_option maxHeartbeats 0

namespace TinylambOpt.ResearchProofs.QueryRewrites.KeyRoutedJoinStates.CachedExtremaAcrossCoveringMasks

private theorem mem_filterMap_slices_of_mem_selected {selected : List Nat} {slices : List (List Nat)} {value : Nat → Option Int}
    (hcover : ∀ i, i ∈ selected ↔ i ∈ slices.flatten) {v : Int}
    (hv : v ∈ selected.filterMap value) : ∃ s ∈ slices, v ∈ s.filterMap value := by
  rw [List.mem_filterMap] at hv
  rcases hv with ⟨i, hi, hiv⟩
  have hiF : i ∈ slices.flatten := (hcover i).1 hi
  rw [List.mem_flatten] at hiF
  rcases hiF with ⟨s, hs, his⟩
  exact ⟨s, hs, List.mem_filterMap.mpr ⟨i, his, hiv⟩⟩

private theorem mem_selected_filterMap_of_mem_slices {selected : List Nat} {slices : List (List Nat)} {value : Nat → Option Int}
    (hcover : ∀ i, i ∈ selected ↔ i ∈ slices.flatten) {s : List Nat} (hs : s ∈ slices) {v : Int}
    (hv : v ∈ s.filterMap value) : v ∈ selected.filterMap value := by
  rw [List.mem_filterMap] at hv ⊢
  rcases hv with ⟨i, his, hiv⟩
  refine ⟨i, (hcover i).2 (List.mem_flatten.mpr ⟨s, hs, his⟩), hiv⟩

private theorem min?_eq_of_mem_bounds {xs ys : List Int}
    (hxy : ∀ v ∈ xs, ∃ w ∈ ys, w ≤ v)
    (hyx : ∀ w ∈ ys, ∃ v ∈ xs, v ≤ w) : xs.min? = ys.min? := by
  cases hxs : xs.min? with
  | none =>
      rw [List.min?_eq_none_iff] at hxs
      cases hys : ys.min? with
      | none => rfl
      | some m =>
          have hm : m ∈ ys := List.min?_mem hys
          rcases hyx m hm with ⟨v, hv, _⟩
          exact absurd hv (by simp [hxs])
  | some a =>
      cases hys : ys.min? with
      | none =>
          rw [List.min?_eq_none_iff] at hys
          have ha : a ∈ xs := List.min?_mem hxs
          rcases hxy a ha with ⟨w, hw, _⟩
          exact absurd hw (by simp [hys])
      | some b =>
          have ha : a ∈ xs := List.min?_mem hxs
          have hb : b ∈ ys := List.min?_mem hys
          have hab : a ≤ b := by
            rcases hyx b hb with ⟨v, hv, hvb⟩
            have hav : a ≤ v := ((List.le_min?_iff hxs).1 (by omega) _ hv)
            omega
          have hba : b ≤ a := by
            rcases hxy a ha with ⟨w, hw, hwa⟩
            have hbw : b ≤ w := ((List.le_min?_iff hys).1 (by omega) _ hw)
            omega
          have : a = b := by omega
          simp [this]

private theorem max?_eq_of_mem_bounds {xs ys : List Int}
    (hxy : ∀ v ∈ xs, ∃ w ∈ ys, v ≤ w)
    (hyx : ∀ w ∈ ys, ∃ v ∈ xs, w ≤ v) : xs.max? = ys.max? := by
  cases hxs : xs.max? with
  | none =>
      rw [List.max?_eq_none_iff] at hxs
      cases hys : ys.max? with
      | none => rfl
      | some m =>
          have hm : m ∈ ys := List.max?_mem hys
          rcases hyx m hm with ⟨v, hv, _⟩
          exact absurd hv (by simp [hxs])
  | some a =>
      cases hys : ys.max? with
      | none =>
          rw [List.max?_eq_none_iff] at hys
          have ha : a ∈ xs := List.max?_mem hxs
          rcases hxy a ha with ⟨w, hw, _⟩
          exact absurd hw (by simp [hys])
      | some b =>
          have ha : a ∈ xs := List.max?_mem hxs
          have hb : b ∈ ys := List.max?_mem hys
          have hab : a ≤ b := by
            rcases hxy a ha with ⟨w, hw, hwa⟩
            have hwb : w ≤ b := ((List.max?_le_iff hys).1 (by omega) _ hw)
            omega
          have hba : b ≤ a := by
            rcases hyx b hb with ⟨v, hv, hvb⟩
            have hva : v ≤ a := ((List.max?_le_iff hxs).1 (by omega) _ hv)
            omega
          have : a = b := by omega
          simp [this]

theorem sound : TinylambOpt.ResearchGoals.QueryRewrites.KeyRoutedJoinStates.CachedExtremaAcrossCoveringMasks.Goal := by
  intro selected slices value hcover
  apply Prod.ext
  · apply min?_eq_of_mem_bounds
    · intro v hv
      rw [List.mem_filterMap] at hv
      rcases hv with ⟨s, hs, hsv⟩
      have hvmem : v ∈ s.filterMap value := List.min?_mem hsv
      have hvsel : v ∈ selected.filterMap value := mem_selected_filterMap_of_mem_slices hcover hs hvmem
      exact ⟨v, hvsel, (by omega)⟩
    · intro w hw
      rcases mem_filterMap_slices_of_mem_selected hcover hw with ⟨s, hs, hws⟩
      cases hsm : (s.filterMap value).min? with
      | none =>
          rw [List.min?_eq_none_iff, List.filterMap_eq_nil_iff] at hsm
          rcases (List.mem_filterMap.mp hws) with ⟨i, his, hiv⟩
          exact absurd (hsm i his) (by simp [hiv])
      | some sm =>
          have hsm_mem : sm ∈ slices.filterMap (fun s => (s.filterMap value).min?) :=
            List.mem_filterMap.mpr ⟨s, hs, hsm⟩
          exact ⟨sm, hsm_mem, ((List.le_min?_iff hsm).1 (by omega) _ hws)⟩
  · apply max?_eq_of_mem_bounds
    · intro v hv
      rw [List.mem_filterMap] at hv
      rcases hv with ⟨s, hs, hsv⟩
      have hvmem : v ∈ s.filterMap value := List.max?_mem hsv
      have hvsel : v ∈ selected.filterMap value := mem_selected_filterMap_of_mem_slices hcover hs hvmem
      exact ⟨v, hvsel, (by omega)⟩
    · intro w hw
      rcases mem_filterMap_slices_of_mem_selected hcover hw with ⟨s, hs, hws⟩
      cases hsm : (s.filterMap value).max? with
      | none =>
          rw [List.max?_eq_none_iff, List.filterMap_eq_nil_iff] at hsm
          rcases (List.mem_filterMap.mp hws) with ⟨i, his, hiv⟩
          exact absurd (hsm i his) (by simp [hiv])
      | some sm =>
          have hsm_mem : sm ∈ slices.filterMap (fun s => (s.filterMap value).max?) :=
            List.mem_filterMap.mpr ⟨s, hs, hsm⟩
          exact ⟨sm, hsm_mem, ((List.max?_le_iff hsm).1 (by omega) _ hws)⟩

#print axioms TinylambOpt.ResearchProofs.QueryRewrites.KeyRoutedJoinStates.CachedExtremaAcrossCoveringMasks.sound
