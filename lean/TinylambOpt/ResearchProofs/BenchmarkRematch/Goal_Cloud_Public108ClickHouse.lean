import TinylambOpt.ResearchGoals.BenchmarkRematch.Cloud

set_option maxHeartbeats 0
namespace TinylambOpt.ResearchProofs.BenchmarkRematch.Goal_Cloud_Public108ClickHouse
open TinylambOpt.ResearchGoals.BenchmarkRematch
open TinylambOpt.ResearchGoals.BenchmarkRematch.Cloud

theorem min_iff (xs : List Int) (a : Int) : xs.min? = some a ↔ a ∈ xs ∧ ∀ b ∈ xs, a ≤ b :=
  List.min?_eq_some_iff

theorem max_iff (xs : List Int) (a : Int) : xs.max? = some a ↔ a ∈ xs ∧ ∀ b ∈ xs, b ≤ a :=
  List.max?_eq_some_iff

theorem min_flat (L : List (List Int)) : (L.filterMap List.min?).min? = L.flatten.min? := by
  cases hf : L.flatten.min? with
  | none =>
    have : L.flatten = [] := by
      cases h : L.flatten with
      | nil => rfl
      | cons x t => rw [h] at hf; simp [List.min?_cons'] at hf
    have hL : ∀ l ∈ L, l = [] := by
      intro l hl
      have : ∀ x ∈ l, x ∈ L.flatten := fun x hx => List.mem_flatten.mpr ⟨l, hl, hx⟩
      rw [‹L.flatten = []›] at this
      cases l with
      | nil => rfl
      | cons y t => exact absurd (this y (by simp)) (by simp)
    have : L.filterMap List.min? = [] := by
      rw [List.filterMap_eq_nil_iff]
      intro l hl
      rw [hL l hl]; rfl
    rw [this]; rfl
  | some m =>
    rw [min_iff] at hf
    rw [min_iff]
    obtain ⟨hm, hle⟩ := hf
    obtain ⟨l, hl, hml⟩ := List.mem_flatten.mp hm
    refine ⟨?_, ?_⟩
    · cases hl' : l.min? with
      | none =>
        have h0 := List.min?_eq_none_iff.mp hl'
        rw [h0] at hml; simp at hml
      | some m' =>
        have := (min_iff l m').mp hl'
        have h1 : m' ∈ L.flatten := List.mem_flatten.mpr ⟨l, hl, this.1⟩
        have h2 := hle m' h1
        have h3 := this.2 m hml
        have : m' = m := by omega
        subst this
        exact List.mem_filterMap.mpr ⟨l, hl, hl'⟩
    · intro b hb
      obtain ⟨l', hl', hb'⟩ := List.mem_filterMap.mp hb
      have := (min_iff l' b).mp hb'
      exact hle b (List.mem_flatten.mpr ⟨l', hl', this.1⟩)

theorem max_flat (L : List (List Int)) : (L.filterMap List.max?).max? = L.flatten.max? := by
  cases hf : L.flatten.max? with
  | none =>
    have : L.flatten = [] := by
      cases h : L.flatten with
      | nil => rfl
      | cons x t => rw [h] at hf; simp [List.max?_cons'] at hf
    have hL : ∀ l ∈ L, l = [] := by
      intro l hl
      have : ∀ x ∈ l, x ∈ L.flatten := fun x hx => List.mem_flatten.mpr ⟨l, hl, hx⟩
      rw [‹L.flatten = []›] at this
      cases l with
      | nil => rfl
      | cons y t => exact absurd (this y (by simp)) (by simp)
    have : L.filterMap List.max? = [] := by
      rw [List.filterMap_eq_nil_iff]
      intro l hl
      rw [hL l hl]; rfl
    rw [this]; rfl
  | some m =>
    rw [max_iff] at hf
    rw [max_iff]
    obtain ⟨hm, hle⟩ := hf
    obtain ⟨l, hl, hml⟩ := List.mem_flatten.mp hm
    refine ⟨?_, ?_⟩
    · cases hl' : l.max? with
      | none =>
        have h0 := List.max?_eq_none_iff.mp hl'
        rw [h0] at hml; simp at hml
      | some m' =>
        have := (max_iff l m').mp hl'
        have h1 : m' ∈ L.flatten := List.mem_flatten.mpr ⟨l, hl, this.1⟩
        have h2 := hle m' h1
        have h3 := this.2 m hml
        have : m' = m := by omega
        subst this
        exact List.mem_filterMap.mpr ⟨l, hl, hl'⟩
    · intro b hb
      obtain ⟨l', hl', hb'⟩ := List.mem_filterMap.mp hb
      have := (max_iff l' b).mp hb'
      exact hle b (List.mem_flatten.mpr ⟨l', hl', this.1⟩)

theorem sound : Public108ClickHouse.Goal := by
  intro pages passes metric col
  dsimp only
  have hs : ∀ p : List Nat, (if p.all passes then ((p.filterMap (metric col)).min?, (p.filterMap (metric col)).max?)
      else (((p.filter passes).filterMap (metric col)).min?, ((p.filter passes).filterMap (metric col)).max?))
      = (((p.filter passes).filterMap (metric col)).min?, ((p.filter passes).filterMap (metric col)).max?) := by
    intro p
    by_cases h : p.all passes = true
    · have : p.filter passes = p := List.filter_eq_self.mpr (by simpa using h)
      simp [h, this]
    · simp [h]
  simp only [hs]
  have hV : List.filterMap (metric col) (List.filter passes pages.flatten) =
      (pages.map (fun p => List.filterMap (metric col) (List.filter passes p))).flatten := by
    simp [List.filter_flatten, List.filterMap_flatten, List.map_map, Function.comp_def]
  rw [hV, ← min_flat, ← max_flat]
  simp [List.filterMap_map, Function.comp_def]

end TinylambOpt.ResearchProofs.BenchmarkRematch.Goal_Cloud_Public108ClickHouse

#print axioms TinylambOpt.ResearchProofs.BenchmarkRematch.Goal_Cloud_Public108ClickHouse.sound
