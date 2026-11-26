import TinylambOpt.ResearchGoals.Atlas.OrderedMeasures

/-!
SQL execution can use cumulative counts or ordered measures to answer range and rank
queries under the modeled bounds.

Proved

- `RisePairs.RankCoding` — `∀ x (f : Int → Int), (∀ a ∈ x, ∀ b ∈ x, f a < f b ↔ a < b)
  → measure (x.map f) = measure x`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_RisePairs_RankCoding

open TinylambOpt.ResearchGoals.Atlas.OrderedMeasures
open TinylambOpt.ResearchGoals.Atlas.OrderedMeasures.RisePairs

private theorem pairs_cons (a : Int) (xs : Series) :
    pairs (a :: xs) = xs.map (fun b => (a, b)) ++ pairs xs := by
  simp only [pairs, List.zipIdx_cons', List.flatMap_cons, List.flatMap_map,
    List.drop_succ_cons, List.drop_zero]
  rfl

private theorem mem_pairs {x : Series} {p : Int × Int} (h : p ∈ pairs x) :
    p.1 ∈ x ∧ p.2 ∈ x := by
  induction x with
  | nil => simp [pairs] at h
  | cons a xs ih =>
      rw [pairs_cons] at h
      rw [List.mem_append] at h
      rcases h with h | h
      · rw [List.mem_map] at h
        obtain ⟨b, hb, rfl⟩ := h
        exact ⟨List.mem_cons_self, List.mem_cons_of_mem a hb⟩
      · obtain ⟨h1, h2⟩ := ih h
        exact ⟨List.mem_cons_of_mem a h1, List.mem_cons_of_mem a h2⟩

private theorem pairs_map (f : Int → Int) (x : Series) :
    pairs (x.map f) = (pairs x).map (fun p => (f p.1, f p.2)) := by
  induction x with
  | nil => rfl
  | cons a xs ih =>
      rw [List.map_cons, pairs_cons, pairs_cons, ih, List.map_append,
        List.map_map, List.map_map]
      rfl

private theorem countP_map_comp {α β : Type} (P : β → Bool) (g : α → β) (xs : List α) :
    (xs.map g).countP P = xs.countP (fun x => P (g x)) := by
  induction xs with
  | nil => rfl
  | cons x xs ih =>
      by_cases h : P (g x) = true <;> simp [h, ih]

private theorem countP_congr_bool {α : Type} (P Q : α → Bool) (l : List α)
    (h : ∀ a ∈ l, P a = Q a) : l.countP P = l.countP Q := by
  induction l with
  | nil => rfl
  | cons a as ih =>
      rw [List.countP_cons, List.countP_cons, h a (by simp),
        ih (fun b hb => h b (by simp [hb]))]

theorem sound_RisePairs_RankCoding : RisePairs.RankCoding.Goal := by
  intro x f hf
  unfold RisePairs.measure
  rw [pairs_map]
  rw [countP_map_comp]
  apply congrArg Int.ofNat
  apply countP_congr_bool
  intro p hp
  have hmem := mem_pairs hp
  have hiff : f p.1 < f p.2 ↔ p.1 < p.2 := hf p.1 hmem.1 p.2 hmem.2
  simp [rising, hiff]

end TinylambOpt.ResearchProofs.Atlas.Goal_RisePairs_RankCoding
