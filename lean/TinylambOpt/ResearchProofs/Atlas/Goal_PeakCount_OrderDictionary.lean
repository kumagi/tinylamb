import TinylambOpt.ResearchGoals.Atlas.OrderedMeasures

/-!
SQL execution can use cumulative counts or ordered measures to answer range and rank
queries under the modeled bounds.

Proved

- `PeakCount.OrderDictionary` — `∀ x (f : Int → Int), (∀ a ∈ x, ∀ b ∈ x, a < b ↔ f a <
  f b) → measure (x.map f) = measure x`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_PeakCount_OrderDictionary

open TinylambOpt.ResearchGoals.Atlas.OrderedMeasures
open TinylambOpt.ResearchGoals.Atlas.OrderedMeasures.PeakCount

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

private theorem zipIdx_map_eq (f : Int → Int) (x : Series) :
    (x.map f).zipIdx = x.zipIdx.map (fun p => (f p.1, p.2)) := by
  apply List.ext_getElem?
  intro j
  cases h : x[j]? with
  | none => simp [List.getElem?_zipIdx, List.getElem?_map, h]
  | some v => simp [List.getElem?_zipIdx, List.getElem?_map, h]

private theorem fst_mem_of_mem_zipIdx {x : Series} {p : Int × Nat} (hp : p ∈ x.zipIdx) :
    p.1 ∈ x := by
  have h' : p.1 ∈ x.zipIdx.map Prod.fst := List.mem_map_of_mem hp
  simpa [List.zipIdx_map_fst] using h'

private theorem any_map_eq (x : Series) (f : Int → Int)
    (hf : ∀ a ∈ x, ∀ b ∈ x, a < b ↔ f a < f b)
    (v : Int) (hv : v ∈ x) (j : Nat) :
    ((x.map f)[j]?).any (fun a => a < f v) = (x[j]?).any (fun a => a < v) := by
  rw [List.getElem?_map]
  cases h : x[j]? with
  | none => simp
  | some w =>
      have hw : w ∈ x := List.mem_of_getElem? h
      have hiff : f w < f v ↔ w < v := (hf w hw v hv).symm
      simp [hiff]

private def peakPred (x : Series) (p : Int × Nat) : Bool :=
  0 < p.2 && x[p.2 - 1]?.any (fun a => a < p.1) && x[p.2 + 1]?.any (fun a => a < p.1)

private theorem measure_eq_count (x : Series) :
    PeakCount.measure x = (x.zipIdx.countP (peakPred x) : Int) := by
  rfl

theorem sound_PeakCount_OrderDictionary : PeakCount.OrderDictionary.Goal := by
  intro x f hf
  rw [measure_eq_count (x.map f), measure_eq_count x]
  rw [zipIdx_map_eq, countP_map_comp]
  apply congrArg Int.ofNat
  apply countP_congr_bool
  intro p hp
  have hv : p.1 ∈ x := fst_mem_of_mem_zipIdx hp
  simp only [peakPred]
  rw [any_map_eq x f hf p.1 hv (p.2 - 1)]
  rw [any_map_eq x f hf p.1 hv (p.2 + 1)]

end TinylambOpt.ResearchProofs.Atlas.Goal_PeakCount_OrderDictionary
