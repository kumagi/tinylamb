import TinylambOpt.ResearchGoals.Atlas.EncodedPages

/-!
SQL scans and aggregates can use encoded page structure when positions and arithmetic
summaries agree with expanded rows.

Proved

- `SaturatingCounter.PlateauFrequency` — `∀ c, c.step ≠ 0 → occurrence layout c
  (c.base+c.step*c.split) = c.n-c.split`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_SaturatingCounter_PlateauFrequency

open TinylambOpt.ResearchGoals.Atlas.EncodedPages
open TinylambOpt.ResearchGoals.Atlas.EncodedPages.SaturatingCounter

private theorem filterMap_some_eq_map {α β : Type} (l : List α) (f : α → β) :
    l.filterMap (fun x => some (f x)) = l.map f := by
  induction l with
  | nil => rfl
  | cons x xs ih => simp [ih]

private theorem range_filterMap_all {α : Type} (n : Nat) (f : Nat → Option α) (g : Nat → α)
    (h : ∀ i, i < n → f i = some (g i)) :
    (List.range n).filterMap f = (List.range n).map g := by
  rw [← filterMap_some_eq_map (List.range n) g]
  induction n with
  | zero => simp
  | succ n ih =>
      rw [List.range_succ]
      simp only [List.filterMap_append]
      rw [ih (fun i hi => h i (by omega))]
      simp [h n (by omega)]

private theorem rows_map (c : Config) :
    rows layout c = (List.range c.n).map (fun i =>
      (i, if i < c.split then c.base + c.step * (i : Int)
        else c.base + c.step * (c.split : Int))) := by
  unfold rows
  apply range_filterMap_all
  intro i hi
  by_cases hlt : i < c.split <;> simp [layout, hlt]

private theorem plateau_count (n split : Nat) (base step : Int) (hstep : step ≠ 0) :
    ((List.range n).map (fun i =>
      (i, if i < split then base + step * (i : Int) else base + step * (split : Int)))).countP
        (fun r => r.2 == (base + step * (split : Int))) = n - split := by
  induction n with
  | zero => simp
  | succ n ih =>
      rw [List.range_succ, List.map_append, List.countP_append, ih]
      simp only [List.map_singleton, List.countP_cons, List.countP_nil]
      by_cases hlt : n < split
      · have hne : base + step * (n : Int) ≠ base + step * (split : Int) := by
          intro heq
          have hm : step * (n : Int) = step * (split : Int) := Int.add_left_cancel heq
          have hcast : (n : Int) = (split : Int) := Int.eq_of_mul_eq_mul_left hstep hm
          omega
        simp [hlt, hne]
        omega
      · simp [hlt]
        omega

theorem sound_SaturatingCounter_PlateauFrequency : SaturatingCounter.PlateauFrequency.Goal := by
  intro c hstep
  unfold occurrence
  rw [rows_map]
  exact plateau_count c.n c.split c.base c.step hstep

end TinylambOpt.ResearchProofs.Atlas.Goal_SaturatingCounter_PlateauFrequency
