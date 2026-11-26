import TinylambOpt.ResearchGoals.Atlas.PlanSearch

/-!
The SQL optimizer can prune or reorganize its plan search only under the relevant
ordering and cost assumptions.

Proved

- `AlphabeticMerge.ScaleReuse` — `∀ ws c, 0 < c → cut (ws.map (c*·)) = cut ws`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.PlanSearch_ScaleReuse

open TinylambOpt.ResearchGoals.Atlas.PlanSearch
open TinylambOpt.ResearchGoals.Atlas.PlanSearch.AlphabeticMerge

private theorem sum_map_mul (ws : List Nat) (c : Nat) :
    (ws.map (fun x => c * x)).sum = c * ws.sum := by
  induction ws with
  | nil => simp
  | cons x xs ih => simp [ih, Nat.mul_add]

private theorem min_mul (c a b : Nat) : min (c * a) (c * b) = c * min a b := by
  by_cases h : a ≤ b
  · rw [Nat.min_eq_left h, Nat.min_eq_left (Nat.mul_le_mul_left c h)]
  · have h' : b ≤ a := by omega
    rw [Nat.min_eq_right h', Nat.min_eq_right (Nat.mul_le_mul_left c h')]

private theorem min?_map_mul (xs : List Nat) (c : Nat) :
    (xs.map (fun x => c * x)).min? = (xs.min?).map (fun x => c * x) := by
  induction xs with
  | nil => simp
  | cons a as ih =>
      simp only [List.map_cons, List.min?_cons]
      rw [ih]
      cases as.min? with
      | none => simp
      | some b => simp

private theorem map_min?_getD_mul (xs : List Nat) (g : Nat → Nat) (c : Nat) :
    (xs.map (fun i => c * g i)).min?.getD 0 = c * (xs.map g).min?.getD 0 := by
  have hmap : xs.map (fun i => c * g i) = (xs.map g).map (fun x => c * x) := by
    rw [List.map_map]
    rfl
  rw [hmap, min?_map_mul]
  have h := Option.getD_map (fun x : Nat => c * x) 0 ((xs.map g).min?)
  rw [Nat.mul_zero] at h
  exact h

private theorem dp_scale (fuel : Nat) (ws : List Nat) (c : Nat) :
    dp fuel (ws.map (fun x => c * x)) = c * dp fuel ws := by
  induction fuel generalizing ws with
  | zero => simp [dp]
  | succ fuel ih =>
      simp only [dp]
      have hlen : (ws.map (fun x => c * x)).length = ws.length := by simp
      rw [hlen]
      by_cases h : ws.length ≤ 1
      · simp [h]
      · simp [h]
        rw [sum_map_mul]
        simp only [← List.map_take, ← List.map_drop, ih]
        simp only [← Nat.mul_add]
        rw [map_min?_getD_mul]
        rw [Nat.mul_add]

private theorem best_scale (ws : List Nat) (c : Nat) :
    best (ws.map (fun x => c * x)) = c * best ws := by
  unfold best
  rw [dp_scale]
  simp

theorem sound_ScaleReuse : AlphabeticMerge.ScaleReuse.Goal := by
  intro ws c hc
  unfold cut
  have hlen : (ws.map (fun x => c * x)).length = ws.length := by simp
  rw [hlen]
  have hcomp :
      (fun i j => decide (best ((ws.map (fun x => c * x)).take i) + best ((ws.map (fun x => c * x)).drop i) ≤
                 best ((ws.map (fun x => c * x)).take j) + best ((ws.map (fun x => c * x)).drop j))) =
      (fun i j => decide (best (ws.take i) + best (ws.drop i) ≤
                 best (ws.take j) + best (ws.drop j))) := by
    funext i j
    simp only [← List.map_take, ← List.map_drop, best_scale]
    simp only [← Nat.mul_add]
    simp [Nat.mul_le_mul_left_iff hc]
  rw [hcomp]

end TinylambOpt.ResearchProofs.Atlas.PlanSearch_ScaleReuse
