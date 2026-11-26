import TinylambOpt.ResearchGoals.RestartRaces.JobJ3

/-!
These models specify the ordering of SQL transaction, recovery, and concurrent
maintenance events.

Proved

- `DeltaRefresh` — Refresh using three sparse change streams inside one publication
  fence. Later streams read the advanced earlier axes, including mixed insert/delete
  terms. Use separately for COUNT, non-NULL years and exact year sum.
-/
set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
namespace TinylambOpt.ResearchProofs.RestartRaces.JobJ3_DeltaRefresh
open TinylambOpt.ResearchGoals.RestartRaces.JobJ3.DeltaRefresh

theorem sum_range_add (m : Nat) (f g : Nat → Int) :
    ((List.range m).map (fun j => f j + g j)).sum =
      ((List.range m).map f).sum + ((List.range m).map g).sum := by
  induction m with
  | zero => simp
  | succ m ih =>
      simp [List.range_succ, ih]
      grind

theorem total_add (n m : Nat) (f g : Nat → Nat → Int) :
    total n m (fun i j => f i j + g i j) = total n m f + total n m g := by
  induction n with
  | zero => simp [total]
  | succ n ih =>
      simp [total, List.range_succ]
      have ih' :
          (List.flatMap (fun i => List.map (fun j => f i j + g i j) (List.range m)) (List.range n)).sum =
            (List.flatMap (fun i => List.map (f i) (List.range m)) (List.range n)).sum +
            (List.flatMap (fun i => List.map (g i) (List.range m)) (List.range n)).sum := by
        simpa [total] using ih
      rw [ih']
      rw [sum_range_add]
      grind

theorem sound : Goal := by
  intro n m w p t dw dp dt
  have hfun :
      (fun i j => cell (fun i j => w i j + dw i j) (fun i => p i + dp i)
        (fun j => t j + dt j) i j) =
      (fun i j => cell w p t i j + refresh dw dp dt p t w i j) := by
    funext i j
    simp [cell, refresh]
    grind
  change total n m (cell w p t) + total n m (refresh dw dp dt p t w) =
    total n m (fun i j => cell (fun i j => w i j + dw i j) (fun i => p i + dp i)
      (fun j => t j + dt j) i j)
  rw [hfun, total_add]
