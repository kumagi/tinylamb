import TinylambOpt.ResearchGoals.LayerContracts.RecursivePaths

/-!
A recursive query uses visited states and frontier updates to avoid redundant work while
retaining visits that affect path acceptance.

Proved

- `PatternAB.Witness` — `∀ ls, (advance model 0 ls).any model.accept = ((ls.zip
  (ls.drop 1)).contains (1,2))`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.LayerContracts.RecursivePaths.Goal_PatternAB_Witness

open TinylambOpt.ResearchGoals.LayerContracts.RecursivePaths

set_option maxHeartbeats 0
open TinylambOpt.ResearchGoals.LayerContracts.RecursivePaths

def paState (q : Nat) : List Nat → Nat
  | [] => q
  | x :: xs => paState (if q == 2 || (q == 1 && x == 2) then 2 else if x == 1 then 1 else 0) xs

theorem paFold : ∀ (q : Nat) (ls : List Nat), q < 3 →
    List.foldlM (fun q x => some (if q == 2 || (q == 1 && x == 2) then 2 else if x == 1 then 1 else 0)) q ls = some (paState q ls) := by
  intro q ls hq
  induction ls generalizing q with
  | nil => simp [paState]
  | cons x xs ih =>
      rw [List.foldlM_cons]
      change List.foldlM (fun q x => some (if q == 2 || (q == 1 && x == 2) then 2 else if x == 1 then 1 else 0))
          (if q == 2 || (q == 1 && x == 2) then 2 else if x == 1 then 1 else 0) xs = some (paState q (x :: xs))
      apply ih
      by_cases h1 : x == 1 <;> by_cases h2 : x == 2 <;> by_cases hq1 : q == 1 <;> by_cases hq2 : q == 2 <;> simp [h1, h2, hq1, hq2] <;> omega

def pat (ls : List Nat) : Bool := (ls.zip (ls.drop 1)).contains (1,2)

theorem pat_cons : ∀ (x : Nat) (xs : List Nat),
    pat (x :: xs) = ((x == 1 && (match xs with | [] => false | y :: _ => y == 2)) || pat xs) := by
  intro x xs
  cases xs with
  | nil => simp [pat]
  | cons y ys =>
      have hbeq1 : (decide (1 = x)) = (x == 1) := by by_cases h : 1 = x <;> simp [h] <;> omega
      have hbeq2 : (decide (2 = y)) = (y == 2) := by by_cases h : 2 = y <;> simp [h] <;> omega
      by_cases hx : x = 1 <;> by_cases hy : y = 2 <;>
      simp [pat, hx, hy, hbeq1, hbeq2]

theorem paStateSpec : ∀ (q : Nat), q < 3 → ∀ (ls : List Nat),
    (paState q ls == 2) = (q == 2 || match ls with | [] => false | x :: _ => (q == 1 && x == 2) || pat ls) := by
  intro q hq ls
  induction ls generalizing q with
  | nil =>
      by_cases hq2 : q == 2 <;> simp [paState, pat, hq2]
  | cons x xs ih =>
      let s : Nat := if q == 2 || (q == 1 && x == 2) then 2 else if x == 1 then 1 else 0
      have hs : s < 3 := by
        dsimp [s]
        by_cases hq2' : q == 2 <;> by_cases hq1' : q == 1 <;> by_cases hx2' : x == 2 <;> by_cases hx1' : x == 1 <;> simp [hq2', hq1', hx2', hx1'] <;> omega
      have hchange : (paState q (x :: xs) == 2) = (paState s xs == 2) := by rfl
      rw [hchange]
      rw [ih s hs]
      rw [pat_cons]
      dsimp [s]
      cases xs with
      | nil =>
          by_cases hq2 : q == 2 <;> by_cases hq1 : q == 1 <;> by_cases hx2 : x == 2 <;> by_cases hx1 : x == 1 <;>
          simp [hq2, hq1, hx2, hx1, pat, Bool.or_assoc, Bool.or_left_comm, Bool.or_comm]
      | cons y ys =>
          by_cases hq2 : q == 2 <;> by_cases hq1 : q == 1 <;> by_cases hx2 : x == 2 <;> by_cases hx1 : x == 1 <;>
          simp [hq2, hq1, hx2, hx1, Bool.or_assoc, Bool.or_left_comm, Bool.or_comm]

theorem sound : PatternAB.Witness.Goal := by
  intro ls
  change (List.foldlM (fun q x => some (if q == 2 || (q == 1 && x == 2) then 2 else if x == 1 then 1 else 0)) 0 ls).any PatternAB.model.accept = pat ls
  rw [paFold 0 ls (by omega)]
  simp [PatternAB.model]
  have hh := paStateSpec 0 (by omega) ls
  cases ls with
  | nil => simp [paState, pat] at hh ⊢
  | cons x xs =>
      simpa [pat] using hh

end TinylambOpt.ResearchProofs.LayerContracts.RecursivePaths.Goal_PatternAB_Witness
