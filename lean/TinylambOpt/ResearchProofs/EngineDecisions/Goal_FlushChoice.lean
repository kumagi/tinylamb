import TinylambOpt.ResearchGoals.EngineDecisions.Executor

/-!
These models compare concrete implementation choices in planning, execution,
concurrency, or recovery against their stated observable contracts.

Proved

- `FlushChoice` — An admission controller can price the cheapest subset with a DP keyed
  by partition ordinal and remaining grant deficit. Covers infeasibility, and asks
  for a realizable cost as well as a lower bound.
-/
set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
namespace TinylambOpt.ResearchProofs.EngineDecisions.Ex_FlushChoice

open TinylambOpt.ResearchGoals.EngineDecisions.Executor.FlushChoice

theorem bestOption_assoc (a b c : Option Nat) :
    bestOption (bestOption a b) c = bestOption a (bestOption b c) := by
  cases a <;> cases b <;> cases c <;> simp [bestOption, Nat.min_assoc]

theorem bestOption_map_add (io : Nat) (a b : Option Nat) :
    bestOption (a.map (io + ·)) (b.map (io + ·)) = (bestOption a b).map (io + ·) := by
  cases a <;> cases b <;> simp [bestOption, Nat.add_min_add_left]

def cmin : List Nat → Option Nat
  | [] => none
  | a :: l => bestOption (some a) (cmin l)

theorem foldl_min_min (l : List Nat) (a b : Nat) :
    l.foldl min (min a b) = min a (l.foldl min b) := by
  induction l generalizing a b with
  | nil => simp
  | cons c cs ih =>
      simp only [List.foldl_cons]
      calc
        cs.foldl min (min (min a b) c) = cs.foldl min (min a (min b c)) := by
          congr 1
          simp [Nat.min_assoc, Nat.min_left_comm, Nat.min_comm]
        _ = min a (cs.foldl min (min b c)) := ih a (min b c)
        _ = min a (cs.foldl min (min b c)) := rfl

theorem foldl_min_best (a : Nat) (l : List Nat) :
    bestOption (some a) l.min? = some (l.foldl min a) := by
  cases l with
  | nil => simp [bestOption]
  | cons b bs =>
      simp only [List.min?, List.foldl_cons]
      simp [bestOption]
      exact (foldl_min_min bs a b).symm

theorem cmin_eq_min? (l : List Nat) : cmin l = l.min? := by
  induction l with
  | nil => rfl
  | cons a as ih =>
      cases as with
      | nil => simp [cmin, bestOption]
      | cons b bs =>
          rw [cmin, ih]
          simp only [List.min?, List.foldl_cons]
          simp [bestOption]
          exact (foldl_min_min bs a b).symm

theorem cmin_append (l₁ l₂ : List Nat) : cmin (l₁ ++ l₂) = bestOption (cmin l₁) (cmin l₂) := by
  induction l₁ with
  | nil =>
      cases h : cmin l₂ <;> simp [cmin, bestOption, h]
  | cons a as ih =>
      simp [cmin, ih, bestOption_assoc]

theorem cmin_map_add (io : Nat) (l : List Nat) :
    cmin (l.map (fun x => io + x)) = (cmin l).map (fun x => io + x) := by
  induction l with
  | nil => rfl
  | cons a as ih =>
      simpa [cmin, ih] using bestOption_map_add io (some a) (cmin as)

def bytes (xs : List Bucket) : Nat := (xs.map Prod.fst).sum
def ioSum (xs : List Bucket) : Nat := (xs.map Prod.snd).sum

def costs (bs : List Bucket) (need : Nat) : List Nat :=
  ((subsets bs).filter (fun xs => need ≤ bytes xs)).map ioSum

theorem filter_map_cons (l : List (List Bucket)) (cb io need : Nat) :
    ((l.map fun xs => (cb, io) :: xs).filter fun xs => need ≤ bytes xs).map ioSum =
      (l.filter fun xs => need - cb ≤ bytes xs).map (fun xs => io + ioSum xs) := by
  induction l with
  | nil => rfl
  | cons xs rest ih =>
      simp only [List.map_cons, List.filter_cons]
      by_cases h : need - cb ≤ bytes xs
      · have h' : need ≤ bytes ((cb, io) :: xs) := by
          simp [bytes] at h ⊢
          omega
        simp [h, h', ih, ioSum]
      · have h' : ¬ need ≤ bytes ((cb, io) :: xs) := by
          simp [bytes] at h ⊢
          omega
        simp [h, h', ih]

theorem costs_cons (bs : List Bucket) (cb io need : Nat) :
    costs ((cb,io)::bs) need =
      costs bs need ++ (costs bs (need - cb)).map (fun x => io + x) := by
  unfold costs
  simp only [subsets, List.filter_append, List.map_append]
  rw [filter_map_cons]
  simp [List.map_map, Function.comp_def]

theorem flushCost_zero : ∀ bs, flushCost bs 0 = some 0
  | [] => by simp [flushCost]
  | (cb,io)::bs => by simp [flushCost]

theorem main : ∀ bs need, flushCost bs need = cmin (costs bs need)
  | [], need => by
      cases need with
      | zero => simp [flushCost, costs, subsets, cmin, bytes, ioSum, bestOption]
      | succ n => simp [flushCost, costs, subsets, cmin, bytes, ioSum, bestOption]
  | (cb,io)::bs, need => by
      rw [costs_cons, cmin_append, cmin_map_add]
      cases need with
      | zero =>
          have hz : cmin (costs bs 0) = some 0 := by
            rw [← main bs 0, flushCost_zero]
          simp [flushCost, bestOption, hz]
      | succ n =>
          rw [← main bs (n+1), ← main bs ((n+1) - cb)]
          simp [flushCost]

theorem sound : Goal := by
  intro bs need
  change flushCost bs need =
    (List.map ioSum ((subsets bs).filter (fun xs => need ≤ bytes xs))).min?
  have h := main bs need
  rw [cmin_eq_min?] at h
  simpa [costs] using h

end TinylambOpt.ResearchProofs.EngineDecisions.Ex_FlushChoice
