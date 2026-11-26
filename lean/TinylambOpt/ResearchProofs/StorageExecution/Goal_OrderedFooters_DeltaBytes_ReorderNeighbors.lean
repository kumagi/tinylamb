import TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters

/-!
A footer can guide sorted scans and key-range probes when its indexed summary is
consistent with the stored blocks.

Proved

- `DeltaBytes.ReorderNeighbors` — Two neighboring states may be scheduled in reverse if
  they commute on their reachable values.
- `DeltaBytes.Stitch` — Compute a page boundary correction using only the two
  serialized states.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_OrderedFooters_DeltaBytes_ReorderNeighbors

open TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters
open TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters.DeltaBytes

private def gen : Nat → List (List Nat)
  | 0 => [[]]
  | n + 1 => let p := gen n; p ++ p.flatMap (fun xs => [0 :: xs, 1 :: xs, 2 :: xs])

/-- Exhaustive small-input check of the goal (all quadruples over 16 lists). -/
private def probe (n : Nat) : Bool :=
  let ls := gen n
  ls.all (fun pre => ls.all (fun a => ls.all (fun b => ls.all (fun suf =>
    let sa := agg.join (agg.reference a) (agg.reference b)
    let sb := agg.join (agg.reference b) (agg.reference a)
    if sa == sb then agg.reference (pre ++ a ++ b ++ suf) == agg.reference (pre ++ b ++ a ++ suf) else true))))

#eval probe 2

private theorem getLast_cons_eq_getD (x : Nat) (xs : List Nat) :
    (x :: xs).getLast (by simp) = xs.getLast?.getD x := by
  have h := List.getLast?_eq_some_getLast (l := x :: xs) (by simp)
  rw [List.getLast?_cons] at h
  simpa using h.symm

private theorem getLast?_append_right (a b : List Nat) (hb : b ≠ []) :
    (a ++ b).getLast? = b.getLast? := by
  rw [List.getLast?_append]
  cases b with
  | nil => contradiction
  | cons y ys => simp [List.getLast?_cons]

private theorem edges_append (a b : List Nat) (ha : a ≠ []) (hb : b ≠ []) :
    edges (a ++ b) = edges a ++ ((a.getLast ha, b.head hb) :: edges b) := by
  induction a with
  | nil => contradiction
  | cons x xs ih =>
      cases xs with
      | nil =>
          cases b with
          | nil => contradiction
          | cons y ys => simp [edges]
      | cons y ys =>
          have ha' : y :: ys ≠ [] := by simp
          have hih := ih ha'
          cases b with
          | nil => contradiction
          | cons z zs =>
              simp [edges] at hih ⊢
              simpa [List.getLast_cons] using hih

/-- The DeltaBytes summary is a monoid homomorphism from list append. -/
private theorem stitch_agg : DeltaBytes.Stitch.Goal := by
  intro a b
  cases a with
  | nil => simp [agg, optionalJoin]
  | cons x xs =>
      cases b with
      | nil => simp [agg, optionalJoin]
      | cons y ys =>
          simp only [agg, List.head?_cons, Option.map_some, optionalJoin]
          have hlast : (x :: xs ++ y :: ys).getLast? = (y :: ys).getLast? :=
            getLast?_append_right (x :: xs) (y :: ys) (by simp)
          have hlastB : (y :: ys).getLast? = some ((y :: ys).getLast (by simp)) :=
            List.getLast?_eq_some_getLast (by simp)
          have hedges := edges_append (x :: xs) (y :: ys) (by simp) (by simp)
          simp only [List.head_cons, getLast_cons_eq_getD] at hedges
          simp only [hlast, hlastB, List.getLast?_cons, Option.getD_some]
          simp only [hedges, List.map_append, List.sum_append]
          simp
          omega

theorem sound_ReorderNeighbors : DeltaBytes.ReorderNeighbors.Goal := by
  intro pre a b suf hcomm
  have step : ∀ x y : List Nat, agg.reference (pre ++ x ++ y ++ suf) =
      agg.join (agg.join (agg.reference pre) (agg.join (agg.reference x) (agg.reference y))) (agg.reference suf) := by
    intro x y
    rw [show pre ++ x ++ y ++ suf = (pre ++ (x ++ y)) ++ suf by simp [List.append_assoc]]
    rw [← stitch_agg, ← stitch_agg, ← stitch_agg]
  rw [step a b, step b a, hcomm]

end TinylambOpt.ResearchProofs.StorageExecution.Goal_OrderedFooters_DeltaBytes_ReorderNeighbors
