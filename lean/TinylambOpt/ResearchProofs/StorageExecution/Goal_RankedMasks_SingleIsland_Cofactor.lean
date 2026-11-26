import TinylambOpt.ResearchGoals.StorageExecution.RankedMasks

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_RankedMasks_SingleIsland_Cofactor

open TinylambOpt.ResearchGoals.StorageExecution.RankedMasks
open TinylambOpt.ResearchGoals.StorageExecution.RankedMasks.SingleIsland

private abbrev hiCount (m : Machine) (n q : Nat) : Nat :=
  ((m.step q true).map (count m n)).getD 0

private theorem count_succ (m : Machine) (n q : Nat) :
    count m (n + 1) q = lowCount m n q + hiCount m n q := by
  simp [count, lowCount, hiCount]

-- Consuming one symbol delegates to the successor state (if the step is legal).
private theorem term_cons (m : Machine) (q b' : Nat) (b : Bool) (pre : List Bool) :
    ((run m q (b :: pre)).map (count m b')).getD 0 =
      ((m.step q b).map (fun q' => ((run m q' pre).map (count m b')).getD 0)).getD 0 := by
  simp [run]
  cases m.step q b <;> simp

private theorem sum_map_zero {β : Type} (xs : List β) : (xs.map (fun _ => (0 : Nat))).sum = 0 := by
  induction xs with
  | nil => simp
  | cons x xs ih => simp [ih]

-- Pushing a sum through an optional state: zero when the step is illegal.
private theorem sum_option_map {β : Type} (o : Option Nat) (xs : List β) (f : Nat → β → Nat) :
    (xs.map (fun y => ((o.map (fun x => f x y)).getD 0))).sum =
      ((o.map (fun x => (xs.map (f x)).sum)).getD 0) := by
  cases o <;> simp [sum_map_zero]

-- The cofactor identity holds for every machine: splitting a word into a
-- prefix and a suffix, then summing the suffix counts over all prefixes,
-- reconstructs the count of the whole word. This is just the dynamic
-- programming recurrence of `count` composed with itself.
private theorem cofactor_generic (m : Machine) : cofactor m := by
  intro q a
  induction a generalizing q with
  | zero =>
      intro b
      simp [words, run]
  | succ n ih =>
      intro b
      rw [show (n + 1) + b = (n + b) + 1 by omega]
      rw [count_succ]
      rw [words]
      simp only [List.map_append, List.sum_append, List.map_map, Function.comp_def]
      simp only [term_cons]
      simp only [sum_option_map, lowCount, hiCount]
      cases hf : m.step q false with
      | none =>
          cases ht : m.step q true with
          | none => simp [hf, ht]
          | some qt => simpa [hf, ht] using ih qt b
      | some qf =>
          cases ht : m.step q true with
          | none => simpa [hf, ht] using ih qf b
          | some qt => simp [hf, ht, ih]

theorem sound_Cofactor : SingleIsland.Cofactor.Goal := by
  exact cofactor_generic model

end TinylambOpt.ResearchProofs.StorageExecution.Goal_RankedMasks_SingleIsland_Cofactor
