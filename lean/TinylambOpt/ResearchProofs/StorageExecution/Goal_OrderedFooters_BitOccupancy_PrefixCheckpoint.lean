import TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_OrderedFooters_BitOccupancy_PrefixCheckpoint

open TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters
open TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters.BitOccupancy

/-! Small-input truth check (run during development):
private def sampleGen : Nat → List (List Nat)
  | 0 => [[]]
  | n + 1 => let p := sampleGen n; p ++ p.flatMap (fun xs => [0 :: xs, 1 :: xs, 2 :: xs])
private def sampleProbe (n : Nat) : Bool :=
  let ls := sampleGen n
  ls.all (fun init => ls.all (fun a => ls.all (fun b =>
    ([[a], [a, b], [b, a]].all (fun ps =>
      (ps.map agg.reference).scanl agg.join (agg.reference init) ==
        (List.range (ps.length + 1)).map (fun i => agg.reference (init ++ (ps.take i).flatten)))))))
#eval sampleProbe 2   -- true
#eval sampleProbe 3   -- true
-/

private def bitf (a x : Nat) : Nat := a ||| 2 ^ (x % 64)

private theorem foldl_or_bits (c : Nat) (xs : List Nat) :
    xs.foldl bitf c = c ||| xs.foldl bitf 0 := by
  induction xs generalizing c with
  | nil => simp
  | cons x xs ih =>
      simp only [List.foldl_cons, bitf]
      rw [ih (c ||| 2 ^ (x % 64))]
      simp
      rw [ih (2 ^ (x % 64))]
      rw [Nat.or_assoc]

private theorem stitch_agg : stitch agg := by
  intro a b
  change (a.length + b.length, a.foldl bitf 0 ||| b.foldl bitf 0) =
    ((a ++ b).length, (a ++ b).foldl bitf 0)
  rw [List.length_append]
  congr 1
  rw [List.foldl_append]
  exact (foldl_or_bits (a.foldl bitf 0) b).symm

section Generic

variable {σ : Type} (f : Fold σ)

private theorem prefixCheckpoint_of_stitch (h : stitch f) : prefixCheckpoint f := by
  intro init ps
  have hlen : (ps.map f.reference).length = ps.length := by simp
  apply List.ext_getElem
  · simp [List.length_scanl, hlen]
  · intro i hi1 hi2
    simp only [List.length_scanl, List.length_map, hlen, List.length_range] at hi1 hi2
    simp only [List.getElem_scanl, List.getElem_map, List.getElem_range]
    have gen : ∀ (l : List (List Nat)) (acc : List Nat),
        List.foldl f.join (f.reference acc) (l.map f.reference) = f.reference (acc ++ l.flatten) := by
      intro l
      induction l with
      | nil => intro acc; simp
      | cons p ps ih =>
          intro acc
          simp only [List.map_cons, List.foldl, List.flatten_cons]
          rw [h acc p, ih (acc ++ p)]
          simp [List.append_assoc]
    have htake : (List.take i (ps.map f.reference)) = (ps.take i).map f.reference := by
      simp [List.map_take]
    rw [htake, gen]

end Generic

theorem sound_PrefixCheckpoint : BitOccupancy.PrefixCheckpoint.Goal :=
  prefixCheckpoint_of_stitch _ stitch_agg

end TinylambOpt.ResearchProofs.StorageExecution.Goal_OrderedFooters_BitOccupancy_PrefixCheckpoint
