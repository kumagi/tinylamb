import TinylambOpt.ResearchProofs.QueryRewrites.NullableSum
import TinylambOpt.ResearchGoals.QueryRewrites.KeyRoutedJoinStates

namespace TinylambOpt.ResearchProofs.QueryRewrites.KeyRoutedJoinStates.Goal_SkewSplitWeightedJoinPartial

open TinylambOpt.ResearchGoals.QueryRewrites.KeyRoutedJoinStates


/-! Splitting the build side of one skewed join key into shards is safe when the
    partial nullable SUM states are merged: the projected product rows are only
    permuted, and nullable SUM is invariant under permutation. This lets the
    executor keep independent per-shard states without changing the result. -/

open TinylambOpt.ResearchGoals.QueryRewrites
open TinylambOpt.ResearchProofs.QueryRewrites.NullableSum

/-- The product pairs of a probe list with a build list. -/
def joinProduct (probe ys : List (Option Int)) :=
  probe.flatMap fun x => ys.map fun y => (x, y)

/-- Appending two build shards only permutes the probe/build pair stream. -/
theorem joinProduct_append_perm (probe s t : List (Option Int)) :
    (joinProduct probe (s ++ t)).Perm (joinProduct probe s ++ joinProduct probe t) := by
  induction probe with
  | nil => simp [joinProduct]
  | cons x xs ih =>
      unfold joinProduct
      simp only [List.flatMap_cons, List.map_append]
      let A := List.map (fun y => (x, y)) s
      let B := List.map (fun y => (x, y)) t
      let Cs := List.flatMap (fun z => List.map (fun y => (z, y)) s) xs
      let Ct := List.flatMap (fun z => List.map (fun y => (z, y)) t) xs
      let C := List.flatMap (fun z => List.map (fun y => (z, y)) s ++ List.map (fun y => (z, y)) t) xs
      have hC : C.Perm (Cs ++ Ct) := by
        dsimp [C, Cs, Ct]
        simpa [joinProduct, List.map_append] using ih
      have h1 : ((A ++ B) ++ C).Perm ((A ++ B) ++ (Cs ++ Ct)) := hC.append_left (A ++ B)
      have h2a : ((A ++ B) ++ (Cs ++ Ct)) = (A ++ ((B ++ Cs) ++ Ct)) := by simp [List.append_assoc]
      have h2b : (A ++ ((B ++ Cs) ++ Ct)).Perm (A ++ ((Cs ++ B) ++ Ct)) :=
        (List.perm_append_comm.append_right Ct).append_left A
      have h2c : (A ++ ((Cs ++ B) ++ Ct)) = ((A ++ Cs) ++ (B ++ Ct)) := by simp [List.append_assoc]
      have h2 : ((A ++ B) ++ (Cs ++ Ct)).Perm ((A ++ Cs) ++ (B ++ Ct)) := by
        rw [h2a]
        rw [← h2c]
        exact h2b
      exact h1.trans h2

/-- Projecting the pair stream preserves the shard-append permutation. -/
theorem projectedGrid_perm (probe : List (Option Int))
    (col : Option Int × Option Int → Option Int) (s t : List (Option Int)) :
    ((joinProduct probe (s ++ t)).map col).Perm
      (((joinProduct probe s).map col) ++ ((joinProduct probe t).map col)) := by
  have h := (joinProduct_append_perm probe s t).map col
  simpa [List.map_append] using h

/-- Flattening the projected shard streams is a permutation of the projected
    stream of the flattened shards. -/
theorem flatten_project_perm (probe : List (Option Int))
    (shards : List (List (Option Int))) (col : Option Int × Option Int → Option Int) :
    ((shards.map (fun s => ((joinProduct probe s).map col))).flatten).Perm
      (((joinProduct probe shards.flatten).map col)) := by
  induction shards with
  | nil => simp [joinProduct]
  | cons s ss ih =>
      change (((joinProduct probe s).map col) ++
        ((ss.map (fun t => ((joinProduct probe t).map col))).flatten)).Perm
        (((joinProduct probe (s ++ ss.flatten)).map col))
      have h1 := projectedGrid_perm probe col s ss.flatten
      have h2 := ih.symm.append_left ((joinProduct probe s).map col)
      exact (h1.trans h2).symm

theorem sound : TinylambOpt.ResearchGoals.QueryRewrites.KeyRoutedJoinStates.SkewSplitWeightedJoinPartial.Goal := by
  intro probe shards
  dsimp only
  intro col hcol
  have hmap := nsum_map_nsum (shards.map (fun s => ((joinProduct probe s).map col)))
  rw [List.map_map] at hmap
  simpa [joinProduct, Function.comp_def] using
    hmap.trans (nsum_perm (flatten_project_perm probe shards col))
end TinylambOpt.ResearchProofs.QueryRewrites.KeyRoutedJoinStates.Goal_SkewSplitWeightedJoinPartial
