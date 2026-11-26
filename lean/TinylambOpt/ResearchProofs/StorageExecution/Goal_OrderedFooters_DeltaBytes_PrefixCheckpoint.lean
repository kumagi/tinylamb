import TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters

/-!
A footer can guide sorted scans and key-range probes when its indexed summary is
consistent with the stored blocks.

Proved

- `DeltaBytes.PrefixCheckpoint` — A saved prefix plus page states yields a scan
  checkpoint at each boundary.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_OrderedFooters_DeltaBytes_PrefixCheckpoint

open TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters
open TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters.DeltaBytes
open TinylambOpt.ResearchGoals.Compression.Wire

private theorem edges_append_cons (xs : List Nat) (y : Nat) (ys : List Nat) :
    ∀ x : Nat, edges ((x :: xs) ++ (y :: ys)) =
      edges (x :: xs) ++ (((x :: xs).getLast?.getD x, y) :: edges (y :: ys)) := by
  induction xs with
  | nil =>
      intro x
      simp [edges, List.getLast?_cons]
  | cons z zs ih =>
      intro x
      rw [show edges ((x :: z :: zs) ++ (y :: ys)) = (x, z) :: edges ((z :: zs) ++ (y :: ys)) by
        simp [edges]]
      rw [show edges (x :: z :: zs) = (x, z) :: edges (z :: zs) by simp [edges]]
      rw [ih z]
      simp [List.getLast?_cons]

private theorem stitchAgg : stitch agg := by
  intro a b
  cases a with
  | nil =>
      simp [agg, optionalJoin]
  | cons x xs =>
      cases b with
      | nil =>
          simp [agg, optionalJoin]
      | cons y ys =>
          simp only [agg, List.head?_cons, Option.map_some, optionalJoin]
          rw [edges_append_cons xs y ys x]
          simp [List.getLast?_append, List.getLast?_cons,
            List.map_append, List.sum_append, List.map_cons, List.sum_cons]
          omega

theorem sound_PrefixCheckpoint : DeltaBytes.PrefixCheckpoint.Goal := by
  intro init ps
  induction ps generalizing init with
  | nil => simp
  | cons p ps ih =>
      simp only [List.map_cons, List.scanl_cons]
      rw [stitchAgg init p]
      rw [ih (init ++ p)]
      simp [List.range_succ_eq_map, Function.comp_def, List.append_assoc]

end TinylambOpt.ResearchProofs.StorageExecution.Goal_OrderedFooters_DeltaBytes_PrefixCheckpoint
