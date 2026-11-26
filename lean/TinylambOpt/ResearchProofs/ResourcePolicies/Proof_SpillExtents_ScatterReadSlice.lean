import TinylambOpt.ResearchGoals.ResourcePolicies.SpillExtents

namespace TinylambOpt.ResearchProofs.ResourcePolicies.SpillExtents.Goal_ScatterReadSlice

open TinylambOpt.ResearchGoals.ResourcePolicies.SpillExtents


/-! Scratch proof: a scatter read selects exactly the byte slice of the
    flattened extents. Prefix bases from scanl give each chunk's global start,
    and the auxiliary theorem splices the per-chunk slices across the boundary. -/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

open TinylambOpt.ResearchGoals.ResourcePolicies.SpillExtents

private theorem take_append_prefix (xs ys : List Nat) (c : Nat) :
    (xs.take c) ++ (ys.take (c - xs.length)) = (xs ++ ys).take c := by
  induction xs generalizing c with
  | nil => simp
  | cons x xs ih =>
      cases c with
      | zero => simp
      | succ c =>
          simp [ih]

private theorem drop_take_append (xs ys : List Nat) (s c : Nat) :
    (xs.drop s).take c ++ (ys.drop (s - xs.length)).take (c - (xs.length - s)) =
      ((xs ++ ys).drop s).take c := by
  induction xs generalizing s c with
  | nil => simp
  | cons x xs ih =>
      cases s with
      | zero =>
          simpa using take_append_prefix (x :: xs) ys c
      | succ s =>
          simpa [Nat.succ_sub_succ_eq_sub] using ih s c

private theorem scatter_slice_aux (chunks : List (List Nat)) (base start len : Nat) :
    let bases := (chunks.map List.length).scanl (·+·) base
    ((bases.zip chunks).flatMap (fun (p, bs) =>
      (bs.drop (start - p)).take (start + len - max p start))) =
      (chunks.flatten.drop (start - base)).take (start + len - max base start) := by
  induction chunks generalizing base with
  | nil => simp
  | cons chunk rest ih =>
      simp only [List.map_cons, List.scanl_cons, List.zip_cons_cons, List.flatMap_cons,
        List.flatten_cons]
      rw [ih (base + chunk.length)]
      have hs : start - (base + chunk.length) = (start - base) - chunk.length := by omega
      have hc : start + len - max (base + chunk.length) start =
          (start + len - max base start) - (chunk.length - (start - base)) := by omega
      rw [hs, hc]
      exact drop_take_append chunk rest.flatten (start - base) (start + len - max base start)

theorem sound : TinylambOpt.ResearchGoals.ResourcePolicies.SpillExtents.ScatterReadSlice.Goal := by
  intro chunks start len
  simpa using scatter_slice_aux chunks 0 start len
end TinylambOpt.ResearchProofs.ResourcePolicies.SpillExtents.Goal_ScatterReadSlice
