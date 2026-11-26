import TinylambOpt.ResearchGoals.JitKernels.ExcursionGuards

/-!
Scanning a chunk backwards visits exactly the complements of the forward
prefixes: the reversed scan-lattice is the forward one reflected about
`xs.sum`, i.e. `path xs.reverse = ((path xs).map (xs.sum - ·)).reverse`.
Hence `low xs.reverse = xs.sum - high xs` and dually. SQL execution can
therefore specialize reverse-order scans from the forward extrema and total
without a second pass over the data.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false
set_option linter.unusedSimpArgs false
set_option linter.deprecated false

namespace TinylambOpt.ResearchProofs.JitKernels.ExcursionGuards_ReverseChunkRange
open TinylambOpt.ResearchGoals.JitKernels.ExcursionGuards

private theorem some_min (xs : List Int) : ∃ m, (path xs).min? = some m := by
  cases h : (path xs).min? with
  | some m => exact ⟨m, rfl⟩
  | none => exact absurd (List.min?_eq_none_iff.mp h) (by simp [path])

private theorem some_max (xs : List Int) : ∃ m, (path xs).max? = some m := by
  cases h : (path xs).max? with
  | some m => exact ⟨m, rfl⟩
  | none => exact absurd (List.max?_eq_none_iff.mp h) (by simp [path])

private theorem foldr_flip_sum :
    ∀ (t : List Int) (c : Int), List.foldr (flip (·+·)) c t = c + t.sum := by
  intro t
  induction t with
  | nil => intro c; simp
  | cons a t ih =>
    intro c
    rw [List.foldr_cons, ih, List.sum_cons]
    simp only [flip]
    omega

private theorem path_shift (u : Int) :
    ∀ (t : List Int), t.scanl (·+·) u = (path t).map (u + ·) := by
  intro t
  induction t generalizing u with
  | nil => simp [path]
  | cons c s ih =>
    simp only [List.scanl_cons, path, Int.zero_add, Int.add_zero, List.map_cons]
    rw [ih (u + c), ih c, List.map_map, List.cons.injEq]
    refine ⟨rfl, congrArg (fun h => List.map h (path s))
      (funext (fun x => by simp only [Function.comp_apply]; omega))⟩

private theorem scanr_map_sum :
    ∀ (xs : List Int) (c : Int),
      List.scanr (flip (·+·)) c xs = (path xs).map (c + xs.sum - ·) := by
  intro xs
  induction xs with
  | nil => intro c; simp [path]
  | cons a t ih =>
    intro c
    have hunfold : path (a :: t) = (0 :: t.scanl (·+·) a) := by
      show (a :: t).scanl (·+·) 0 = _
      rw [List.scanl_cons, Int.zero_add]
    rw [List.scanr_cons, foldr_flip_sum, List.sum_cons, ih,
      hunfold, List.map_cons, Int.sub_zero, List.cons.injEq]
    refine ⟨by omega, ?_⟩
    rw [path_shift a, List.map_map]
    refine congrArg (fun h => List.map h (path t))
      (funext (fun x => by simp only [Function.comp_apply]; omega))

private theorem path_reverse_map (xs : List Int) :
    path xs.reverse = ((path xs).map (xs.sum - ·)).reverse := by
  show (xs.reverse).scanl (·+·) 0 = _
  rw [List.scanl_reverse, scanr_map_sum]
  simp only [Int.zero_add]

theorem sound : ReverseChunkRange.Goal := by
  intro xs
  obtain ⟨M, hM⟩ := some_max xs
  obtain ⟨m, hm⟩ := some_min xs
  obtain ⟨hMem, hBnd⟩ := List.max?_eq_some_iff.mp hM
  obtain ⟨hmem, hbnd⟩ := List.min?_eq_some_iff.mp hm
  have hhigh : high xs = M := by rw [high, hM, Option.getD_some]
  have hlow : low xs = m := by rw [low, hm, Option.getD_some]
  constructor
  · have hsome : (path xs.reverse).min? = some (xs.sum - M) := by
      rw [path_reverse_map]
      refine List.min?_eq_some_iff.mpr ⟨?_, ?_⟩
      · refine List.mem_reverse.mpr (List.mem_map.mpr ⟨M, hMem, rfl⟩)
      · intro v hv
        obtain ⟨u, hu, heq⟩ := List.mem_map.mp (List.mem_reverse.mp hv)
        have := hBnd u hu
        rw [← heq]
        omega
    show (path xs.reverse).min?.getD 0 = xs.sum - high xs
    rw [hsome, hhigh]
    exact Option.getD_some
  · have hsome : (path xs.reverse).max? = some (xs.sum - m) := by
      rw [path_reverse_map]
      refine List.max?_eq_some_iff.mpr ⟨?_, ?_⟩
      · refine List.mem_reverse.mpr (List.mem_map.mpr ⟨m, hmem, rfl⟩)
      · intro v hv
        obtain ⟨u, hu, heq⟩ := List.mem_map.mp (List.mem_reverse.mp hv)
        have := hbnd u hu
        rw [← heq]
        omega
    show (path xs.reverse).max?.getD 0 = xs.sum - low xs
    rw [hsome, hlow]
    exact Option.getD_some

end TinylambOpt.ResearchProofs.JitKernels.ExcursionGuards_ReverseChunkRange
