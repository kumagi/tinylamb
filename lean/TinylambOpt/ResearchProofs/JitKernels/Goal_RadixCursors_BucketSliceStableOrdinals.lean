import TinylambOpt.ResearchGoals.JitKernels.RadixCursors

/-!
Within one bucket, live lanes receive consecutive destination ordinals in
their original selection order. This lets a SQL scatter kernel emit a bucket
slice as a tight vector store without holes or reordering.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitKernels.Goal_RadixCursors_BucketSliceStableOrdinals

open TinylambOpt.ResearchGoals.JitKernels.RadixCursors

theorem rank_succ_cons (a : Option Nat) (rest : List (Option Nat)) (i k : Nat) :
    rank (a :: rest) (i + 1) k = (if a = some k then 1 else 0) + rank rest i k := by
  cases a with
  | none => simp [rank, active]
  | some b =>
      by_cases h : b = k
      · subst b
        simp [rank, active]
        omega
      · simp [rank, active, h]

/-- The same-bucket ranks of the matching lanes are exactly `0 … count-1`. -/
theorem rank_filter_range : ∀ (xs : List (Option Nat)) (k : Nat),
    ((List.range xs.length).filter (fun i => xs[i]? == some (some k))).map (fun i => rank xs i k) =
      List.range ((active xs).count k) := by
  intro xs
  induction xs with
  | nil => intro k; simp [rank, active]
  | cons a rest ih =>
      intro k
      cases a with
      | none =>
          have hf : (List.range (none :: rest).length).filter (fun i => (none :: rest)[i]? == some (some k)) =
              ((List.range rest.length).filter (fun i => rest[i]? == some (some k))).map Nat.succ := by
            simp only [List.length_cons]
            rw [List.range_succ_eq_map]
            simp [List.filter_map, Function.comp_def]
          rw [hf]
          rw [List.map_map]
          change ((List.range rest.length).filter (fun i => rest[i]? == some (some k))).map
              (fun i => rank (none :: rest) (i + 1) k) = List.range ((active (none :: rest)).count k)
          have hm : ((List.range rest.length).filter (fun i => rest[i]? == some (some k))).map
              (fun i => rank (none :: rest) (i + 1) k) =
              ((List.range rest.length).filter (fun i => rest[i]? == some (some k))).map (fun i => rank rest i k) := by
            apply List.map_congr_left
            intro i hi
            simpa using rank_succ_cons none rest i k
          rw [hm, ih k]
          simp [active]
      | some b =>
          by_cases hbk : b = k
          · subst b
            have hf : (List.range (some k :: rest).length).filter (fun i => (some k :: rest)[i]? == some (some k)) =
                0 :: ((List.range rest.length).filter (fun i => rest[i]? == some (some k))).map Nat.succ := by
              simp only [List.length_cons]
              rw [List.range_succ_eq_map]
              simp [List.filter_map, Function.comp_def]
            rw [hf]
            simp only [List.map_cons, List.map_map]
            change rank (some k :: rest) 0 k ::
              ((List.range rest.length).filter (fun i => rest[i]? == some (some k))).map
                (fun i => rank (some k :: rest) (i + 1) k) =
              List.range ((active (some k :: rest)).count k)
            have hm : ((List.range rest.length).filter (fun i => rest[i]? == some (some k))).map
                (fun i => rank (some k :: rest) (i + 1) k) =
                (((List.range rest.length).filter (fun i => rest[i]? == some (some k))).map (fun i => rank rest i k)).map Nat.succ := by
              rw [List.map_map]
              apply List.map_congr_left
              intro i hi
              simpa [Nat.add_comm] using rank_succ_cons (some k) rest i k
            have hrank0 : rank (some k :: rest) 0 k = 0 := by simp [rank, active]
            rw [hm, ih k, hrank0]
            simp [active, List.range_succ_eq_map]
          · have hf : (List.range (some b :: rest).length).filter (fun i => (some b :: rest)[i]? == some (some k)) =
                ((List.range rest.length).filter (fun i => rest[i]? == some (some k))).map Nat.succ := by
              simp only [List.length_cons]
              rw [List.range_succ_eq_map]
              simp [hbk, List.filter_map, Function.comp_def]
            rw [hf]
            rw [List.map_map]
            change ((List.range rest.length).filter (fun i => rest[i]? == some (some k))).map
                (fun i => rank (some b :: rest) (i + 1) k) = List.range ((active (some b :: rest)).count k)
            have hm : ((List.range rest.length).filter (fun i => rest[i]? == some (some k))).map
                (fun i => rank (some b :: rest) (i + 1) k) =
                ((List.range rest.length).filter (fun i => rest[i]? == some (some k))).map (fun i => rank rest i k) := by
              apply List.map_congr_left
              intro i hi
              simpa [hbk] using rank_succ_cons (some b) rest i k
            rw [hm, ih k]
            simp [active, hbk]

theorem sound : BucketSliceStableOrdinals.Goal := by
  intro xs k
  let idx := (List.range xs.length).filter (fun i => xs[i]? == some (some k))
  have hmem : ∀ i ∈ idx, xs[i]? = some (some k) := by
    intro i hi
    have h := (List.mem_filter.mp hi).2
    simpa using h
  have hfm : ∀ l : List Nat, (∀ i ∈ l, xs[i]? = some (some k)) →
      l.filterMap (dst xs) = l.map (fun i => base xs k + rank xs i k) := by
    intro l
    induction l with
    | nil => intro; simp
    | cons i is ih =>
        intro hl
        have hi : xs[i]? = some (some k) := hl i (by simp)
        have his : ∀ j ∈ is, xs[j]? = some (some k) := by
          intro j hj
          exact hl j (by simp [hj])
        simp [dst, hi, ih his]
  rw [hfm idx hmem]
  calc
    idx.map (fun i => base xs k + rank xs i k) =
        (idx.map (fun i => rank xs i k)).map (fun r => base xs k + r) := by
      rw [List.map_map]
      simp [Function.comp_def]
    _ = (List.range ((active xs).count k)).map (fun r => base xs k + r) := by
      rw [rank_filter_range]

end TinylambOpt.ResearchProofs.JitKernels.Goal_RadixCursors_BucketSliceStableOrdinals
