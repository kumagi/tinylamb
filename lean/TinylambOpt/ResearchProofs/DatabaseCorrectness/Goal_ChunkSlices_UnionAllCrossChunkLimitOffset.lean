import TinylambOpt.ResearchGoals.DatabaseCorrectness.ChunkSlices

/-! OFFSET/LIMIT across UNION ALL chunks equals the same operator over the
    concatenated scalar bag, including chunk boundaries and constant columns. -/
set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.DatabaseCorrectness.ChunkSlices.Goal_UnionAllCrossChunkLimitOffset

open TinylambOpt.ResearchGoals.DatabaseCorrectness.ChunkSlices

private theorem rows_portion (c : Layout) (i n : Nat) :
    rows (portion c i n) = ((rows c).drop i).take n := by
  cases c with
  | flat xs => rfl
  | constant size x =>
      change List.replicate (min n (size - i)) x = ((List.replicate size x).drop i).take n
      rw [List.drop_replicate, List.take_replicate]

private theorem sub_min_count (count len : Nat) : count - min count len = count - len := by
  by_cases h : count ≤ len
  · rw [Nat.min_eq_left h]
    omega
  · have hle : len ≤ count := by omega
    rw [Nat.min_eq_right hle]

theorem sound : UnionAllCrossChunkLimitOffset.Goal := by
  intro chunks off count
  induction chunks generalizing off count with
  | nil => simp [unionSlice]
  | cons c cs ih =>
      let l := rows c
      have hpart : rows (portion c off count) = (l.drop off).take count := by
        simpa [l] using rows_portion c off count
      have hlen : ((l.drop off).take count).length = min count (l.drop off).length := by
        simp
      have hsub : count - ((l.drop off).take count).length = count - (l.drop off).length := by
        rw [hlen, sub_min_count]
      simp only [unionSlice, hpart]
      change (l.drop off).take count ++
          unionSlice cs (off - l.length) (count - ((l.drop off).take count).length) =
        ((l ++ cs.flatMap rows).drop off).take count
      rw [List.drop_append, List.take_append]
      simp only [List.length_take]
      rw [sub_min_count]
      rw [ih]

end TinylambOpt.ResearchProofs.DatabaseCorrectness.ChunkSlices.Goal_UnionAllCrossChunkLimitOffset
