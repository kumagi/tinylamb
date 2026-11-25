import TinylambOpt.ResearchGoals.Compression.Container

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.NewGoalsBatch.A10

open TinylambOpt.ResearchGoals.Compression.Container
open TinylambOpt.ResearchGoals.Compression.Wire

/-! Per-goal notes:
  * PointFanout: one byte fits in exactly one chunk index; the clamp is precise.
  * ReadAmplification: the spanned frame count times k is at most the requested
    length plus the two partial boundary frames.
  * BatchedReads: each visited frame contains a representative byte in the
    (clamped) request, and conversely every such byte names a visited frame.
  * ReuseClean: equal bytes at offsets i*k+j (j<k) give equal i-th chunks, and
    blocks depend only on their chunk payload.
  * BodyLocator: induction on the block list; dropping the prefix payload
    lands exactly at the i-th frame body.
-/

theorem PointFanout_sound :
    TinylambOpt.ResearchGoals.Compression.Container.PointFanout.Goal := by
  intro k n i hk
  unfold rangeBlocks
  by_cases h : i < n
  · have hmin : min n (i + 1) = i + 1 := Nat.min_eq_right (by omega)
    simp [h, hmin]
  · have hmin : min n (i + 1) = n := Nat.min_eq_left (by omega)
    simp [h, hmin]

theorem ReadAmplification_sound :
    TinylambOpt.ResearchGoals.Compression.Container.ReadAmplification.Goal := by
  intro k n pos len hk
  unfold rangeBlocks
  dsimp
  split
  next hlt =>
    simp only [List.length_map, List.length_range]
    have hstop : min n (pos + len) ≤ pos + len := Nat.min_le_right _ _
    have hposstop : pos < min n (pos + len) := hlt
    have hmuldiv_stop : k * ((min n (pos + len) - 1) / k) ≤
        min n (pos + len) - 1 := Nat.mul_div_le _ _
    have hdecomp : k * (pos / k) + pos % k = pos := Nat.div_add_mod pos k
    have hmod : pos % k < k := Nat.mod_lt pos hk
    have hsubmul : (((min n (pos + len) - 1) / k - pos / k) * k) =
        ((min n (pos + len) - 1) / k) * k - (pos / k) * k := Nat.sub_mul _ _ _
    rw [Nat.add_mul, Nat.one_mul, hsubmul]
    rw [Nat.mul_comm ((min n (pos + len) - 1) / k) k,
        Nat.mul_comm (pos / k) k]
    omega
  next hge =>
    simp

theorem BatchedReads_sound :
    TinylambOpt.ResearchGoals.Compression.Container.BatchedReads.Goal := by
  intro k n req i hk
  constructor
  · intro hi
    rw [List.mem_eraseDups, List.mem_flatMap] at hi
    rcases hi with ⟨⟨pos, len⟩, hp, hi⟩
    unfold rangeBlocks at hi
    dsimp at hi
    split at hi
    · rename_i hlt
      simp only [List.mem_map, List.mem_range] at hi
      rcases hi with ⟨j, hj, rfl⟩
      have hstop_n : min n (pos + len) ≤ n := Nat.min_le_left _ _
      have hstop_len : min n (pos + len) ≤ pos + len := Nat.min_le_right _ _
      have hdiv_le : pos / k ≤ (min n (pos + len) - 1) / k := by
        apply Nat.div_le_div_right
        omega
      have hj_le : j ≤ (min n (pos + len) - 1) / k - pos / k := by omega
      by_cases hj0 : j = 0
      · subst j
        refine ⟨(pos, len), hp, pos, ?_, ?_, ?_, by simp⟩
        · omega
        · omega
        · omega
      · have hjpos : 0 < j := Nat.pos_of_ne_zero hj0
        refine ⟨(pos, len), hp, (pos / k + j) * k, ?_, ?_, ?_,
          by exact Nat.mul_div_cancel (pos / k + j) hk⟩
        · have hle : (pos / k + j) * k ≤
              ((min n (pos + len) - 1) / k) * k := by
            exact Nat.mul_le_mul_right k (by omega)
          have hmul : ((min n (pos + len) - 1) / k) * k ≤
              min n (pos + len) - 1 := Nat.div_mul_le_self _ _
          omega
        · have hpos_lt : pos < (pos / k + 1) * k := by
            simpa [Nat.mul_comm] using Nat.lt_mul_div_succ pos hk
          have hle : (pos / k + 1) * k ≤ (pos / k + j) * k := by
            exact Nat.mul_le_mul_right k (by omega)
          omega
        · have hle : (pos / k + j) * k ≤
              ((min n (pos + len) - 1) / k) * k := by
            exact Nat.mul_le_mul_right k (by omega)
          have hmul : ((min n (pos + len) - 1) / k) * k ≤
              min n (pos + len) - 1 := Nat.div_mul_le_self _ _
          omega
    · simp at hi
  · rintro ⟨⟨pos, len⟩, hp, x, hxn, hposx, hxupper, rfl⟩
    rw [List.mem_eraseDups, List.mem_flatMap]
    refine ⟨(pos, len), hp, ?_⟩
    unfold rangeBlocks
    dsimp
    split
    · rename_i hlt
      simp only [List.mem_map, List.mem_range]
      refine ⟨x / k - pos / k, ?_, ?_⟩
      · have hdiv_le : x / k ≤ (min n (pos + len) - 1) / k := by
          apply Nat.div_le_div_right
          omega
        omega
      · have hdiv_ge : pos / k ≤ x / k := Nat.div_le_div_right hposx
        omega
    · omega

theorem ReuseClean_sound :
    TinylambOpt.ResearchGoals.Compression.Container.ReuseClean.Goal := by
  intro c k s t i hk hlen hsame
  have hchunk : (s.drop (i * k)).take k = (t.drop (i * k)).take k := by
    apply List.ext_getElem?
    intro j
    simp only [List.getElem?_take, List.getElem?_drop]
    by_cases hj : j < k
    · simp [hj, hsame j hj]
    · simp [hj]
  have hchunks_len : (chunks k s).length = (chunks k t).length := by
    simp [chunks, hlen]
  by_cases hi : i < (chunks k s).length
  · have hit : i < (chunks k t).length := by omega
    have hget : (chunks k s)[i]? = (chunks k t)[i]? := by
      rw [List.getElem?_eq_getElem hi, List.getElem?_eq_getElem hit]
      simp [chunks, hchunk, List.getElem_map, List.getElem_range]
    simp [blocks, List.getElem?_map, hget]
  · have hit' : (chunks k t).length ≤ i := by omega
    have his : (chunks k s).length ≤ i := Nat.le_of_not_gt hi
    simp [blocks, List.getElem?_map, List.getElem?_eq_none his,
      List.getElem?_eq_none hit']

theorem BodyLocator_sound :
    TinylambOpt.ResearchGoals.Compression.Container.BodyLocator.Goal := by
  intro bs
  induction bs with
  | nil => simp
  | cons b bs ih =>
      intro i hi
      cases i with
      | zero => simp [offset, payload]
      | succ i =>
          have hi' : i < bs.length := by
            simpa using Nat.lt_of_succ_lt_succ hi
          specialize ih i hi'
          have hoff_cons : offset (b :: bs) (Nat.succ i) - 32 =
              b.body.length + payload (bs.take i) := by
            simp [offset, payload]
          have hoff_tail : offset bs i - 32 = payload (bs.take i) := by
            simp [offset]
          have hdropb : List.drop (b.body.length + payload (bs.take i)) b.body = [] := by
            rw [List.drop_eq_nil_iff]
            omega
          rw [hoff_cons]
          simpa [List.drop_append, hdropb, hoff_tail, Nat.add_assoc, Nat.add_comm,
            Nat.add_left_comm] using ih

end TinylambOpt.ResearchProofs.NewGoalsBatch.A10
