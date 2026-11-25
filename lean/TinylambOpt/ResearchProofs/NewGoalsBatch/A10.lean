import TinylambOpt.ResearchGoals.Compression.Container

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.NewGoalsBatch.A10

open TinylambOpt.ResearchGoals.Compression.Container
open TinylambOpt.ResearchGoals.Compression.Wire

/-!
Per-goal notes:

* `PointFanout`: `rangeBlocks k n i 1` clamps `i+1` to `n`; when `i < n` the
  range is exactly the singleton `[i/k]`, otherwise it is empty.
* `ReadAmplification`: the number of touched frames is
  `(stop-1)/k - pos/k + 1`; bounding the division difference gives the stated
  `len + 2*(k-1)` bound.
* `BatchedReads`: membership in the flatMap of `rangeBlocks` is equivalent to
  the existence of a concrete in-range byte whose frame index is `i`.
* `ReuseClean`: the `i`-th chunk only depends on positions `i*k .. i*k+k-1`,
  so pointwise agreement there yields identical chunk bodies.
* `BodyLocator`: dropping the concatenated prefix of bodies and taking the
  `i`-th body length lands exactly on the `i`-th body.
-/

/-- The `i`-th body starts right after the total length of the first `i` bodies. -/
private theorem flatMap_drop_prefix (f : Block → Bytes) (bs : List Block) (i : Nat) :
    (bs.flatMap f).drop (((bs.take i).map (fun b => List.length (f b))).sum) =
      (bs.drop i).flatMap f := by
  have h1 : bs.flatMap f = (bs.take i).flatMap f ++ (bs.drop i).flatMap f := by
    rw [← List.flatMap_append, List.take_append_drop]
  rw [h1, ← List.length_flatMap]
  simp

theorem BodyLocator_sound :
    TinylambOpt.ResearchGoals.Compression.Container.BodyLocator.Goal := by
  intro bs i hi
  unfold TinylambOpt.ResearchGoals.Compression.Container.offset
  unfold TinylambOpt.ResearchGoals.Compression.Container.payload
  rw [Nat.add_sub_cancel_left]
  rw [flatMap_drop_prefix]
  have hget : bs[i]? = some bs[i] := List.getElem?_eq_getElem hi
  rw [hget]
  simp only [Option.map_some, Option.getD_some]
  rw [List.drop_eq_getElem_cons hi, List.flatMap_cons]
  rw [List.take_append_of_le_length (by simp), List.take_length]

theorem PointFanout_sound :
    TinylambOpt.ResearchGoals.Compression.Container.PointFanout.Goal := by
  intro k n i hk
  simp only [TinylambOpt.ResearchGoals.Compression.Container.rangeBlocks]
  by_cases h : i < n
  · have hmin : min n (i + 1) = i + 1 := Nat.min_eq_right (by omega)
    rw [hmin]
    have hlt : i < i + 1 := by omega
    rw [if_pos hlt, if_pos h]
    have hsub : i + 1 - 1 = i := by omega
    rw [hsub]
    simp
  · have hmin : min n (i + 1) = n := Nat.min_eq_left (by omega)
    rw [hmin]
    simp [h]

private theorem chunks_getElem?_some (k : Nat) (s : Bytes) (i : Nat)
    (h : i < (s.length + k - 1) / k) :
    (chunks k s)[i]? = some ((s.drop (i * k)).take k) := by
  unfold TinylambOpt.ResearchGoals.Compression.Container.chunks
  rw [List.getElem?_map, List.getElem?_range h]
  simp

private theorem chunks_getElem?_none (k : Nat) (s : Bytes) (i : Nat)
    (h : (s.length + k - 1) / k ≤ i) :
    (chunks k s)[i]? = none := by
  unfold TinylambOpt.ResearchGoals.Compression.Container.chunks
  rw [List.getElem?_map, List.getElem?_eq_none (by simpa using h)]
  simp

private theorem chunk_i_eq (k : Nat) (s t : Bytes) (i : Nat)
    (hlen : s.length = t.length)
    (h : ∀ j < k, s[i * k + j]? = t[i * k + j]?) :
    (s.drop (i * k)).take k = (t.drop (i * k)).take k := by
  apply List.ext_getElem?
  intro j
  rw [List.getElem?_take, List.getElem?_take, List.getElem?_drop, List.getElem?_drop]
  by_cases hj : j < k
  · rw [if_pos hj, if_pos hj]
    exact h j hj
  · rw [if_neg hj, if_neg hj]

private theorem chunks_i_eq (k : Nat) (s t : Bytes) (i : Nat)
    (hlen : s.length = t.length)
    (h : ∀ j < k, s[i * k + j]? = t[i * k + j]?) :
    (chunks k s)[i]? = (chunks k t)[i]? := by
  have hN : (s.length + k - 1) / k = (t.length + k - 1) / k := by rw [hlen]
  by_cases hi : i < (s.length + k - 1) / k
  · have hi' : i < (t.length + k - 1) / k := by rw [← hN]; exact hi
    rw [chunks_getElem?_some k s i hi, chunks_getElem?_some k t i hi']
    rw [chunk_i_eq k s t i hlen h]
  · have hi2 : (s.length + k - 1) / k ≤ i := by omega
    have hi2' : (t.length + k - 1) / k ≤ i := by rw [← hN]; exact hi2
    rw [chunks_getElem?_none k s i hi2, chunks_getElem?_none k t i hi2']

theorem ReuseClean_sound :
    TinylambOpt.ResearchGoals.Compression.Container.ReuseClean.Goal := by
  intro c k s t i hk hlen h
  have hch : (chunks k s)[i]? = (chunks k t)[i]? := chunks_i_eq k s t i hlen h
  unfold TinylambOpt.ResearchGoals.Compression.Container.blocks
  rw [List.getElem?_map, List.getElem?_map, hch]

private theorem mem_rangeBlocks_iff (k n pos len i : Nat) (hk : 0 < k) :
    i ∈ rangeBlocks k n pos len ↔
      ∃ x, pos ≤ x ∧ x < pos + len ∧ x < n ∧ x / k = i := by
  simp only [TinylambOpt.ResearchGoals.Compression.Container.rangeBlocks]
  set stop := min n (pos + len) with hstop
  by_cases hps : pos < stop
  · rw [if_pos hps, List.mem_map, List.mem_range]
    constructor
    · rintro ⟨j, hj, hji⟩
      have hik : pos / k + j ≤ (stop - 1) / k := by omega
      have hmul : (pos / k + j) * k ≤ stop - 1 :=
        le_trans (Nat.mul_le_mul_right k hik) (Nat.div_mul_le_self _ _)
      have hposle : pos ≤ stop - 1 := by omega
      refine ⟨max pos ((pos / k + j) * k), le_max_left _ _, ?_, ?_, ?_⟩
      · have hstople : stop ≤ pos + len := by rw [hstop]; exact Nat.min_le_right _ _
        have hmax : max pos ((pos / k + j) * k) ≤ stop - 1 := max_le hposle hmul
        omega
      · have hstople : stop ≤ n := by rw [hstop]; exact Nat.min_le_left _ _
        have hmax : max pos ((pos / k + j) * k) ≤ stop - 1 := max_le hposle hmul
        omega
      · by_cases hcase : pos < (pos / k + j) * k
        · rw [max_eq_right (le_of_lt hcase), Nat.mul_div_cancel_left _ hk]
          exact hji
        · have hcase' : (pos / k + j) * k ≤ pos := by omega
          rw [max_eq_left hcase']
          have hj0 : j = 0 := by
            have hdecomp : pos = (pos / k) * k + pos % k := by
              rw [Nat.mul_comm]
              exact (Nat.div_add_mod pos k).symm
            have hmod : pos % k < k := Nat.mod_lt _ hk
            have hle : (pos / k) * k + j * k ≤ (pos / k) * k + pos % k := by
              have := hcase'
              rw [hdecomp, Nat.add_mul] at this
              exact this
            have hjk : j * k < k := by omega
            rcases Nat.eq_zero_or_pos j with h0 | hpos
            · exact h0
            · have : k ≤ j * k := by
                calc k = 1 * k := (Nat.one_mul k).symm
                  _ ≤ j * k := Nat.mul_le_mul_right k hpos
              omega
          rw [hj0] at hji
          simpa using hji
  · rw [if_neg hps]
    constructor
    · intro hmem
      exact List.not_mem_nil hmem
    · rintro ⟨x, hx1, hx2, hx3, hx4⟩
      have : pos < stop := by
        rw [hstop]
        omega
      exact absurd this hps

theorem BatchedReads_sound :
    TinylambOpt.ResearchGoals.Compression.Container.BatchedReads.Goal := by
  intro k n req i hk
  rw [List.mem_eraseDups, List.mem_flatMap]
  constructor
  · rintro ⟨p, hp, hmem⟩
    obtain ⟨x, hx1, hx2, hx3, hx4⟩ := (mem_rangeBlocks_iff k n p.1 p.2 i hk).mp hmem
    exact ⟨p, hp, x, hx3, hx1, hx2, hx4⟩
  · rintro ⟨p, hp, x, hx3, hx1, hx2, hx4⟩
    exact ⟨p, hp, (mem_rangeBlocks_iff k n p.1 p.2 i hk).mpr ⟨x, hx1, hx2, hx3, hx4⟩⟩

end TinylambOpt.ResearchProofs.NewGoalsBatch.A10
