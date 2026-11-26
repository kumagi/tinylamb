import TinylambOpt.ResearchGoals.JitKernels.PackedFieldLoads

/-!
Appending zero bytes to a packed allocation changes neither the bytes a masked
word load sees nor the decoded value: bytes beyond the original frame are read
as zero. This licenses physical zero padding of the final allocation.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
namespace Nov26.Proof_PackedFieldLoads_ZeroPaddingMaskedWord
open TinylambOpt.ResearchGoals.JitKernels.PackedFieldLoads
open TinylambOpt.ResearchGoals.Compression.Wire

private theorem fromLittle_take_of_all_zero (lz : Bytes) (k : Nat)
    (hlz : ∀ b ∈ lz, b = 0) : fromLittle (lz.take k) = 0 := by
  induction lz generalizing k with
  | nil => simp [fromLittle]
  | cons b lz ih =>
      cases k with
      | zero => rfl
      | succ k =>
          have hb : b = 0 := hlz b (by simp)
          rw [List.take_succ_cons, fromLittle, hb]
          rw [ih k (fun x hx => hlz x (by simp [hx]))]

private theorem fromLittle_take_append_zeros (l lz : Bytes) (k : Nat)
    (hlz : ∀ b ∈ lz, b = 0) :
    fromLittle ((l ++ lz).take k) = fromLittle (l.take k) := by
  induction l generalizing k with
  | nil =>
      simpa [fromLittle] using fromLittle_take_of_all_zero lz k hlz
  | cons b l ih =>
      cases k with
      | zero => rfl
      | succ k => simp [fromLittle, ih k]

private theorem fromLittle_drop_take_append_zeros (l lz : Bytes) (s k : Nat)
    (hlz : ∀ b ∈ lz, b = 0) :
    fromLittle (((l ++ lz).drop s).take k) = fromLittle ((l.drop s).take k) := by
  induction l generalizing s with
  | nil =>
      simpa [fromLittle] using fromLittle_take_of_all_zero (lz.drop s) k (by
        intro b hb
        have hb' : b ∈ lz := List.mem_of_mem_drop hb
        exact hlz b hb')
  | cons b l ih =>
      cases s with
      | zero => exact fromLittle_take_append_zeros (b :: l) lz k hlz
      | succ s => simpa using ih s

theorem sound : ZeroPaddingMaskedWord.Goal := by
  intro w xs i hi
  unfold load
  rw [fromLittle_drop_take_append_zeros]
  intro b hb
  simpa using hb

end Nov26.Proof_PackedFieldLoads_ZeroPaddingMaskedWord
