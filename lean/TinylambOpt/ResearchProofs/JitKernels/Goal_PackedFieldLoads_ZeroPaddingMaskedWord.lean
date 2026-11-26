import TinylambOpt.ResearchGoals.JitKernels.PackedFieldLoads

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitKernels.Goal_PackedFieldLoads_ZeroPaddingMaskedWord

open TinylambOpt.ResearchGoals.JitKernels.PackedFieldLoads
open TinylambOpt.ResearchGoals.Compression.Wire

private theorem fromLittle_replicate_zero (k : Nat) : fromLittle (List.replicate k 0) = 0 := by
  induction k with
  | zero => rfl
  | succ k ih => simp [List.replicate_succ, fromLittle, ih]

private theorem fromLittle_take_append_zeros (bs : List Nat) (n k : Nat) :
    fromLittle ((bs ++ List.replicate k 0).take n) = fromLittle (bs.take n) := by
  induction bs generalizing n with
  | nil => simp [fromLittle_replicate_zero, fromLittle]
  | cons b rest ih =>
      cases n with
      | zero => simp
      | succ n => simp [fromLittle, ih]

private theorem fromLittle_drop_append_zeros (bs : List Nat) (s n k : Nat) :
    fromLittle (((bs ++ List.replicate k 0).drop s).take n) = fromLittle ((bs.drop s).take n) := by
  induction s generalizing bs with
  | zero => exact fromLittle_take_append_zeros bs n k
  | succ s ih =>
      cases bs with
      | nil => simp [fromLittle_replicate_zero, fromLittle]
      | cons b rest => simp [ih rest]

theorem sound : TinylambOpt.ResearchGoals.JitKernels.PackedFieldLoads.ZeroPaddingMaskedWord.Goal := by
  intro w xs i hi
  unfold load
  rw [fromLittle_drop_append_zeros]

end TinylambOpt.ResearchProofs.JitKernels.Goal_PackedFieldLoads_ZeroPaddingMaskedWord
