import TinylambOpt.ResearchProofs.StorageExecution.SE_RankedMasks_Common
-- Proves all 10 `StorageExecution.RankedMasks/Cooldown` goals.
open TinylambOpt.ResearchGoals.StorageExecution.RankedMasks
set_option maxHeartbeats 0

namespace TinylambOpt.ResearchProofs.StorageExecution.SE_RankedMasks.Cooldown
theorem sound_Ordinal : Cooldown.Ordinal.Goal :=
  TinylambOpt.ResearchProofs.StorageExecution.SE_SK.ordinal_gen (m := Cooldown.model)

theorem sound_Decoder : Cooldown.Decoder.Goal :=
  TinylambOpt.ResearchProofs.StorageExecution.SE_SK.decoder_gen (m := Cooldown.model)

theorem sound_RejectSlack : Cooldown.RejectSlack.Goal :=
  TinylambOpt.ResearchProofs.StorageExecution.SE_SK.rejectSlack_gen (m := Cooldown.model)

theorem sound_PrefixRange : Cooldown.PrefixRange.Goal :=
  TinylambOpt.ResearchProofs.StorageExecution.SE_SK.prefixRange_gen (m := Cooldown.model)

theorem sound_SplitCode : Cooldown.SplitCode.Goal :=
  TinylambOpt.ResearchProofs.StorageExecution.SE_SK.splitCode_gen (m := Cooldown.model)

theorem sound_WireLookup : Cooldown.WireLookup.Goal :=
  TinylambOpt.ResearchProofs.StorageExecution.SE_SK.wireLookup_gen (m := Cooldown.model)

theorem sound_SpaceTest : Cooldown.SpaceTest.Goal :=
  TinylambOpt.ResearchProofs.StorageExecution.SE_SK.spaceTest_gen (m := Cooldown.model)

theorem sound_Cofactor : Cooldown.Cofactor.Goal :=
  TinylambOpt.ResearchProofs.StorageExecution.SE_SK.cofactor_gen (m := Cooldown.model)

theorem sound_CheckpointConvergence : Cooldown.CheckpointConvergence.Goal := by
  intro q hq
  rcases q with _|_|_|h
  · rfl
  · rfl
  · rfl
  · omega

theorem sound_SpacingRecurrence : Cooldown.SpacingRecurrence.Goal := by
  have hs0f : Cooldown.model.step 0 false = some 0 := rfl
  have hs0t : Cooldown.model.step 0 true = some 2 := rfl
  have hsnf : ∀ s : Nat, Cooldown.model.step (s + 1) false = some s := by
    intro s; rfl
  have hsnt : ∀ s : Nat, Cooldown.model.step (s + 1) true = none := by
    intro s; rfl
  have e0 : ∀ m : Nat, count Cooldown.model (m + 1) 0 =
      count Cooldown.model m 0 + count Cooldown.model m 2 := by
    intro m
    rw [show count Cooldown.model (m + 1) 0 =
        ((Cooldown.model.step 0 false).map (count Cooldown.model m)).getD 0 +
        ((Cooldown.model.step 0 true).map (count Cooldown.model m)).getD 0 from rfl]
    rw [hs0f, hs0t]
    simp only [Option.map_some, Option.getD_some]
  have e1 : ∀ m : Nat, count Cooldown.model (m + 1) 1 = count Cooldown.model m 0 := by
    intro m
    rw [show count Cooldown.model (m + 1) 1 =
        ((Cooldown.model.step 1 false).map (count Cooldown.model m)).getD 0 +
        ((Cooldown.model.step 1 true).map (count Cooldown.model m)).getD 0 from rfl]
    rw [hsnf, hsnt]
    simp
  have e2 : ∀ m : Nat, count Cooldown.model (m + 1) 2 = count Cooldown.model m 1 := by
    intro m
    rw [show count Cooldown.model (m + 1) 2 =
        ((Cooldown.model.step 2 false).map (count Cooldown.model m)).getD 0 +
        ((Cooldown.model.step 2 true).map (count Cooldown.model m)).getD 0 from rfl]
    rw [hsnf, hsnt]
    simp
  intro n
  have h3 : count Cooldown.model (n + 3) 0 =
      count Cooldown.model (n + 2) 0 + count Cooldown.model (n + 2) 2 := by
    rw [show (n : Nat) + 3 = (n + 2) + 1 from by omega]
    exact e0 (n + 2)
  have h2 : count Cooldown.model (n + 2) 2 = count Cooldown.model n 0 := by
    rw [show (n : Nat) + 2 = (n + 1) + 1 from by omega]
    rw [e2, e1]
  rw [h3, h2]

end Cooldown
