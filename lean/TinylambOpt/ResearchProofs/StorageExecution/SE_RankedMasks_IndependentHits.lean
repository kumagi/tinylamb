import TinylambOpt.ResearchProofs.StorageExecution.SE_RankedMasks_Common
-- Proves all 10 `StorageExecution.RankedMasks/IndependentHits` goals.
open TinylambOpt.ResearchGoals.StorageExecution.RankedMasks
set_option maxHeartbeats 0

namespace TinylambOpt.ResearchProofs.StorageExecution.SE_RankedMasks.IndependentHits
theorem sound_Ordinal : IndependentHits.Ordinal.Goal :=
  TinylambOpt.ResearchProofs.StorageExecution.SE_SK.ordinal_gen (m := IndependentHits.model)

theorem sound_Decoder : IndependentHits.Decoder.Goal :=
  TinylambOpt.ResearchProofs.StorageExecution.SE_SK.decoder_gen (m := IndependentHits.model)

theorem sound_RejectSlack : IndependentHits.RejectSlack.Goal :=
  TinylambOpt.ResearchProofs.StorageExecution.SE_SK.rejectSlack_gen (m := IndependentHits.model)

theorem sound_PrefixRange : IndependentHits.PrefixRange.Goal :=
  TinylambOpt.ResearchProofs.StorageExecution.SE_SK.prefixRange_gen (m := IndependentHits.model)

theorem sound_SplitCode : IndependentHits.SplitCode.Goal :=
  TinylambOpt.ResearchProofs.StorageExecution.SE_SK.splitCode_gen (m := IndependentHits.model)

theorem sound_WireLookup : IndependentHits.WireLookup.Goal :=
  TinylambOpt.ResearchProofs.StorageExecution.SE_SK.wireLookup_gen (m := IndependentHits.model)

theorem sound_SpaceTest : IndependentHits.SpaceTest.Goal :=
  TinylambOpt.ResearchProofs.StorageExecution.SE_SK.spaceTest_gen (m := IndependentHits.model)

theorem sound_Cofactor : IndependentHits.Cofactor.Goal :=
  TinylambOpt.ResearchProofs.StorageExecution.SE_SK.cofactor_gen (m := IndependentHits.model)

-- Step facts for the IndependentHits machine.
private theorem hsf : ∀ q : Nat, IndependentHits.model.step q false = some 0 := by
  intro q; rfl
private theorem hst0 : IndependentHits.model.step 0 true = some 1 := rfl
private theorem hst1 : IndependentHits.model.step 1 true = none := rfl

theorem sound_CheapConcatenation : IndependentHits.CheapConcatenation.Goal := by
  intro q a b ha hb
  obtain ⟨s, hs⟩ := TinylambOpt.ResearchProofs.StorageExecution.SE_SK.run_some_of_legal (m := IndependentHits.model) a q ha
  have hx0 : run IndependentHits.model s [false] = some 0 := by
    rw [(show ([false] : List Bool) = false :: [] from rfl)]
    rw [TinylambOpt.ResearchProofs.StorageExecution.SE_SK.run_cons, hsf, Option.bind_some, TinylambOpt.ResearchProofs.StorageExecution.SE_SK.run_nil]
  have hrun : run IndependentHits.model q (a ++ [false] ++ b) = run IndependentHits.model 0 b := by
    rw [TinylambOpt.ResearchProofs.StorageExecution.SE_SK.run_append, TinylambOpt.ResearchProofs.StorageExecution.SE_SK.run_append, hs]
    simp only [Option.bind_some]
    rw [hx0, Option.bind_some]
  show ((run IndependentHits.model q (a ++ [false] ++ b)).map IndependentHits.model.accept).getD false = true
  rw [hrun]
  exact hb

theorem sound_FibonacciCount : IndependentHits.FibonacciCount.Goal := by
  have e0 : ∀ m : Nat, count IndependentHits.model (m + 1) 0 =
      count IndependentHits.model m 0 + count IndependentHits.model m 1 := by
    intro m
    rw [show count IndependentHits.model (m + 1) 0 =
        ((IndependentHits.model.step 0 false).map (count IndependentHits.model m)).getD 0 +
        ((IndependentHits.model.step 0 true).map (count IndependentHits.model m)).getD 0 from rfl]
    rw [hsf, hst0]
    simp only [Option.map_some, Option.getD_some]
  have e1 : ∀ m : Nat, count IndependentHits.model (m + 1) 1 =
      count IndependentHits.model m 0 := by
    intro m
    rw [show count IndependentHits.model (m + 1) 1 =
        ((IndependentHits.model.step 1 false).map (count IndependentHits.model m)).getD 0 +
        ((IndependentHits.model.step 1 true).map (count IndependentHits.model m)).getD 0 from rfl]
    rw [hsf, hst1]
    simp
  intro n
  have h2 : count IndependentHits.model (n + 2) 0 =
      count IndependentHits.model (n + 1) 0 + count IndependentHits.model (n + 1) 1 := by
    rw [show (n : Nat) + 2 = (n + 1) + 1 from by omega]
    exact e0 (n + 1)
  have h1 : count IndependentHits.model (n + 1) 1 = count IndependentHits.model n 0 := e1 n
  rw [h2, h1]

end IndependentHits
