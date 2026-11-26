import TinylambOpt.ResearchProofs.StorageExecution.SE_RankedMasks_Common

/-!
A validity or selection mask plus its prefix rank can map logical row positions to
packed values for SQL scans.

Proved (10 claims, including the following)

- `RunLimit.Cofactor` — Compute a parallel shard count by conditioning on its prefix
  state.
- `RunLimit.Decoder` — Recreate every accepted bitmap from its compact code.
- `RunLimit.EraseSeparator` — `∀ a b, legal model 0 a → legal model 0 b → legal model 0
  (a++[false]++b)`
-/
-- Proves all 10 `StorageExecution.RankedMasks/RunLimit` goals.
open TinylambOpt.ResearchGoals.StorageExecution.RankedMasks
set_option maxHeartbeats 0

namespace TinylambOpt.ResearchProofs.StorageExecution.SE_RankedMasks.RunLimit
theorem sound_Ordinal : RunLimit.Ordinal.Goal :=
  TinylambOpt.ResearchProofs.StorageExecution.SE_SK.ordinal_gen (m := RunLimit.model)

theorem sound_Decoder : RunLimit.Decoder.Goal :=
  TinylambOpt.ResearchProofs.StorageExecution.SE_SK.decoder_gen (m := RunLimit.model)

theorem sound_RejectSlack : RunLimit.RejectSlack.Goal :=
  TinylambOpt.ResearchProofs.StorageExecution.SE_SK.rejectSlack_gen (m := RunLimit.model)

theorem sound_PrefixRange : RunLimit.PrefixRange.Goal :=
  TinylambOpt.ResearchProofs.StorageExecution.SE_SK.prefixRange_gen (m := RunLimit.model)

theorem sound_SplitCode : RunLimit.SplitCode.Goal :=
  TinylambOpt.ResearchProofs.StorageExecution.SE_SK.splitCode_gen (m := RunLimit.model)

theorem sound_WireLookup : RunLimit.WireLookup.Goal :=
  TinylambOpt.ResearchProofs.StorageExecution.SE_SK.wireLookup_gen (m := RunLimit.model)

theorem sound_SpaceTest : RunLimit.SpaceTest.Goal :=
  TinylambOpt.ResearchProofs.StorageExecution.SE_SK.spaceTest_gen (m := RunLimit.model)

theorem sound_Cofactor : RunLimit.Cofactor.Goal :=
  TinylambOpt.ResearchProofs.StorageExecution.SE_SK.cofactor_gen (m := RunLimit.model)

theorem sound_EraseSeparator : RunLimit.EraseSeparator.Goal := by
  have hsf : ∀ q : Nat, RunLimit.model.step q false = some 0 := by
    intro q; rfl
  intro a b ha hb
  obtain ⟨s, hs⟩ := TinylambOpt.ResearchProofs.StorageExecution.SE_SK.run_some_of_legal (m := RunLimit.model) a 0 ha
  have hx0 : run RunLimit.model s [false] = some 0 := by
    rw [(show ([false] : List Bool) = false :: [] from rfl)]
    rw [TinylambOpt.ResearchProofs.StorageExecution.SE_SK.run_cons, hsf, Option.bind_some, TinylambOpt.ResearchProofs.StorageExecution.SE_SK.run_nil]
  have hrun : run RunLimit.model 0 (a ++ [false] ++ b) = run RunLimit.model 0 b := by
    rw [TinylambOpt.ResearchProofs.StorageExecution.SE_SK.run_append, TinylambOpt.ResearchProofs.StorageExecution.SE_SK.run_append, hs]
    simp only [Option.bind_some]
    rw [hx0]
    rw [Option.bind_some]
  show ((run RunLimit.model 0 (a ++ [false] ++ b)).map RunLimit.model.accept).getD false = true
  rw [hrun]
  exact hb

theorem sound_TribonacciCount : RunLimit.TribonacciCount.Goal := by
  have hsf : ∀ q : Nat, RunLimit.model.step q false = some 0 := by
    intro q; rfl
  have hst0 : RunLimit.model.step 0 true = some 1 := rfl
  have hst1 : RunLimit.model.step 1 true = some 2 := rfl
  have hstn : ∀ s : Nat, RunLimit.model.step (s + 2) true = none := by
    intro s; rfl
  have e0 : ∀ m : Nat, count RunLimit.model (m + 1) 0 =
      count RunLimit.model m 0 + count RunLimit.model m 1 := by
    intro m
    rw [show count RunLimit.model (m + 1) 0 =
        ((RunLimit.model.step 0 false).map (count RunLimit.model m)).getD 0 +
        ((RunLimit.model.step 0 true).map (count RunLimit.model m)).getD 0 from rfl]
    rw [hsf, hst0]
    simp only [Option.map_some, Option.getD_some]
  have e1 : ∀ m : Nat, count RunLimit.model (m + 1) 1 =
      count RunLimit.model m 0 + count RunLimit.model m 2 := by
    intro m
    rw [show count RunLimit.model (m + 1) 1 =
        ((RunLimit.model.step 1 false).map (count RunLimit.model m)).getD 0 +
        ((RunLimit.model.step 1 true).map (count RunLimit.model m)).getD 0 from rfl]
    rw [hsf, hst1]
    simp only [Option.map_some, Option.getD_some]
  have e2 : ∀ m : Nat, count RunLimit.model (m + 1) 2 = count RunLimit.model m 0 := by
    intro m
    rw [show count RunLimit.model (m + 1) 2 =
        ((RunLimit.model.step 2 false).map (count RunLimit.model m)).getD 0 +
        ((RunLimit.model.step 2 true).map (count RunLimit.model m)).getD 0 from rfl]
    rw [hsf, hstn]
    simp
  intro n
  have h3 : count RunLimit.model (n + 3) 0 =
      count RunLimit.model (n + 2) 0 + count RunLimit.model (n + 2) 1 := by
    rw [show (n : Nat) + 3 = (n + 2) + 1 from by omega]
    exact e0 (n + 2)
  have h1 : count RunLimit.model (n + 2) 1 =
      count RunLimit.model (n + 1) 0 + count RunLimit.model (n + 1) 2 := by
    rw [show (n : Nat) + 2 = (n + 1) + 1 from by omega]
    exact e1 (n + 1)
  have h2 : count RunLimit.model (n + 1) 2 = count RunLimit.model n 0 := e2 n
  rw [h3, h1, h2, Nat.add_assoc]

end RunLimit
