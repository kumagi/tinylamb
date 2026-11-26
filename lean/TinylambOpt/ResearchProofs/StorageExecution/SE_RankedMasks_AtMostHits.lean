import TinylambOpt.ResearchProofs.StorageExecution.SE_RankedMasks_Common

/-!
A validity or selection mask plus its prefix rank can map logical row positions to
packed values for SQL scans.

Proved (10 claims, including the following)

- `AtMostHits.Cofactor` — Compute a parallel shard count by conditioning on its prefix
  state.
- `AtMostHits.Decoder` — Recreate every accepted bitmap from its compact code.
- `AtMostHits.OneOutlier` — `∀ n, count model n 1 = n+1`
-/
-- Proves all 10 `StorageExecution.RankedMasks/AtMostHits` goals.
-- Eight follow from the generic kernel `K3.lean`; the two model-specific
-- goals are proven directly from the count recurrence.
open TinylambOpt.ResearchGoals.StorageExecution.RankedMasks
set_option maxHeartbeats 0

namespace TinylambOpt.ResearchProofs.StorageExecution.SE_RankedMasks.AtMostHits
theorem sound_Ordinal : AtMostHits.Ordinal.Goal :=
  TinylambOpt.ResearchProofs.StorageExecution.SE_SK.ordinal_gen (m := AtMostHits.model)

theorem sound_Decoder : AtMostHits.Decoder.Goal :=
  TinylambOpt.ResearchProofs.StorageExecution.SE_SK.decoder_gen (m := AtMostHits.model)

theorem sound_RejectSlack : AtMostHits.RejectSlack.Goal :=
  TinylambOpt.ResearchProofs.StorageExecution.SE_SK.rejectSlack_gen (m := AtMostHits.model)

theorem sound_PrefixRange : AtMostHits.PrefixRange.Goal :=
  TinylambOpt.ResearchProofs.StorageExecution.SE_SK.prefixRange_gen (m := AtMostHits.model)

theorem sound_SplitCode : AtMostHits.SplitCode.Goal :=
  TinylambOpt.ResearchProofs.StorageExecution.SE_SK.splitCode_gen (m := AtMostHits.model)

theorem sound_WireLookup : AtMostHits.WireLookup.Goal :=
  TinylambOpt.ResearchProofs.StorageExecution.SE_SK.wireLookup_gen (m := AtMostHits.model)

theorem sound_SpaceTest : AtMostHits.SpaceTest.Goal :=
  TinylambOpt.ResearchProofs.StorageExecution.SE_SK.spaceTest_gen (m := AtMostHits.model)

theorem sound_Cofactor : AtMostHits.Cofactor.Goal :=
  TinylambOpt.ResearchProofs.StorageExecution.SE_SK.cofactor_gen (m := AtMostHits.model)

theorem sound_OneOutlier : AtMostHits.OneOutlier.Goal := by
  have h0f : AtMostHits.model.step 0 false = some 0 := rfl
  have h0t : AtMostHits.model.step 0 true = none := rfl
  have h1f : AtMostHits.model.step 1 false = some 1 := rfl
  have h1t : AtMostHits.model.step 1 true = some 0 := rfl
  have h0 : ∀ n, count AtMostHits.model n 0 = 1 := by
    intro n
    induction n with
    | zero => rfl
    | succ k ih =>
      rw [show count AtMostHits.model (k + 1) 0 =
          ((AtMostHits.model.step 0 false).map (count AtMostHits.model k)).getD 0 +
          ((AtMostHits.model.step 0 true).map (count AtMostHits.model k)).getD 0 from rfl]
      rw [h0f, h0t]
      simp [ih]
  intro n
  induction n with
  | zero => rfl
  | succ k ih =>
    rw [show count AtMostHits.model (k + 1) 1 =
        ((AtMostHits.model.step 1 false).map (count AtMostHits.model k)).getD 0 +
        ((AtMostHits.model.step 1 true).map (count AtMostHits.model k)).getD 0 from rfl]
    rw [h1f, h1t]
    simp only [Option.map_some, Option.getD_some]
    rw [ih, h0 k]

theorem sound_UnrestrictedBudget : AtMostHits.UnrestrictedBudget.Goal := by
  have hf : ∀ q : Nat, AtMostHits.model.step q false = some q := by
    intro q; rfl
  have ht : ∀ p : Nat, AtMostHits.model.step (p + 1) true = some p := by
    intro p; rfl
  intro n
  induction n with
  | zero => intro q _; rfl
  | succ k ih =>
    intro q hle
    rcases q with _|p
    · omega
    · rw [show count AtMostHits.model (k + 1) (p + 1) =
          ((AtMostHits.model.step (p + 1) false).map (count AtMostHits.model k)).getD 0 +
          ((AtMostHits.model.step (p + 1) true).map (count AtMostHits.model k)).getD 0 from rfl]
      rw [hf, ht]
      simp only [Option.map_some, Option.getD_some]
      rw [ih (p + 1) (by omega), ih p (by omega)]
      rw [Nat.pow_succ, Nat.mul_two]

end AtMostHits
