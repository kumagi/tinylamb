import TinylambOpt.ResearchProofs.StorageExecution.SE_RankedMasks_Common

/-!
A validity or selection mask plus its prefix rank can map logical row positions to
packed values for SQL scans.

Proved (10 claims, including the following)

- `BalancedNesting.Cofactor` — Compute a parallel shard count by conditioning on its
  prefix state.
- `BalancedNesting.Decoder` — Recreate every accepted bitmap from its compact code.
- `BalancedNesting.DepthCap` — `∀ n q, n<q → count model n q=0`
-/
-- Proves all 10 `StorageExecution.RankedMasks/BalancedNesting` goals.
open TinylambOpt.ResearchGoals.StorageExecution.RankedMasks
set_option maxHeartbeats 0

namespace TinylambOpt.ResearchProofs.StorageExecution.SE_RankedMasks.BalancedNesting
theorem sound_Ordinal : BalancedNesting.Ordinal.Goal :=
  TinylambOpt.ResearchProofs.StorageExecution.SE_SK.ordinal_gen (m := BalancedNesting.model)

theorem sound_Decoder : BalancedNesting.Decoder.Goal :=
  TinylambOpt.ResearchProofs.StorageExecution.SE_SK.decoder_gen (m := BalancedNesting.model)

theorem sound_RejectSlack : BalancedNesting.RejectSlack.Goal :=
  TinylambOpt.ResearchProofs.StorageExecution.SE_SK.rejectSlack_gen (m := BalancedNesting.model)

theorem sound_PrefixRange : BalancedNesting.PrefixRange.Goal :=
  TinylambOpt.ResearchProofs.StorageExecution.SE_SK.prefixRange_gen (m := BalancedNesting.model)

theorem sound_SplitCode : BalancedNesting.SplitCode.Goal :=
  TinylambOpt.ResearchProofs.StorageExecution.SE_SK.splitCode_gen (m := BalancedNesting.model)

theorem sound_WireLookup : BalancedNesting.WireLookup.Goal :=
  TinylambOpt.ResearchProofs.StorageExecution.SE_SK.wireLookup_gen (m := BalancedNesting.model)

theorem sound_SpaceTest : BalancedNesting.SpaceTest.Goal :=
  TinylambOpt.ResearchProofs.StorageExecution.SE_SK.spaceTest_gen (m := BalancedNesting.model)

theorem sound_Cofactor : BalancedNesting.Cofactor.Goal :=
  TinylambOpt.ResearchProofs.StorageExecution.SE_SK.cofactor_gen (m := BalancedNesting.model)

-- Step facts for the BalancedNesting machine.
private theorem hst : ∀ q : Nat, BalancedNesting.model.step q true = some (q + 1) := by
  intro q; rfl
private theorem hsfS : ∀ q : Nat, BalancedNesting.model.step (q + 1) false = some q := by
  intro q; rfl
private theorem hsf0 : BalancedNesting.model.step 0 false = none := rfl

private theorem e : ∀ m q : Nat, count BalancedNesting.model (m + 1) q =
    (if q == 0 then 0 else count BalancedNesting.model m (q - 1)) +
    count BalancedNesting.model m (q + 1) := by
  intro m q
  rw [show count BalancedNesting.model (m + 1) q =
      ((BalancedNesting.model.step q false).map (count BalancedNesting.model m)).getD 0 +
      ((BalancedNesting.model.step q true).map (count BalancedNesting.model m)).getD 0 from rfl]
  rw [hst]
  simp only [Option.map_some, Option.getD_some]
  rcases q with _|p
  · rw [hsf0]
    simp
  · rw [hsfS]
    simp only [Option.map_some, Option.getD_some]
    simp [Nat.succ_sub_succ_eq_sub]

private theorem hpar : ∀ n q : Nat, (n + q) % 2 = 1 → count BalancedNesting.model n q = 0 := by
  intro n
  induction n with
  | zero =>
    intro q h
    show (BalancedNesting.model.accept q).toNat = 0
    rw [show BalancedNesting.model.accept q = (q == 0) from rfl]
    rw [show (q == 0) = false from TinylambOpt.ResearchProofs.StorageExecution.SE_SK.beq_eq_false (by omega : ¬ q = 0)]
    rfl
  | succ m ih =>
    intro q h
    rw [e]
    rcases q with _|p
    · have h1 := ih 1 (by omega)
      simp [h1]
    · rw [show ((p + 1 : Nat) == 0) = false from TinylambOpt.ResearchProofs.StorageExecution.SE_SK.beq_eq_false (by omega : ¬ (p + 1 = 0))]
      have h1 := ih p (by omega)
      have h2 := ih (p + 2) (by omega)
      simp [h1, h2]

theorem sound_DepthCap : BalancedNesting.DepthCap.Goal := by
  intro n
  induction n with
  | zero =>
    intro q h
    show (BalancedNesting.model.accept q).toNat = 0
    rw [show BalancedNesting.model.accept q = (q == 0) from rfl]
    rw [show (q == 0) = false from TinylambOpt.ResearchProofs.StorageExecution.SE_SK.beq_eq_false (by omega : ¬ q = 0)]
    rfl
  | succ m ih =>
    intro q h
    rw [e]
    rcases q with _|p
    · omega
    · rw [show ((p + 1 : Nat) == 0) = false from TinylambOpt.ResearchProofs.StorageExecution.SE_SK.beq_eq_false (by omega : ¬ (p + 1 = 0))]
      have h1 := ih p (by omega)
      have h2 := ih (p + 2) (by omega)
      simp [h1, h2]

theorem sound_OddLengthEmpty : BalancedNesting.OddLengthEmpty.Goal := by
  intro n
  exact hpar (2 * n + 1) 0 (by omega)

end BalancedNesting
