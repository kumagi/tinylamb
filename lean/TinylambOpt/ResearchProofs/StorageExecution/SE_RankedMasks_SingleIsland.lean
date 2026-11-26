import TinylambOpt.ResearchProofs.StorageExecution.SE_RankedMasks_Common

/-!
A validity or selection mask plus its prefix rank can map logical row positions to
packed values for SQL scans.

Proved (10 claims, including the following)

- `SingleIsland.Cofactor` — Compute a parallel shard count by conditioning on its
  prefix state.
- `SingleIsland.Decoder` — Recreate every accepted bitmap from its compact code.
- `SingleIsland.EmptyFirst` — `∀ n, unrank model n 0 0 = some (List.replicate n false)`
-/
-- Proves all 10 `StorageExecution.RankedMasks/SingleIsland` goals.
open TinylambOpt.ResearchGoals.StorageExecution.RankedMasks
set_option maxHeartbeats 0

namespace TinylambOpt.ResearchProofs.StorageExecution.SE_RankedMasks.SingleIsland
theorem sound_Ordinal : SingleIsland.Ordinal.Goal :=
  TinylambOpt.ResearchProofs.StorageExecution.SE_SK.ordinal_gen (m := SingleIsland.model)

theorem sound_Decoder : SingleIsland.Decoder.Goal :=
  TinylambOpt.ResearchProofs.StorageExecution.SE_SK.decoder_gen (m := SingleIsland.model)

theorem sound_RejectSlack : SingleIsland.RejectSlack.Goal :=
  TinylambOpt.ResearchProofs.StorageExecution.SE_SK.rejectSlack_gen (m := SingleIsland.model)

theorem sound_PrefixRange : SingleIsland.PrefixRange.Goal :=
  TinylambOpt.ResearchProofs.StorageExecution.SE_SK.prefixRange_gen (m := SingleIsland.model)

theorem sound_SplitCode : SingleIsland.SplitCode.Goal :=
  TinylambOpt.ResearchProofs.StorageExecution.SE_SK.splitCode_gen (m := SingleIsland.model)

theorem sound_WireLookup : SingleIsland.WireLookup.Goal :=
  TinylambOpt.ResearchProofs.StorageExecution.SE_SK.wireLookup_gen (m := SingleIsland.model)

theorem sound_SpaceTest : SingleIsland.SpaceTest.Goal :=
  TinylambOpt.ResearchProofs.StorageExecution.SE_SK.spaceTest_gen (m := SingleIsland.model)

theorem sound_Cofactor : SingleIsland.Cofactor.Goal :=
  TinylambOpt.ResearchProofs.StorageExecution.SE_SK.cofactor_gen (m := SingleIsland.model)

-- Step facts for the SingleIsland machine.
private theorem hsf0 : SingleIsland.model.step 0 false = some 0 := rfl
private theorem hsf1 : SingleIsland.model.step 1 false = some 2 := rfl
private theorem hsf2 : SingleIsland.model.step 2 false = some 2 := rfl
private theorem hst0 : SingleIsland.model.step 0 true = some 1 := rfl
private theorem hst1 : SingleIsland.model.step 1 true = some 1 := rfl
private theorem hst2 : SingleIsland.model.step 2 true = none := rfl

private theorem c2 : ∀ m : Nat, count SingleIsland.model (m + 1) 2 =
    count SingleIsland.model m 2 := by
  intro m
  rw [show count SingleIsland.model (m + 1) 2 =
      ((SingleIsland.model.step 2 false).map (count SingleIsland.model m)).getD 0 +
      ((SingleIsland.model.step 2 true).map (count SingleIsland.model m)).getD 0 from rfl]
  rw [hsf2, hst2]
  simp only [Option.map_some, Option.getD_some, Option.map_none, Option.getD_none, Nat.add_zero]

private theorem c2n : ∀ m : Nat, count SingleIsland.model m 2 = 1 := by
  intro m
  induction m with
  | zero => rfl
  | succ k ih => rw [c2, ih]

private theorem c1 : ∀ m : Nat, count SingleIsland.model (m + 1) 1 =
    count SingleIsland.model m 2 + count SingleIsland.model m 1 := by
  intro m
  rw [show count SingleIsland.model (m + 1) 1 =
      ((SingleIsland.model.step 1 false).map (count SingleIsland.model m)).getD 0 +
      ((SingleIsland.model.step 1 true).map (count SingleIsland.model m)).getD 0 from rfl]
  rw [hsf1, hst1]
  simp only [Option.map_some, Option.getD_some]

private theorem c1n : ∀ m : Nat, count SingleIsland.model m 1 = m + 1 := by
  intro m
  induction m with
  | zero => rfl
  | succ k ih => rw [c1, c2n, ih]; omega

private theorem c0 : ∀ m : Nat, count SingleIsland.model (m + 1) 0 =
    count SingleIsland.model m 0 + count SingleIsland.model m 1 := by
  intro m
  rw [show count SingleIsland.model (m + 1) 0 =
      ((SingleIsland.model.step 0 false).map (count SingleIsland.model m)).getD 0 +
      ((SingleIsland.model.step 0 true).map (count SingleIsland.model m)).getD 0 from rfl]
  rw [hsf0, hst0]
  simp only [Option.map_some, Option.getD_some]

private theorem evenD : ∀ k : Nat, (k * (k + 1)) % 2 = 0 := by
  intro k
  rw [Nat.mul_mod, Nat.add_mod]
  have hm := Nat.mod_lt k (by omega : 0 < 2)
  rcases Nat.eq_zero_or_pos (k % 2) with hz | hp
  · rw [hz]
  · have h1 : k % 2 = 1 := by omega
    rw [h1]

theorem sound_IntervalsAndEmpty : SingleIsland.IntervalsAndEmpty.Goal := by
  intro n
  induction n with
  | zero => rfl
  | succ k ih =>
    rw [c0, c1n, ih]
    have hd : (k * (k + 1) / 2) * 2 = k * (k + 1) := by
      have h := Nat.div_add_mod (k * (k + 1)) 2
      rw [evenD k] at h
      omega
    have hE : (k + 1) * (k + 2) = (k * (k + 1) / 2 + (k + 1)) * 2 := by
      have h1 : (k * (k + 1) / 2 + (k + 1)) * 2 = k * (k + 1) / 2 * 2 + (k + 1) * 2 :=
        Nat.add_mul _ _ 2
      rw [h1, hd, Nat.mul_two]
      rw [Nat.mul_add (k + 1) k 2, Nat.mul_comm (k + 1) k, Nat.mul_two]
    have hdiv : ((k + 1) * (k + 2)) / 2 = k * (k + 1) / 2 + (k + 1) :=
      Nat.div_eq_of_eq_mul_left (by omega : 0 < 2) hE
    rw [hdiv]
    omega

theorem sound_EmptyFirst : SingleIsland.EmptyFirst.Goal := by
  have hge : ∀ m : Nat, lowCount SingleIsland.model m 0 ≥ 1 := by
    intro m
    show ((SingleIsland.model.step 0 false).map (count SingleIsland.model m)).getD 0 ≥ 1
    rw [hsf0]
    simp only [Option.map_some, Option.getD_some]
    have h := sound_IntervalsAndEmpty m
    omega
  intro n
  induction n with
  | zero =>
    rw [List.replicate_zero, TinylambOpt.ResearchProofs.StorageExecution.SE_SK.unrank_zero]
    simp [SingleIsland.model]
  | succ k ih =>
    rw [TinylambOpt.ResearchProofs.StorageExecution.SE_SK.unrank_succ]
    have hb : decide (lowCount SingleIsland.model k 0 ≤ 0) = false := by
      apply decide_eq_false
      intro h
      have h2 := hge k
      omega
    rw [hb]
    simp only [Bool.false_eq_true, reduceIte, hsf0, Option.bind_some]
    rw [ih]
    simp only [Option.bind_some]
    rw [← List.replicate_succ]

end SingleIsland
