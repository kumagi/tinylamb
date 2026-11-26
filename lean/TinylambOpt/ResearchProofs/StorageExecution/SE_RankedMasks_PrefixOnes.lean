import TinylambOpt.ResearchProofs.StorageExecution.SE_RankedMasks_Common

/-!
A validity or selection mask plus its prefix rank can map logical row positions to
packed values for SQL scans.

Proved (10 claims, including the following)

- `PrefixOnes.Cofactor` — Compute a parallel shard count by conditioning on its prefix
  state.
- `PrefixOnes.CutCount` — `∀ n, count model n 0 = n+1`
- `PrefixOnes.CutOrdinal` — `∀ n k, k≤n → rank model 0 (List.replicate k true ++
  List.replicate (n-k) false)=some k`
-/
-- Proves all 10 `StorageExecution.RankedMasks/PrefixOnes` goals.
open TinylambOpt.ResearchGoals.StorageExecution.RankedMasks
set_option maxHeartbeats 0

namespace TinylambOpt.ResearchProofs.StorageExecution.SE_RankedMasks.PrefixOnes
theorem sound_Ordinal : PrefixOnes.Ordinal.Goal :=
  TinylambOpt.ResearchProofs.StorageExecution.SE_SK.ordinal_gen (m := PrefixOnes.model)

theorem sound_Decoder : PrefixOnes.Decoder.Goal :=
  TinylambOpt.ResearchProofs.StorageExecution.SE_SK.decoder_gen (m := PrefixOnes.model)

theorem sound_RejectSlack : PrefixOnes.RejectSlack.Goal :=
  TinylambOpt.ResearchProofs.StorageExecution.SE_SK.rejectSlack_gen (m := PrefixOnes.model)

theorem sound_PrefixRange : PrefixOnes.PrefixRange.Goal :=
  TinylambOpt.ResearchProofs.StorageExecution.SE_SK.prefixRange_gen (m := PrefixOnes.model)

theorem sound_SplitCode : PrefixOnes.SplitCode.Goal :=
  TinylambOpt.ResearchProofs.StorageExecution.SE_SK.splitCode_gen (m := PrefixOnes.model)

theorem sound_WireLookup : PrefixOnes.WireLookup.Goal :=
  TinylambOpt.ResearchProofs.StorageExecution.SE_SK.wireLookup_gen (m := PrefixOnes.model)

theorem sound_SpaceTest : PrefixOnes.SpaceTest.Goal :=
  TinylambOpt.ResearchProofs.StorageExecution.SE_SK.spaceTest_gen (m := PrefixOnes.model)

theorem sound_Cofactor : PrefixOnes.Cofactor.Goal :=
  TinylambOpt.ResearchProofs.StorageExecution.SE_SK.cofactor_gen (m := PrefixOnes.model)

-- Step facts for the PrefixOnes machine.
private theorem hsf : ∀ x : Nat, PrefixOnes.model.step x false = some 1 := by
  intro x
  by_cases hx : x = 1
  · subst hx
    rfl
  · show (if x == 1 && false then none else some (if false then 0 else 1)) = some 1
    rw [show (x == 1) = false from TinylambOpt.ResearchProofs.StorageExecution.SE_SK.beq_eq_false hx]
    rfl
private theorem hst0 : PrefixOnes.model.step 0 true = some 0 := rfl
private theorem hst1 : PrefixOnes.model.step 1 true = none := rfl

private theorem hfalse (m : Nat) : ∀ s : Nat,
    rank PrefixOnes.model s (List.replicate m false) = some 0 := by
  induction m with
  | zero =>
    intro s
    rw [List.replicate_zero, TinylambOpt.ResearchProofs.StorageExecution.SE_SK.rank_nil]
    simp [PrefixOnes.model]
  | succ k ih =>
    intro s
    rw [List.replicate_succ, TinylambOpt.ResearchProofs.StorageExecution.SE_SK.rank_cons, hsf, Option.bind_some, ih 1]
    simp

private theorem hcount1 : ∀ m : Nat, count PrefixOnes.model m 1 = 1 := by
  intro m
  induction m with
  | zero => rfl
  | succ k ih =>
    rw [show count PrefixOnes.model (k + 1) 1 =
        ((PrefixOnes.model.step 1 false).map (count PrefixOnes.model k)).getD 0 +
        ((PrefixOnes.model.step 1 true).map (count PrefixOnes.model k)).getD 0 from rfl]
    rw [hsf, hst1]
    simp [ih]

theorem sound_CutOrdinal : PrefixOnes.CutOrdinal.Goal := by
  have main : ∀ m j : Nat, rank PrefixOnes.model 0
      (List.replicate j true ++ List.replicate m false) = some j := by
    intro m j
    induction j generalizing m with
    | zero =>
      rw [List.replicate_zero, List.nil_append]
      exact hfalse m 0
    | succ j ih =>
      rw [List.replicate_succ, List.cons_append, TinylambOpt.ResearchProofs.StorageExecution.SE_SK.rank_cons, hst0, Option.bind_some]
      have hlen : ((List.replicate j true ++ List.replicate m false).length : Nat) =
          j + m := by
        simp [List.length_append]
      have hl : lowCount PrefixOnes.model (j + m) 0 = 1 := by
        show ((PrefixOnes.model.step 0 false).map (count PrefixOnes.model (j + m))).getD 0 = 1
        rw [hsf]
        simp only [Option.map_some, Option.getD_some]
        exact hcount1 (j + m)
      rw [ih, hlen, hl]
      simp
  intro n k _
  exact main (n - k) k

theorem sound_CutCount : PrefixOnes.CutCount.Goal := by
  have e : ∀ m : Nat, count PrefixOnes.model (m + 1) 0 =
      count PrefixOnes.model m 1 + count PrefixOnes.model m 0 := by
    intro m
    rw [show count PrefixOnes.model (m + 1) 0 =
        ((PrefixOnes.model.step 0 false).map (count PrefixOnes.model m)).getD 0 +
        ((PrefixOnes.model.step 0 true).map (count PrefixOnes.model m)).getD 0 from rfl]
    rw [hsf, hst0]
    simp only [Option.map_some, Option.getD_some]
  intro n
  induction n with
  | zero => rfl
  | succ k ih =>
    rw [e, hcount1, ih]
    omega

end PrefixOnes
