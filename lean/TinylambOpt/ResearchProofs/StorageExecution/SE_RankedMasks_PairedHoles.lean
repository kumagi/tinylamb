import TinylambOpt.ResearchProofs.StorageExecution.SE_RankedMasks_Common

/-!
A validity or selection mask plus its prefix rank can map logical row positions to
packed values for SQL scans.

Proved (10 claims, including the following)

- `PairedHoles.Cofactor` — Compute a parallel shard count by conditioning on its prefix
  state.
- `PairedHoles.Decoder` — Recreate every accepted bitmap from its compact code.
- `PairedHoles.EraseZeroPair` — `∀ a b, legal model 0 (a++[false,false]++b) = legal
  model 0 (a++b)`
-/
-- Proves all 10 `StorageExecution.RankedMasks/PairedHoles` goals.
open TinylambOpt.ResearchGoals.StorageExecution.RankedMasks
set_option maxHeartbeats 0

namespace TinylambOpt.ResearchProofs.StorageExecution.SE_RankedMasks.PairedHoles
theorem sound_Ordinal : PairedHoles.Ordinal.Goal :=
  TinylambOpt.ResearchProofs.StorageExecution.SE_SK.ordinal_gen (m := PairedHoles.model)

theorem sound_Decoder : PairedHoles.Decoder.Goal :=
  TinylambOpt.ResearchProofs.StorageExecution.SE_SK.decoder_gen (m := PairedHoles.model)

theorem sound_RejectSlack : PairedHoles.RejectSlack.Goal :=
  TinylambOpt.ResearchProofs.StorageExecution.SE_SK.rejectSlack_gen (m := PairedHoles.model)

theorem sound_PrefixRange : PairedHoles.PrefixRange.Goal :=
  TinylambOpt.ResearchProofs.StorageExecution.SE_SK.prefixRange_gen (m := PairedHoles.model)

theorem sound_SplitCode : PairedHoles.SplitCode.Goal :=
  TinylambOpt.ResearchProofs.StorageExecution.SE_SK.splitCode_gen (m := PairedHoles.model)

theorem sound_WireLookup : PairedHoles.WireLookup.Goal :=
  TinylambOpt.ResearchProofs.StorageExecution.SE_SK.wireLookup_gen (m := PairedHoles.model)

theorem sound_SpaceTest : PairedHoles.SpaceTest.Goal :=
  TinylambOpt.ResearchProofs.StorageExecution.SE_SK.spaceTest_gen (m := PairedHoles.model)

theorem sound_Cofactor : PairedHoles.Cofactor.Goal :=
  TinylambOpt.ResearchProofs.StorageExecution.SE_SK.cofactor_gen (m := PairedHoles.model)

-- Step facts for the PairedHoles machine.
private theorem hsf0 : PairedHoles.model.step 0 false = some 1 := rfl
private theorem hsfS : ∀ q : Nat, PairedHoles.model.step (q + 1) false = some 0 := by
  intro q
  show some (1 - (q + 1)) = some 0
  rw [Nat.sub_eq_zero_of_le (by omega : 1 ≤ q + 1)]
private theorem hst0 : PairedHoles.model.step 0 true = some 0 := rfl
private theorem hstS : ∀ q : Nat, PairedHoles.model.step (q + 1) true = none := by
  intro q; rfl

-- Only states 0 and 1 are reachable from state 0.
private theorem reach : ∀ (a : List Bool) (s q : Nat), s ≤ 1 → run PairedHoles.model s a = some q →
    q ≤ 1 := by
  intro a
  induction a with
  | nil =>
    intro s q hs hrun
    rw [TinylambOpt.ResearchProofs.StorageExecution.SE_SK.run_nil] at hrun
    exact (Option.some.inj hrun) ▸ hs
  | cons b as ih =>
    intro s q hs hrun
    cases s with
    | zero =>
      cases b with
      | false =>
        rw [TinylambOpt.ResearchProofs.StorageExecution.SE_SK.run_cons, hsf0, Option.bind_some] at hrun
        exact ih 1 q (by omega) hrun
      | true =>
        rw [TinylambOpt.ResearchProofs.StorageExecution.SE_SK.run_cons, hst0, Option.bind_some] at hrun
        exact ih 0 q (by omega) hrun
    | succ s' =>
      obtain rfl : s' = 0 := by omega
      cases b with
      | false =>
        rw [TinylambOpt.ResearchProofs.StorageExecution.SE_SK.run_cons, hsfS, Option.bind_some] at hrun
        exact ih 0 q (by omega) hrun
      | true =>
        rw [TinylambOpt.ResearchProofs.StorageExecution.SE_SK.run_cons, hstS, Option.bind_none] at hrun
        exact absurd hrun (by simp)

theorem sound_EraseZeroPair : PairedHoles.EraseZeroPair.Goal := by
  intro a b
  have hpair : ∀ s : Nat, s ≤ 1 →
      run PairedHoles.model s [false, false] = some s := by
    intro s hs
    have hs' : s = 0 ∨ s = 1 := by omega
    rcases hs' with rfl | rfl
    · rw [(show ([false, false] : List Bool) = false :: [false] from rfl)]
      rw [TinylambOpt.ResearchProofs.StorageExecution.SE_SK.run_cons, hsf0, Option.bind_some]
      rw [(show ([false] : List Bool) = false :: [] from rfl)]
      rw [TinylambOpt.ResearchProofs.StorageExecution.SE_SK.run_cons, hsfS, Option.bind_some, TinylambOpt.ResearchProofs.StorageExecution.SE_SK.run_nil]
    · rw [(show ([false, false] : List Bool) = false :: [false] from rfl)]
      rw [TinylambOpt.ResearchProofs.StorageExecution.SE_SK.run_cons, hsfS, Option.bind_some]
      rw [(show ([false] : List Bool) = false :: [] from rfl)]
      rw [TinylambOpt.ResearchProofs.StorageExecution.SE_SK.run_cons, hsf0, Option.bind_some, TinylambOpt.ResearchProofs.StorageExecution.SE_SK.run_nil]
  show ((run PairedHoles.model 0 (a ++ [false, false] ++ b)).map PairedHoles.model.accept).getD false = ((run PairedHoles.model 0 (a ++ b)).map PairedHoles.model.accept).getD false
  cases hrun0 : run PairedHoles.model 0 a with
  | none =>
    rw [TinylambOpt.ResearchProofs.StorageExecution.SE_SK.run_append, TinylambOpt.ResearchProofs.StorageExecution.SE_SK.run_append, hrun0]
    rw [TinylambOpt.ResearchProofs.StorageExecution.SE_SK.run_append, hrun0]
    simp
  | some s =>
    rw [TinylambOpt.ResearchProofs.StorageExecution.SE_SK.run_append, TinylambOpt.ResearchProofs.StorageExecution.SE_SK.run_append, TinylambOpt.ResearchProofs.StorageExecution.SE_SK.run_append, hrun0]
    simp only [Option.bind_some]
    rw [hpair s (reach a 0 s (by omega) hrun0)]
    rw [Option.bind_some]

theorem sound_EvenTail : PairedHoles.EvenTail.Goal := by
  have hrun : ∀ n : Nat,
      run PairedHoles.model 0 (List.replicate n false) = some (n % 2) ∧
      run PairedHoles.model 1 (List.replicate n false) = some ((n + 1) % 2) := by
    intro n
    induction n with
    | zero => exact ⟨rfl, rfl⟩
    | succ k ih =>
      constructor
      · rw [List.replicate_succ, TinylambOpt.ResearchProofs.StorageExecution.SE_SK.run_cons, hsf0, Option.bind_some, ih.2]
      · rw [List.replicate_succ, TinylambOpt.ResearchProofs.StorageExecution.SE_SK.run_cons, hsfS, Option.bind_some, ih.1]
        exact congrArg some (by omega : k % 2 = (k + 1 + 1) % 2)
  intro n
  show ((run PairedHoles.model 0 (List.replicate n false)).map PairedHoles.model.accept).getD false = decide (n % 2 = 0)
  rw [((hrun n).1)]
  simp only [Option.map_some, Option.getD_some]
  have hm : n % 2 < 2 := Nat.mod_lt n (by omega : 0 < 2)
  rcases h : n % 2 with _|_|h
  · rfl
  · rfl
  · omega

end PairedHoles
