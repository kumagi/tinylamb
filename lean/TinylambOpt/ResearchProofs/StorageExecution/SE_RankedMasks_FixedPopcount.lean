import TinylambOpt.ResearchProofs.StorageExecution.SE_RankedMasks_Common
-- Proves all 10 `StorageExecution.RankedMasks/FixedPopcount` goals.
-- Eight follow from the generic kernel `K3.lean`; the two model-specific
-- goals are proven via `count model n q = Samples.choose n q`.
open TinylambOpt.ResearchGoals.StorageExecution.RankedMasks
open TinylambOpt.ResearchGoals.Atlas.SynopsisBounds
open TinylambOpt.ResearchGoals.Compression.Wire
set_option maxHeartbeats 0

namespace TinylambOpt.ResearchProofs.StorageExecution.SE_RankedMasks.FixedPopcount
theorem sound_Ordinal : FixedPopcount.Ordinal.Goal :=
  TinylambOpt.ResearchProofs.StorageExecution.SE_SK.ordinal_gen (m := FixedPopcount.model)

theorem sound_Decoder : FixedPopcount.Decoder.Goal :=
  TinylambOpt.ResearchProofs.StorageExecution.SE_SK.decoder_gen (m := FixedPopcount.model)

theorem sound_RejectSlack : FixedPopcount.RejectSlack.Goal :=
  TinylambOpt.ResearchProofs.StorageExecution.SE_SK.rejectSlack_gen (m := FixedPopcount.model)

theorem sound_PrefixRange : FixedPopcount.PrefixRange.Goal :=
  TinylambOpt.ResearchProofs.StorageExecution.SE_SK.prefixRange_gen (m := FixedPopcount.model)

theorem sound_SplitCode : FixedPopcount.SplitCode.Goal :=
  TinylambOpt.ResearchProofs.StorageExecution.SE_SK.splitCode_gen (m := FixedPopcount.model)

theorem sound_WireLookup : FixedPopcount.WireLookup.Goal :=
  TinylambOpt.ResearchProofs.StorageExecution.SE_SK.wireLookup_gen (m := FixedPopcount.model)

theorem sound_SpaceTest : FixedPopcount.SpaceTest.Goal :=
  TinylambOpt.ResearchProofs.StorageExecution.SE_SK.spaceTest_gen (m := FixedPopcount.model)

theorem sound_Cofactor : FixedPopcount.Cofactor.Goal :=
  TinylambOpt.ResearchProofs.StorageExecution.SE_SK.cofactor_gen (m := FixedPopcount.model)

-- Step facts for the FixedPopcount machine.
private theorem hsf : ∀ q, FixedPopcount.model.step q false = some q := fun _ => rfl
private theorem hst : ∀ q, FixedPopcount.model.step (q + 1) true = some q := fun _ => rfl
private theorem hst0 : FixedPopcount.model.step 0 true = none := rfl

private theorem czero : ∀ n, count FixedPopcount.model n 0 = 1 := by
  intro n
  induction n with
  | zero => rfl
  | succ m ih =>
    rw [show count FixedPopcount.model (m + 1) 0 =
        ((FixedPopcount.model.step 0 false).map (count FixedPopcount.model m)).getD 0 +
        ((FixedPopcount.model.step 0 true).map (count FixedPopcount.model m)).getD 0 from rfl]
    rw [hsf, hst0]
    simp only [Option.map_some, Option.getD_some, Option.map_none, Option.getD_none, Nat.add_zero]
    exact ih

private theorem cz : ∀ n : Nat, Samples.choose n 0 = 1 := by
  intro n
  cases n with
  | zero => rfl
  | succ m => rfl

private theorem cc : ∀ n q : Nat, count FixedPopcount.model n q = Samples.choose n q := by
  intro n
  induction n with
  | zero =>
    intro q
    cases q with
    | zero => rfl
    | succ j => rfl
  | succ m ih =>
    intro q
    rw [show count FixedPopcount.model (m + 1) q =
        ((FixedPopcount.model.step q false).map (count FixedPopcount.model m)).getD 0 +
        ((FixedPopcount.model.step q true).map (count FixedPopcount.model m)).getD 0 from rfl]
    rw [hsf]
    simp only [Option.map_some, Option.getD_some]
    rw [ih]
    cases q with
    | zero =>
      rw [hst0, Option.map_none, Option.getD_none, cz, cz, Nat.add_zero]
    | succ j =>
      rw [hst, Option.map_some, Option.getD_some, ih]
      show Samples.choose m (j + 1) + Samples.choose m j = Samples.choose (m + 1) (j + 1)
      rw [show Samples.choose (m + 1) (j + 1) = Samples.choose m j + Samples.choose m (j + 1) from rfl]
      omega

private theorem choose_over' : ∀ l k : Nat, l < k → Samples.choose l k = 0 := by
  intro l
  induction l with
  | zero =>
    intro k hk
    cases k with
    | zero => omega
    | succ K => rfl
  | succ L ih =>
    intro k hk
    cases k with
    | zero => omega
    | succ K =>
      have h : Samples.choose (L + 1) (K + 1) = Samples.choose L K + Samples.choose L (K + 1) := rfl
      rw [h, ih K (by omega), ih (K + 1) (by omega)]

private theorem choose_self' : ∀ m : Nat, Samples.choose m m = 1 := by
  intro m
  induction m with
  | zero => rfl
  | succ L ih =>
    have h : Samples.choose (L + 1) (L + 1) = Samples.choose L L + Samples.choose L (L + 1) := rfl
    rw [h, ih, choose_over' L (L + 1) (by omega), Nat.add_zero]

/-- Binomial symmetry with the necessary bound. -/
private theorem choose_sym' : ∀ m p : Nat, p ≤ m →
    Samples.choose m p = Samples.choose m (m - p) := by
  intro m
  induction m with
  | zero =>
    intro p hp
    obtain rfl : p = 0 := by omega
    rfl
  | succ M ih =>
    intro p hp
    cases p with
    | zero =>
      rw [Nat.sub_zero, cz, choose_self']
    | succ j =>
      have hj : j ≤ M := by omega
      have hL : Samples.choose (M + 1) (j + 1) = Samples.choose M j + Samples.choose M (j + 1) := rfl
      rw [hL, Nat.succ_sub_succ_eq_sub]
      rcases Nat.eq_zero_or_pos (M - j) with hz | hpos
      · obtain rfl : M = j := by omega
        rw [Nat.sub_self, choose_self', choose_over' M (M + 1) (by omega), Nat.add_zero, cz]
      · obtain ⟨D, hD⟩ : ∃ D, M - j = D + 1 := ⟨M - j - 1, by omega⟩
        have hR : Samples.choose (M + 1) (D + 1) =
            Samples.choose M D + Samples.choose M (D + 1) := rfl
        rw [hD, hR, ih j (by omega), ih (j + 1) (by omega)]
        rw [hD]
        have hjd : M - (j + 1) = D := by omega
        rw [hjd]
        omega

theorem sound_ComplementSymmetry : FixedPopcount.ComplementSymmetry.Goal := by
  intro n k hk
  rw [codeBits, codeBits, cc, cc]
  rw [choose_sym' n (n - k) (Nat.sub_le n k)]
  rw [Nat.sub_sub_self hk]

-- PopulationFrame
private theorem frun : ∀ (xs : List Bool) (q : Nat), q ≥ List.count true xs →
    run FixedPopcount.model q xs = some (q - List.count true xs) := by
  intro xs
  induction xs with
  | nil => intros; rfl
  | cons b xs ih =>
    intro q hq
    show (FixedPopcount.model.step q b).bind (fun s => run FixedPopcount.model s xs) =
      some (q - List.count true (b :: xs))
    cases b with
    | false =>
      have hc : List.count true (false :: xs) = List.count true xs := by
        simp
      rw [hc, hsf]
      exact ih q hq
    | true =>
      have hc : List.count true (true :: xs) = List.count true xs + 1 := by
        simp
      rw [hc] at hq ⊢
      cases q with
      | zero =>
        rw [hst0, Option.bind_none]
        exact absurd hq (by omega)
      | succ qa =>
        rw [hst]
        exact (ih qa (by omega : qa ≥ List.count true xs)).trans
          (congrArg some ((by omega : qa + 1 - (List.count true xs + 1) = qa - List.count true xs).symm))

private theorem legalFP : ∀ xs : List Bool,
    legal FixedPopcount.model (List.count true xs) xs = true := by
  intro xs
  show ((run FixedPopcount.model (List.count true xs) xs).map FixedPopcount.model.accept).getD false = true
  rw [frun xs (List.count true xs) (Nat.le_refl _)]
  simp [FixedPopcount.model, Nat.sub_self]

private theorem pkt_len (k r w : Nat) :
    ([k] ++ little r (byteCount w)).length = 1 + byteCount w := by
  rw [List.length_append, List.length_singleton, little, List.length_map, List.length_range]

theorem sound_PopulationFrame : FixedPopcount.PopulationFrame.Goal := by
  intro xs hlen
  dsimp only
  constructor
  · obtain ⟨R, hR⟩ := TinylambOpt.ResearchProofs.StorageExecution.SE_SK.rank_some_of_legal (m := FixedPopcount.model) xs
      (List.count true xs) (legalFP xs)
    have hgd : (rank FixedPopcount.model (List.count true xs) xs).getD 0 = R := by
      rw [hR, Option.getD_some]
    have hlt : R < Samples.choose xs.length (List.count true xs) := by
      rw [← cc]
      exact TinylambOpt.ResearchProofs.StorageExecution.SE_SK.rank_lt_count (m := FixedPopcount.model) (List.count true xs) xs R hR
    have hw : Samples.choose xs.length (List.count true xs) - 1 <
        2 ^ width (Samples.choose xs.length (List.count true xs) - 1) :=
      TinylambOpt.ResearchProofs.StorageExecution.SE_SK.width_gt (Samples.choose xs.length (List.count true xs) - 1)
    have h8 : width (Samples.choose xs.length (List.count true xs) - 1) ≤
        8 * byteCount (width (Samples.choose xs.length (List.count true xs) - 1)) :=
      TinylambOpt.ResearchProofs.StorageExecution.SE_SK.eight_byteCover (width (Samples.choose xs.length (List.count true xs) - 1))
    have hstep : 2 ^ width (Samples.choose xs.length (List.count true xs) - 1) ≤
        2 ^ (8 * byteCount (width (Samples.choose xs.length (List.count true xs) - 1))) :=
      Nat.pow_le_pow_right (by decide : 0 < 2) h8
    have hpow : 2 ^ (8 * byteCount (width (Samples.choose xs.length (List.count true xs) - 1))) =
        256 ^ byteCount (width (Samples.choose xs.length (List.count true xs) - 1)) := by
      rw [show (256 : Nat) = 2 ^ 8 from rfl, ← Nat.pow_mul]
    have hbound : R % 256 ^ byteCount (width (Samples.choose xs.length (List.count true xs) - 1)) = R := by
      apply Nat.mod_eq_of_lt
      omega
    show unrank FixedPopcount.model xs.length (List.count true xs)
        (fromLittle (little ((rank FixedPopcount.model (List.count true xs) xs).getD 0)
          (byteCount (width (Samples.choose xs.length (List.count true xs) - 1))))) = some xs
    rw [hgd, TinylambOpt.ResearchProofs.StorageExecution.SE_SK.fromLittle_little, hbound]
    exact TinylambOpt.ResearchProofs.StorageExecution.SE_SK.decoder_gen (m := FixedPopcount.model) (List.count true xs) xs R hR
  · rw [codeBits, cc]
    exact pkt_len (List.count true xs) ((rank FixedPopcount.model (List.count true xs) xs).getD 0)
      (width (Samples.choose xs.length (List.count true xs) - 1))

end FixedPopcount
