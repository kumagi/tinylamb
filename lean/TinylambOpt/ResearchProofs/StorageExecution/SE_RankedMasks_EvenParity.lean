import TinylambOpt.ResearchProofs.StorageExecution.SE_RankedMasks_Common
-- Proves all 10 `StorageExecution.RankedMasks/EvenParity` goals.
-- Eight follow from the generic kernel `K3.lean`; the two model-specific
-- goals use `count model (n+1) q = 2^n` for `q < 2`.
open TinylambOpt.ResearchGoals.StorageExecution.RankedMasks
set_option maxHeartbeats 0

namespace TinylambOpt.ResearchProofs.StorageExecution.SE_RankedMasks.EvenParity
theorem sound_Ordinal : EvenParity.Ordinal.Goal :=
  TinylambOpt.ResearchProofs.StorageExecution.SE_SK.ordinal_gen (m := EvenParity.model)

theorem sound_Decoder : EvenParity.Decoder.Goal :=
  TinylambOpt.ResearchProofs.StorageExecution.SE_SK.decoder_gen (m := EvenParity.model)

theorem sound_RejectSlack : EvenParity.RejectSlack.Goal :=
  TinylambOpt.ResearchProofs.StorageExecution.SE_SK.rejectSlack_gen (m := EvenParity.model)

theorem sound_PrefixRange : EvenParity.PrefixRange.Goal :=
  TinylambOpt.ResearchProofs.StorageExecution.SE_SK.prefixRange_gen (m := EvenParity.model)

theorem sound_SplitCode : EvenParity.SplitCode.Goal :=
  TinylambOpt.ResearchProofs.StorageExecution.SE_SK.splitCode_gen (m := EvenParity.model)

theorem sound_WireLookup : EvenParity.WireLookup.Goal :=
  TinylambOpt.ResearchProofs.StorageExecution.SE_SK.wireLookup_gen (m := EvenParity.model)

theorem sound_SpaceTest : EvenParity.SpaceTest.Goal :=
  TinylambOpt.ResearchProofs.StorageExecution.SE_SK.spaceTest_gen (m := EvenParity.model)

theorem sound_Cofactor : EvenParity.Cofactor.Goal :=
  TinylambOpt.ResearchProofs.StorageExecution.SE_SK.cofactor_gen (m := EvenParity.model)

-- Facts about the EvenParity machine.
private theorem hstep : ∀ q b, EvenParity.model.step q b = some ((q + b.toNat) % 2) :=
  fun _ _ => rfl

private theorem fpar : ∀ (xs : List Bool) (s : Nat), s < 2 →
    run EvenParity.model s xs = some ((s + List.count true xs) % 2) := by
  intro xs
  induction xs with
  | nil =>
    intro s hs
    rw [show List.count true ([] : List Bool) = 0 from rfl, Nat.add_zero, Nat.mod_eq_of_lt hs]
    rfl
  | cons b xs ih =>
    intro s hs
    rw [TinylambOpt.ResearchProofs.StorageExecution.SE_SK.run_cons, hstep, Option.bind_some,
      ih ((s + b.toNat) % 2) (Nat.mod_lt _ (by decide : 0 < 2))]
    refine Option.some.inj ?_
    cases b <;> simp <;> omega

private theorem legalChar : ∀ (xs : List Bool) (s : Nat), s < 2 →
    legal EvenParity.model s xs = ((s + List.count true xs) % 2 == 0) := by
  intro xs s hs
  show ((run EvenParity.model s xs).map EvenParity.model.accept).getD false = _
  rw [fpar xs s hs, Option.map_some, Option.getD_some]
  rfl

private theorem countNot : ∀ xs : List Bool,
    List.count true (xs.map (!·)) = xs.length - List.count true xs := by
  intro xs
  induction xs with
  | nil => rfl
  | cons b xs ih =>
    rw [List.map_cons, List.length_cons]
    cases b
    · rw [List.count_cons, ih, List.count_cons]
      simp
      have hcb : List.count true xs ≤ xs.length := List.count_le_length (a := true)
      omega
    · rw [List.count_cons, ih, List.count_cons]
      simp

private theorem ci : ∀ m q : Nat, q < 2 → count EvenParity.model (m + 1) q = 2 ^ m := by
  intro m
  induction m with
  | zero =>
    intro q hq
    have : q = 0 ∨ q = 1 := by omega
    rcases this with rfl | rfl <;> rfl
  | succ M ih =>
    intro q hq
    rw [show count EvenParity.model (M + 1 + 1) q =
        ((EvenParity.model.step q false).map (count EvenParity.model (M + 1))).getD 0 +
        ((EvenParity.model.step q true).map (count EvenParity.model (M + 1))).getD 0 from rfl]
    rw [hstep, Option.map_some, Option.getD_some, hstep, Option.map_some, Option.getD_some]
    rw [ih ((q + false.toNat) % 2) (Nat.mod_lt _ (by decide : 0 < 2)),
      ih ((q + true.toNat) % 2) (Nat.mod_lt _ (by decide : 0 < 2))]
    rw [Nat.pow_succ, Nat.mul_two]

theorem sound_ImpliedLastBit : EvenParity.ImpliedLastBit.Goal := ci

private theorem hlow : ∀ (n u : Nat), u < 2 →
    lowCount EvenParity.model n u = count EvenParity.model n u := by
  intro n u hu
  show ((EvenParity.model.step u false).map (count EvenParity.model n)).getD 0 =
    count EvenParity.model n u
  rw [hstep, Option.map_some, Option.getD_some, Bool.toNat_false, Nat.add_zero,
    Nat.mod_eq_of_lt hu]

/-- Mutual statement: for even-length words the complement keeps the start
state; for odd-length words it flips it. Both sums of ranks are `count n s - 1`. -/
private theorem co : ∀ (xs : List Bool) (s : Nat), s < 2 →
    (xs.length % 2 = 0 → legal EvenParity.model s xs →
        legal EvenParity.model s (xs.map (!·)) →
        (rank EvenParity.model s xs).getD 0 + (rank EvenParity.model s (xs.map (!·))).getD 0 =
          count EvenParity.model xs.length s - 1) ∧
    (xs.length % 2 = 1 → legal EvenParity.model s xs →
        legal EvenParity.model ((s + 1) % 2) (xs.map (!·)) →
        (rank EvenParity.model s xs).getD 0 +
          (rank EvenParity.model ((s + 1) % 2) (xs.map (!·))).getD 0 =
          count EvenParity.model xs.length s - 1) := by
  intro xs
  induction xs with
  | nil =>
    intro s hs
    constructor
    · intro hlen hl _
      have hs0 : s = 0 := by
        rcases s with _|t
        · rfl
        · exfalso
          rw [legalChar ([] : List Bool) (t + 1) hs, List.count_nil, Nat.add_zero] at hl
          have h2 : (t + 1) % 2 = 1 := by omega
          rw [h2] at hl
          simp at hl
      rw [hs0]
      rfl
    · intro hlen _ _
      simp at hlen
  | cons b xs ih =>
    intro s hs
    have hs' : ((s + b.toNat) % 2) < 2 := Nat.mod_lt _ (by decide : 0 < 2)
    have hty : ((s + 1) % 2) < 2 := Nat.mod_lt _ (by decide : 0 < 2)
    have hcb : List.count true xs ≤ xs.length := List.count_le_length (a := true)
    have hc : List.count true (b :: xs) = List.count true xs + b.toNat := by
      cases b <;> simp
    have hmap : (b :: xs).map (!·) = (!b) :: xs.map (!·) := rfl
    have hlen1 : (b :: xs).length = xs.length + 1 := rfl
    have he1 : (s + List.count true (b :: xs)) % 2 =
        (((s + b.toNat) % 2) + List.count true xs) % 2 := by
      rw [hc]
      cases b <;> simp <;> omega
    have he2 : (s + List.count true ((b :: xs).map (!·))) % 2 =
        ((((s + b.toNat) % 2) + 1) % 2 + List.count true (xs.map (!·))) % 2 := by
      rw [countNot, hlen1, hc, countNot]
      cases b <;> simp <;> omega
    constructor
    · -- even branch: goal for `b :: xs`
      intro hlen hl hlc
      have hly : legal EvenParity.model ((s + b.toNat) % 2) xs = true := by
        rw [legalChar xs _ hs', ← he1, ← legalChar (b :: xs) s hs]
        exact hl
      have hlm : legal EvenParity.model (((s + b.toNat) % 2 + 1) % 2) (xs.map (!·)) = true := by
        rw [legalChar (xs.map (!·)) _ (by omega : ((s + b.toNat) % 2 + 1) % 2 < 2), ← he2,
          ← legalChar ((b :: xs).map (!·)) s hs]
        exact hlc
      obtain ⟨A, hA⟩ := TinylambOpt.ResearchProofs.StorageExecution.SE_SK.rank_some_of_legal xs ((s + b.toNat) % 2) hly
      obtain ⟨B, hB⟩ := TinylambOpt.ResearchProofs.StorageExecution.SE_SK.rank_some_of_legal (xs.map (!·)) (((s + b.toNat) % 2 + 1) % 2) hlm
      have hm : xs.length % 2 = 1 := by omega
      have hI := (ih _ hs').2 hm hly hlm
      have hst2 : (s + (!b).toNat) % 2 = ((s + b.toNat) % 2 + 1) % 2 := by
        cases b
        · have h : s = 0 ∨ s = 1 := by omega
          rcases h with rfl | rfl <;> decide
        · have h : s = 0 ∨ s = 1 := by omega
          rcases h with rfl | rfl <;> decide
      rw [hmap, TinylambOpt.ResearchProofs.StorageExecution.SE_SK.rank_cons, TinylambOpt.ResearchProofs.StorageExecution.SE_SK.rank_cons, hstep, hstep, hst2, Option.bind_some,
        Option.bind_some, hA, hB, Option.map_some, Option.map_some, Option.getD_some,
        Option.getD_some] at *
      have hle : 1 ≤ xs.length := by omega
      have hsc : s = 0 ∨ s = 1 := by omega
      rcases hsc with rfl | rfl <;> cases b <;>
        { have hl0 : lowCount EvenParity.model xs.length 0 =
            count EvenParity.model xs.length 0 := hlow xs.length 0 (by decide)
          have hl1 : lowCount EvenParity.model xs.length 1 =
            count EvenParity.model xs.length 1 := hlow xs.length 1 (by decide)
          have hc0 : count EvenParity.model xs.length 0 = 2 ^ (xs.length - 1) := by
            rw [show xs.length = (xs.length - 1) + 1 from by omega]
            exact ci (xs.length - 1) 0 (by decide)
          have hc1 : count EvenParity.model xs.length 1 = 2 ^ (xs.length - 1) := by
            rw [show xs.length = (xs.length - 1) + 1 from by omega]
            exact ci (xs.length - 1) 1 (by decide)
          have hc30 : count EvenParity.model (xs.length + 1) 0 = 2 ^ xs.length :=
            ci _ _ (by decide)
          have hc31 : count EvenParity.model (xs.length + 1) 1 = 2 ^ xs.length :=
            ci _ _ (by decide)
          have hpw : 2 ^ xs.length = 2 ^ (xs.length - 1) + 2 ^ (xs.length - 1) := by
            rw [show xs.length = (xs.length - 1) + 1 from by omega, Nat.pow_succ, Nat.mul_two,
              Nat.add_sub_cancel]
          simp [hlen1, hl0, hl1, hc0, hc1, hc30, hc31, hpw] at *
          obtain ⟨P, hP⟩ : ∃ P : Nat, 2 ^ (xs.length - 1) = P := ⟨_, rfl⟩
          obtain ⟨Q, hQ⟩ : ∃ Q : Nat, 2 ^ xs.length = Q := ⟨_, rfl⟩
          simp only [hP, hQ] at hI ⊢
          omega }
    · -- odd branch
      intro hlen hl hlc
      have hly : legal EvenParity.model ((s + b.toNat) % 2) xs = true := by
        rw [legalChar xs _ hs', ← he1, ← legalChar (b :: xs) s hs]
        exact hl
      have hst3 : (((s + 1) % 2) + (!b).toNat) % 2 = (s + b.toNat) % 2 := by
        cases b
        · have h : s = 0 ∨ s = 1 := by omega
          rcases h with rfl | rfl <;> decide
        · have h : s = 0 ∨ s = 1 := by omega
          rcases h with rfl | rfl <;> decide
      have hlm : legal EvenParity.model ((s + b.toNat) % 2) (xs.map (!·)) = true := by
        rw [← hst3, ← TinylambOpt.ResearchProofs.StorageExecution.SE_SK.legal_cons_some (m := EvenParity.model) ((s + 1) % 2) _ (!b)
          (xs.map (!·)) (hstep _ _), ← hmap]
        exact hlc
      obtain ⟨A, hA⟩ := TinylambOpt.ResearchProofs.StorageExecution.SE_SK.rank_some_of_legal xs ((s + b.toNat) % 2) hly
      obtain ⟨B, hB⟩ := TinylambOpt.ResearchProofs.StorageExecution.SE_SK.rank_some_of_legal (xs.map (!·)) ((s + b.toNat) % 2) hlm
      have hm : xs.length % 2 = 0 := by omega
      have hI := (ih _ hs').1 hm hly hlm
      rw [hmap, TinylambOpt.ResearchProofs.StorageExecution.SE_SK.rank_cons, TinylambOpt.ResearchProofs.StorageExecution.SE_SK.rank_cons, hstep, hstep, hst3, Option.bind_some,
        Option.bind_some, hA, hB, Option.map_some, Option.map_some, Option.getD_some,
        Option.getD_some] at *
      rcases Nat.eq_zero_or_pos xs.length with hz | hp
      · cases xs with
        | nil =>
          have h : s = 0 ∨ s = 1 := by omega
          rcases h with rfl | rfl <;> cases b <;>
            simp [rank, legal, run, count, lowCount, EvenParity.model] at * <;> omega
        | cons a as => simp at *
      · have hle : 2 ≤ xs.length := by omega
        have hsc : s = 0 ∨ s = 1 := by omega
        rcases hsc with rfl | rfl <;> cases b <;>
          { have hl0 : lowCount EvenParity.model xs.length 0 =
              count EvenParity.model xs.length 0 := hlow xs.length 0 (by decide)
            have hl1 : lowCount EvenParity.model xs.length 1 =
              count EvenParity.model xs.length 1 := hlow xs.length 1 (by decide)
            have hc0 : count EvenParity.model xs.length 0 = 2 ^ (xs.length - 1) := by
              rw [show xs.length = (xs.length - 1) + 1 from by omega]
              exact ci (xs.length - 1) 0 (by decide)
            have hc1 : count EvenParity.model xs.length 1 = 2 ^ (xs.length - 1) := by
              rw [show xs.length = (xs.length - 1) + 1 from by omega]
              exact ci (xs.length - 1) 1 (by decide)
            have hc30 : count EvenParity.model (xs.length + 1) 0 = 2 ^ xs.length :=
              ci _ _ (by decide)
            have hc31 : count EvenParity.model (xs.length + 1) 1 = 2 ^ xs.length :=
              ci _ _ (by decide)
            have hpw : 2 ^ xs.length = 2 ^ (xs.length - 1) + 2 ^ (xs.length - 1) := by
              rw [show xs.length = (xs.length - 1) + 1 from by omega, Nat.pow_succ, Nat.mul_two,
                Nat.add_sub_cancel]
            simp [hlen1, hl0, hl1, hc0, hc1, hc30, hc31, hpw] at *
            obtain ⟨P, hP⟩ : ∃ P : Nat, 2 ^ (xs.length - 1) = P := ⟨_, rfl⟩
            obtain ⟨Q, hQ⟩ : ∃ Q : Nat, 2 ^ xs.length = Q := ⟨_, rfl⟩
            simp only [hP, hQ] at hI ⊢
            omega }

theorem sound_ComplementOrder : EvenParity.ComplementOrder.Goal := by
  intro xs hlen hl
  have h0 : (0 : Nat) < 2 := by decide
  have hc : ((0 : Nat) + List.count true xs) % 2 =
      ((0 : Nat) + List.count true (xs.map (!·))) % 2 := by
    rw [Nat.zero_add, Nat.zero_add, countNot]
    have hcb : List.count true xs ≤ xs.length := List.count_le_length (a := true)
    omega
  have hlm : legal EvenParity.model 0 (xs.map (!·)) = true := by
    rw [legalChar (xs.map (!·)) 0 h0, ← hc, ← legalChar xs 0 h0]
    exact hl
  exact (co xs 0 h0).1 hlen hl hlm

end EvenParity
