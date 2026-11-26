import TinylambOpt.ResearchProofs.JitFrontiers.PackedKeys.Common_PackedKeys

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitFrontiers.PackedKeys.BackFields

open TinylambOpt.ResearchGoals.JitFrontiers.PackedKeys
open TinylambOpt.ResearchProofs.JitFrontiers.PackedKeys.Common

theorem signedHalves_back : ∀ k a b, SignedHalves.codec.fields k = some (a,b) → SignedHalves.codec.back (a,b) = k := by
  intro k a b hf
  rcases k with ⟨xk,yk⟩
  cases xk with
  | none => simp [SignedHalves.codec, pick] at hf
  | some x =>
    cases yk with
    | none => simp [SignedHalves.codec, pick] at hf
    | some y =>
      cases hx' : signedRaw 32 x with
      | none => simp [SignedHalves.codec, pick, hx'] at hf
      | some sx =>
        cases hy' : signedRaw 32 y with
        | none => simp [SignedHalves.codec, pick, hx', hy'] at hf
        | some sy =>
          simp [SignedHalves.codec, pick, hx', hy'] at hf
          rcases hf with ⟨rfl, rfl⟩
          have hbx := signedRaw_back (w := 32) (by decide) hx'
          have hby := signedRaw_back (w := 32) (by decide) hy'
          change pair (BitVec.ofNat 32 sx).toInt (BitVec.ofNat 32 sy).toInt = (some x, some y)
          simp [pair, hbx, hby]

theorem orderBiased_back : ∀ k a b, OrderBiased.codec.fields k = some (a,b) → OrderBiased.codec.back (a,b) = k := by
  intro k a b hf
  rcases k with ⟨xk,yk⟩
  cases xk with
  | none => simp [OrderBiased.codec, pick] at hf
  | some x =>
    cases yk with
    | none => simp [OrderBiased.codec, pick] at hf
    | some y =>
      cases hux : unsigned (x + 2147483648) with
      | none => simp [OrderBiased.codec, pick, biased, hux] at hf
      | some sx =>
        cases huy : unsigned (y + 2147483648) with
        | none => simp [OrderBiased.codec, pick, biased, hux, huy] at hf
        | some sy =>
          simp [OrderBiased.codec, pick, biased, hux, huy] at hf
          rcases hf with ⟨rfl, rfl⟩
          have hxnonneg : 0 ≤ x + 2147483648 := by
            unfold unsigned at hux
            by_cases h : x + 2147483648 < 0
            · simp [h] at hux
            · omega
          have hynonneg : 0 ≤ y + 2147483648 := by
            unfold unsigned at huy
            by_cases h : y + 2147483648 < 0
            · simp [h] at huy
            · omega
          have hxval : (x + 2147483648).toNat = sx := by
            simpa [unsigned, (show ¬ x + 2147483648 < 0 by omega)] using hux
          have hyval : (y + 2147483648).toNat = sy := by
            simpa [unsigned, (show ¬ y + 2147483648 < 0 by omega)] using huy
          have hbx : unbias 32 sx = x := by
            rw [← hxval]
            exact unbias_biased_of_nonneg hxnonneg
          have hby : unbias 32 sy = y := by
            rw [← hyval]
            exact unbias_biased_of_nonneg hynonneg
          change pair (unbias 32 sx) (unbias 32 sy) = (some x, some y)
          simp [pair, hbx, hby]

theorem mixedDirection_back : ∀ k a b, MixedDirection.codec.fields k = some (a,b) → MixedDirection.codec.back (a,b) = k := by
  intro k a b hf
  rcases k with ⟨xk,yk⟩
  cases xk with
  | none => simp [MixedDirection.codec, pick] at hf
  | some x =>
    cases yk with
    | none => simp [MixedDirection.codec, pick] at hf
    | some y =>
      cases hux : unsigned (x + 2147483648) with
      | none => simp [MixedDirection.codec, pick, biased, hux] at hf
      | some sx =>
        cases huy : unsigned (-y - 1 + 2147483648) with
        | none => simp [MixedDirection.codec, pick, biased, hux, huy] at hf
        | some sy =>
          simp [MixedDirection.codec, pick, biased, hux, huy] at hf
          rcases hf with ⟨rfl, rfl⟩
          have hxnonneg : 0 ≤ x + 2147483648 := by
            unfold unsigned at hux
            by_cases h : x + 2147483648 < 0
            · simp [h] at hux
            · omega
          have hynonneg : 0 ≤ -y - 1 + 2147483648 := by
            unfold unsigned at huy
            by_cases h : -y - 1 + 2147483648 < 0
            · simp [h] at huy
            · omega
          have hxval : (x + 2147483648).toNat = sx := by
            simpa [unsigned, (show ¬ x + 2147483648 < 0 by omega)] using hux
          have hyval : (-y - 1 + 2147483648).toNat = sy := by
            simpa [unsigned, (show ¬ -y - 1 + 2147483648 < 0 by omega)] using huy
          have hbx : unbias 32 sx = x := by
            rw [← hxval]
            exact unbias_biased_of_nonneg hxnonneg
          have hby : unbias 32 sy = -y - 1 := by
            rw [← hyval]
            exact unbias_biased_of_nonneg hynonneg
          change pair (unbias 32 sx) (-unbias 32 sy - 1) = (some x, some y)
          simp [pair, hbx, hby]
          omega

theorem zigzagPair_back : ∀ k a b, ZigzagPair.codec.fields k = some (a,b) → ZigzagPair.codec.back (a,b) = k := by
  intro k a b hf
  rcases k with ⟨xk,yk⟩
  cases xk with
  | none => simp [ZigzagPair.codec, pick] at hf
  | some x =>
    cases yk with
    | none => simp [ZigzagPair.codec, pick] at hf
    | some y =>
      simp [ZigzagPair.codec, pick] at hf
      rcases hf with ⟨rfl, rfl⟩
      change pair (unzig (zig x)) (unzig (zig y)) = (some x, some y)
      simp [pair, unzig_zig]

theorem correlatedDelta_back : ∀ k a b, CorrelatedDelta.codec.fields k = some (a,b) → CorrelatedDelta.codec.back (a,b) = k := by
  intro k a b hf
  rcases k with ⟨xk,yk⟩
  cases xk with
  | none => simp [CorrelatedDelta.codec, pick] at hf
  | some x =>
    cases yk with
    | none => simp [CorrelatedDelta.codec, pick] at hf
    | some y =>
      cases hux : unsigned x with
      | none => simp [CorrelatedDelta.codec, pick, hux] at hf
      | some ux =>
        cases hdy : unsigned (y - x + 32768) with
        | none => simp [CorrelatedDelta.codec, pick, hux, hdy, biased] at hf
        | some dy =>
          simp [CorrelatedDelta.codec, pick, hux, hdy, biased] at hf
          rcases hf with ⟨rfl, rfl⟩
          have hxnonneg : 0 ≤ x := by unfold unsigned at hux; by_cases h : x < 0 <;> simp [h] at hux <;> omega
          have hdynonneg : 0 ≤ y - x + 32768 := by unfold unsigned at hdy; by_cases h : y - x + 32768 < 0 <;> simp [h] at hdy <;> omega
          have huxval : x.toNat = ux := by simpa [unsigned, (show ¬ x < 0 by omega)] using hux
          have hdyval : (y - x + 32768).toNat = dy := by
            simpa [unsigned, (show ¬ y - x + 32768 < 0 by omega)] using hdy
          have hxa : (ux : Int) = x := by rw [← huxval]; exact Int.toNat_of_nonneg hxnonneg
          have hby : unbias 16 dy = y - x := by
            rw [← hdyval]
            exact unbias_biased_of_nonneg hdynonneg
          change pair (ux : Int) ((ux : Int) + unbias 16 dy) = (some x, some y)
          simp [pair, hxa, hby]
          omega

theorem deltaMagnitude_back : ∀ k a b, DeltaMagnitude.codec.fields k = some (a,b) → DeltaMagnitude.codec.back (a,b) = k := by
  intro k a b hf
  rcases k with ⟨xk,yk⟩
  cases xk with
  | none => simp [DeltaMagnitude.codec, pick] at hf
  | some x =>
    cases yk with
    | none => simp [DeltaMagnitude.codec, pick] at hf
    | some y =>
      cases hux : unsigned x with
      | none => simp [DeltaMagnitude.codec, pick, hux] at hf
      | some ux =>
        simp [DeltaMagnitude.codec, pick, hux] at hf
        rcases hf with ⟨rfl, rfl⟩
        have hxnonneg : 0 ≤ x := by unfold unsigned at hux; by_cases h : x < 0 <;> simp [h] at hux <;> omega
        have huxval : x.toNat = ux := by simpa [unsigned, (show ¬ x < 0 by omega)] using hux
        have hxa : (ux : Int) = x := by rw [← huxval]; exact Int.toNat_of_nonneg hxnonneg
        change pair (ux : Int) ((ux : Int) + unzig (zig (y - x))) = (some x, some y)
        simp [pair, hxa, unzig_zig]
        omega

theorem taggedNull_back : ∀ k a b, TaggedNull.codec.fields k = some (a,b) → TaggedNull.codec.back (a,b) = k := by
  intro k a b hf
  rcases k with ⟨xk,yk⟩
  cases xk with
  | none => simp [TaggedNull.codec] at hf
  | some x =>
    cases yk with
    | none =>
      cases hux : unsigned x with
      | none => simp [TaggedNull.codec, hux] at hf
      | some ux =>
        simp [TaggedNull.codec, hux] at hf
        rcases hf with ⟨rfl, rfl⟩
        have hxnonneg : 0 ≤ x := by unfold unsigned at hux; by_cases h : x < 0 <;> simp [h] at hux <;> omega
        have huxval : x.toNat = ux := by simpa [unsigned, (show ¬ x < 0 by omega)] using hux
        have hxa : (ux : Int) = x := by rw [← huxval]; exact Int.toNat_of_nonneg hxnonneg
        change (some (ux : Int), if 0 == 0 then none else some ((0-1:Nat):Int)) = (some x, none)
        simp [hxa]
    | some y =>
      cases hux : unsigned x with
      | none => simp [TaggedNull.codec, hux] at hf
      | some ux =>
        cases huy : unsigned y with
        | none => simp [TaggedNull.codec, hux, huy] at hf
        | some uy =>
          simp [TaggedNull.codec, hux, huy] at hf
          rcases hf with ⟨rfl, rfl⟩
          have hxnonneg : 0 ≤ x := by unfold unsigned at hux; by_cases h : x < 0 <;> simp [h] at hux <;> omega
          have hynonneg : 0 ≤ y := by unfold unsigned at huy; by_cases h : y < 0 <;> simp [h] at huy <;> omega
          have huxval : x.toNat = ux := by simpa [unsigned, (show ¬ x < 0 by omega)] using hux
          have huyval : y.toNat = uy := by simpa [unsigned, (show ¬ y < 0 by omega)] using huy
          have hxa : (ux : Int) = x := by rw [← huxval]; exact Int.toNat_of_nonneg hxnonneg
          have hyac : (uy : Int) = y := by rw [← huyval]; exact Int.toNat_of_nonneg hynonneg
          change (some (ux : Int), if (uy+1) == 0 then none else some ((uy+1-1:Nat):Int)) = (some x, some y)
          simp [hxa, hyac]

theorem xor_cancel (a b : Nat) : a ^^^ (a ^^^ b) = b := by
  rw [← Nat.xor_assoc, Nat.xor_self, Nat.zero_xor]

theorem xorSide_back : ∀ k a b, XorSide.codec.fields k = some (a,b) → XorSide.codec.back (a,b) = k := by
  intro k a b hf
  rcases k with ⟨xk,yk⟩
  cases xk with
  | none => simp [XorSide.codec, pick] at hf
  | some x =>
    cases yk with
    | none => simp [XorSide.codec, pick] at hf
    | some y =>
      cases hux : unsigned x with
      | none => simp [XorSide.codec, pick, hux] at hf
      | some ux =>
        cases huy : unsigned y with
        | none => simp [XorSide.codec, pick, hux, huy] at hf
        | some uy =>
          simp [XorSide.codec, pick, hux, huy] at hf
          rcases hf with ⟨rfl, rfl⟩
          have hxnonneg : 0 ≤ x := by unfold unsigned at hux; by_cases h : x < 0 <;> simp [h] at hux <;> omega
          have hynonneg : 0 ≤ y := by unfold unsigned at huy; by_cases h : y < 0 <;> simp [h] at huy <;> omega
          have huxval : x.toNat = ux := by simpa [unsigned, (show ¬ x < 0 by omega)] using hux
          have huyval : y.toNat = uy := by simpa [unsigned, (show ¬ y < 0 by omega)] using huy
          have hxa : (ux : Int) = x := by rw [← huxval]; exact Int.toNat_of_nonneg hxnonneg
          have hyac : (uy : Int) = y := by rw [← huyval]; exact Int.toNat_of_nonneg hynonneg
          have hxor : ux ^^^ (ux ^^^ uy) = uy := xor_cancel ux uy
          change pair ux (ux ^^^ (ux ^^^ uy)) = (some x, some y)
          simp [pair, hxa, hyac, hxor]

theorem scaledKey_back : ∀ k a b, ScaledKey.codec.fields k = some (a,b) → ScaledKey.codec.back (a,b) = k := by
  intro k a b hf
  rcases k with ⟨xk,yk⟩
  cases xk with
  | none => simp [ScaledKey.codec, pick] at hf
  | some x =>
    cases yk with
    | none => simp [ScaledKey.codec, pick] at hf
    | some y =>
      by_cases hx : x % 100 == 0
      · cases hux : unsigned (x / 100) with
        | none => simp [ScaledKey.codec, pick, hx, hux] at hf
        | some ux =>
          cases huy : unsigned y with
          | none => simp [ScaledKey.codec, pick, hx, hux, huy] at hf
          | some uy =>
            simp [ScaledKey.codec, pick, hx, hux, huy] at hf
            rcases hf with ⟨hxmod, rfl, rfl⟩
            have hxdiv : x / 100 * 100 = x := by
              have h := Int.ediv_mul_add_emod x 100
              omega
            have hqnonneg : 0 ≤ x / 100 := by
              unfold unsigned at hux
              by_cases h : x / 100 < 0 <;> simp [h] at hux <;> omega
            have hynonneg : 0 ≤ y := by unfold unsigned at huy; by_cases h : y < 0 <;> simp [h] at huy <;> omega
            have huxval : (x / 100).toNat = ux := by
              simpa [unsigned, (show ¬ x / 100 < 0 by omega)] using hux
            have huyval : y.toNat = uy := by simpa [unsigned, (show ¬ y < 0 by omega)] using huy
            have hxa : (ux : Int) = x / 100 := by rw [← huxval]; exact Int.toNat_of_nonneg hqnonneg
            have hya : (uy : Int) = y := by rw [← huyval]; exact Int.toNat_of_nonneg hynonneg
            change pair (100 * (ux : Int)) (uy : Int) = (some x, some y)
            simp [pair, hxa, hya]
            omega
      · simp [ScaledKey.codec, pick, hx] at hf
        rcases hf with ⟨hxmod, _⟩
        have hxb : (x % 100 == 0) = true := by simp [hxmod]
        exact False.elim (absurd hxb hx)

theorem shardLocal_back : ∀ k a b, ShardLocal.codec.fields k = some (a,b) → ShardLocal.codec.back (a,b) = k := by
  intro k a b hf
  rcases k with ⟨xk,yk⟩
  cases xk with
  | none => simp [ShardLocal.codec, pick] at hf
  | some x =>
    cases yk with
    | none => simp [ShardLocal.codec, pick] at hf
    | some y =>
      cases hux : unsigned x with
      | none => simp [ShardLocal.codec, pick, hux] at hf
      | some ux =>
        cases huy : unsigned y with
        | none => simp [ShardLocal.codec, pick, hux, huy] at hf
        | some uy =>
          by_cases hshard : ux / 65536 == uy / 65536
          · simp [ShardLocal.codec, pick, hux, huy, hshard] at hf
            rcases hf with ⟨hshard', rfl, rfl⟩
            have hxnonneg : 0 ≤ x := by unfold unsigned at hux; by_cases h : x < 0 <;> simp [h] at hux <;> omega
            have hynonneg : 0 ≤ y := by unfold unsigned at huy; by_cases h : y < 0 <;> simp [h] at huy <;> omega
            have huxval : x.toNat = ux := by simpa [unsigned, (show ¬ x < 0 by omega)] using hux
            have huyval : y.toNat = uy := by simpa [unsigned, (show ¬ y < 0 by omega)] using huy
            have hxa : (ux : Int) = x := by rw [← huxval]; exact Int.toNat_of_nonneg hxnonneg
            have hya : ((uy / 65536 * 65536 + uy % 65536 : Nat) : Int) = y := by
              have hdiv := Nat.div_add_mod uy 65536
              have : uy / 65536 * 65536 + uy % 65536 = uy := by omega
              rw [this, ← huyval]
              exact Int.toNat_of_nonneg hynonneg
            change pair (ux : Int) (((ux / 65536) * 65536 + uy % 65536 : Nat) : Int) = (some x, some y)
            simp [pair, hxa, hya, hshard']
          · simp [ShardLocal.codec, pick, hux, huy, hshard] at hf
            rcases hf with ⟨hshard', _, _⟩
            have hb : (ux / 65536 == uy / 65536) = true := by simp [hshard']
            exact False.elim (absurd hb hshard)


theorem recover_SignedHalves : recover SignedHalves.codec :=
  recover_of_back_fields SignedHalves.codec (by decide) signedHalves_back
theorem recover_OrderBiased : recover OrderBiased.codec :=
  recover_of_back_fields OrderBiased.codec (by decide) orderBiased_back
theorem recover_MixedDirection : recover MixedDirection.codec :=
  recover_of_back_fields MixedDirection.codec (by decide) mixedDirection_back
theorem recover_CorrelatedDelta : recover CorrelatedDelta.codec :=
  recover_of_back_fields CorrelatedDelta.codec (by decide) correlatedDelta_back
theorem recover_TaggedNull : recover TaggedNull.codec :=
  recover_of_back_fields TaggedNull.codec (by decide) taggedNull_back
theorem recover_ZigzagPair : recover ZigzagPair.codec :=
  recover_of_back_fields ZigzagPair.codec (by decide) zigzagPair_back
theorem recover_XorSide : recover XorSide.codec :=
  recover_of_back_fields XorSide.codec (by decide) xorSide_back
theorem recover_DeltaMagnitude : recover DeltaMagnitude.codec :=
  recover_of_back_fields DeltaMagnitude.codec (by decide) deltaMagnitude_back
theorem recover_ScaledKey : recover ScaledKey.codec :=
  recover_of_back_fields ScaledKey.codec (by decide) scaledKey_back
theorem recover_ShardLocal : recover ShardLocal.codec :=
  recover_of_back_fields ShardLocal.codec (by decide) shardLocal_back

end TinylambOpt.ResearchProofs.JitFrontiers.PackedKeys.BackFields
