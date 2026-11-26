import TinylambOpt.ResearchProofs.JitFrontiers.PackedKeys.BackFields_PackedKeys

/-!
SQL grouping or ordering can use a packed key when the representation preserves the
relevant comparisons and validity bits.

Proved (19 claims, including the following)

- `CorrelatedDelta.FarApart` — `∀ x, encode codec (pair x (x+2^15))=none`
- `CorrelatedDelta.WideSecond` — `(encode codec (pair (2^48-1) (2^48))).isSome=true`
- `DeltaMagnitude.AdjacentNegative` — `∀ x, 0≤x → x<2^52 → encode codec (pair x
  (x-1))=some (BitVec.ofNat 64 (x.toNat*4096+1))`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitFrontiers.PackedKeys.Specific

open TinylambOpt.ResearchGoals.JitFrontiers.PackedKeys
open TinylambOpt.ResearchProofs.JitFrontiers.PackedKeys.Common
open TinylambOpt.ResearchProofs.JitFrontiers.PackedKeys.BackFields

theorem xorSide_fields (x y : Nat) :
    XorSide.codec.fields (pair (x:Int) (y:Int)) = some (x, x ^^^ y) := by
  unfold XorSide.codec
  have hx : ¬ (x : Int) < 0 := by omega
  have hy : ¬ (y : Int) < 0 := by omega
  simp [pick, pair, unsigned, hx, hy]


theorem foldBoundary : ZigzagPair.FoldBoundary.Goal := zig_lt_bound

theorem allOnes : SignedHalves.AllOnesKey.Goal := by
  unfold SignedHalves.AllOnesKey.Goal
  native_decide

theorem upperCut : SignedHalves.UpperCut.Goal := by
  intro y
  unfold SignedHalves.codec
  simp [encode, pair, pick, signedRaw]


theorem zeroCorner : OrderBiased.ZeroCorner.Goal := by
  unfold OrderBiased.ZeroCorner.Goal
  native_decide

theorem signCross : OrderBiased.SignCross.Goal := by
  intro y u v hu hv
  obtain ⟨a1,b1,hf1,hga1,hgb1,hdiv1,hmod1⟩ := fields_of_encode OrderBiased.codec (by decide) hu
  obtain ⟨a2,b2,hf2,hga2,hgb2,hdiv2,hmod2⟩ := fields_of_encode OrderBiased.codec (by decide) hv
  unfold OrderBiased.codec at hf1 hf2
  simp [pick, pair] at hf1 hf2
  cases hby : biased 32 y with
  | none => simp [hby] at hf1 hf2
  | some by' =>
    simp [hby] at hf1 hf2
    cases hb1 : biased 32 (-1) with
    | none => simp [hb1] at hf1
    | some b1' =>
      simp [hb1] at hf1
      rcases hf1 with ⟨rfl, rfl⟩
      cases hb2 : biased 32 0 with
      | none => simp [hb2] at hf2
      | some b2' =>
        simp [hb2] at hf2
        rcases hf2 with ⟨rfl, rfl⟩
        have hb1val : b1' = 2147483647 := by
          unfold biased unsigned at hb1
          simp at hb1
          omega
        have hb2val : b2' = 2147483648 := by
          unfold biased unsigned at hb2
          simp at hb2
          omega
        change u.toNat / 4294967296 = b1' at hdiv1
        change u.toNat % 4294967296 = by' at hmod1
        change v.toNat / 4294967296 = b2' at hdiv2
        change v.toNat % 4294967296 = by' at hmod2
        have hbybound : by' < 4294967296 := by
          have hbnd : u.toNat % 4294967296 < 4294967296 := Nat.mod_lt _ (by decide)
          omega
        have huword := Nat.div_add_mod u.toNat 4294967296
        have hvword := Nat.div_add_mod v.toNat 4294967296
        change u.toNat < v.toNat
        omega


theorem reverseTie : MixedDirection.ReverseTie.Goal := by
  intro x a b p q hab hp hq
  obtain ⟨a1,b1,hf1,hga1,hgb1,hdiv1,hmod1⟩ := fields_of_encode MixedDirection.codec (by decide) hp
  obtain ⟨a2,b2,hf2,hga2,hgb2,hdiv2,hmod2⟩ := fields_of_encode MixedDirection.codec (by decide) hq
  unfold MixedDirection.codec at hf1 hf2
  simp [pick, pair] at hf1 hf2
  cases hbx : biased 32 x with
  | none => simp [hbx] at hf1 hf2
  | some bx =>
    simp [hbx] at hf1 hf2
    cases hbyA : biased 32 (-a-1) with
    | none => simp [hbyA] at hf1
    | some ba =>
      simp [hbyA] at hf1
      rcases hf1 with ⟨rfl, rfl⟩
      cases hbyB : biased 32 (-b-1) with
      | none => simp [hbyB] at hf2
      | some bb =>
        simp [hbyB] at hf2
        rcases hf2 with ⟨rfl, rfl⟩
        unfold biased at hbyA hbyB
        have ha_nonneg : 0 ≤ -a - 1 + 2147483648 := by
          unfold unsigned at hbyA
          by_cases h : -a - 1 + 2147483648 < 0 <;> simp [h] at hbyA <;> omega
        have hb_nonneg : 0 ≤ -b - 1 + 2147483648 := by
          unfold unsigned at hbyB
          by_cases h : -b - 1 + 2147483648 < 0 <;> simp [h] at hbyB <;> omega
        have haval : (-a - 1 + 2147483648).toNat = ba := by
          simpa [unsigned, (show ¬ -a - 1 + 2147483648 < 0 by omega)] using hbyA
        have hbval : (-b - 1 + 2147483648).toNat = bb := by
          simpa [unsigned, (show ¬ -b - 1 + 2147483648 < 0 by omega)] using hbyB
        have hab' : -b - 1 < -a - 1 := by omega
        have hlt : bb < ba := by
          rw [← hbval, ← haval]
          omega
        change p.toNat / 4294967296 = bx at hdiv1
        change p.toNat % 4294967296 = ba at hmod1
        change q.toNat / 4294967296 = bx at hdiv2
        change q.toNat % 4294967296 = bb at hmod2
        have hbnd : ba < 4294967296 := by
          have hbnd' : p.toNat % 4294967296 < 4294967296 := Nat.mod_lt _ (by decide)
          omega
        have hpword := Nat.div_add_mod p.toNat 4294967296
        have hqword := Nat.div_add_mod q.toNat 4294967296
        change q.toNat < p.toNat
        omega

theorem symmetricEndpoints : MixedDirection.SymmetricEndpoints.Goal := by
  unfold MixedDirection.SymmetricEndpoints.Goal
  native_decide

theorem farApart : CorrelatedDelta.FarApart.Goal := by
  intro x
  rw [encode]
  cases hf : CorrelatedDelta.codec.fields (pair x (x + 32768)) with
  | none => simp [hf]
  | some p =>
    rcases p with ⟨a,b⟩
    simp [hf]
    intro ha
    unfold CorrelatedDelta.codec at hf
    simp [pick, pair] at hf
    cases hux : unsigned x with
    | none =>
      rw [hux] at hf
      simp at hf
    | some ux =>
      rw [hux] at hf
      simp at hf
      have hd : x + 32768 - x = 32768 := by omega
      rw [hd] at hf
      unfold biased at hf
      have hsum : (32768 : Int) + 2 ^ (16 - 1) = 65536 := by decide
      rw [hsum] at hf
      cases hdy : unsigned 65536 with
      | none =>
        rw [hdy] at hf
        simp at hf
      | some dy =>
        rw [hdy] at hf
        simp at hf
        rcases hf with ⟨rfl, rfl⟩
        have hdyval : dy = 65536 := by simpa [unsigned] using hdy.symm
        change 65536 ≤ dy
        omega


theorem wideSecond : CorrelatedDelta.WideSecond.Goal := by
  unfold CorrelatedDelta.WideSecond.Goal
  native_decide

theorem nullSlot : TaggedNull.NullSlot.Goal := by
  unfold TaggedNull.NullSlot.Goal
  native_decide

theorem sentinelCliff : TaggedNull.SentinelCliff.Goal := by
  unfold TaggedNull.SentinelCliff.Goal
  have hf : TaggedNull.codec.fields (pair 0 4294967295) = some (0, 4294967296) := by
    unfold TaggedNull.codec
    simp [pair, unsigned]
  change encode TaggedNull.codec (pair 0 4294967295) = none
  rw [encode, hf]
  unfold TaggedNull.codec
  dsimp

theorem nearbyNegative : ZigzagPair.NearbyNegative.Goal := by
  unfold ZigzagPair.NearbyNegative.Goal
  native_decide

theorem sameHigh : XorSide.SameHigh.Goal := by
  intro x y
  rw [encode, xorSide_fields]
  simp [XorSide.codec]
  have hxorbound := xor_lt_two_pow_iff_div_eq (x:=x) (y:=y) (n:=24) (by decide)
  constructor
  · intro h
    have hxorlt : x ^^^ y < 16777216 := of_decide_eq_true h.2
    exact ⟨of_decide_eq_true h.1, hxorbound.mp hxorlt⟩
  · intro h
    have hxorlt : x ^^^ y < 16777216 := hxorbound.mpr h.2
    exact ⟨decide_eq_true h.1, decide_eq_true hxorlt⟩


theorem equalKeys : XorSide.EqualKeys.Goal := by
  intro x hx
  rw [encode, xorSide_fields]
  unfold XorSide.codec
  dsimp
  have hxlt : x < 1099511627776 := by omega
  have hxor : x ^^^ x = 0 := Nat.xor_self x
  rw [hxor]
  simp [hxlt]

theorem adjacentNegative : DeltaMagnitude.AdjacentNegative.Goal := by
  intro x hx0 hx52
  change x < 4503599627370496 at hx52
  unfold encode DeltaMagnitude.codec
  simp [pick, pair]
  have hxnon : ¬ x < 0 := by omega
  simp [unsigned, hxnon]
  have hd : x - 1 - x = (-1 : Int) := by omega
  rw [hd]
  unfold zig
  simp
  have hxc : (x.toNat : Int) = x := Int.toNat_of_nonneg hx0
  omega


theorem rangeAsymmetry : DeltaMagnitude.RangeAsymmetry.Goal := by
  unfold DeltaMagnitude.RangeAsymmetry.Goal
  native_decide

theorem rejectRemainder : ScaledKey.RejectRemainder.Goal := by
  intro x y h
  rw [encode]
  cases hf : ScaledKey.codec.fields (pair x y) with
  | none => simp [hf]
  | some p =>
    rcases p with ⟨a,b⟩
    simp [hf]
    intro ha
    unfold ScaledKey.codec at hf
    simp [pick, pair] at hf
    exact False.elim (h hf.1)



theorem wrongShard : ShardLocal.WrongShard.Goal := by
  intro x y h
  rw [encode]
  cases hf : ShardLocal.codec.fields (pair (x:Int) (y:Int)) with
  | none => simp [hf]
  | some p =>
    rcases p with ⟨a,b⟩
    simp [hf]
    intro ha
    unfold ShardLocal.codec at hf
    simp [pick, pair] at hf
    cases hux : unsigned (x:Int) with
    | none =>
      rw [hux] at hf
      simp at hf
    | some ux =>
      rw [hux] at hf
      simp at hf
      cases huy : unsigned (y:Int) with
      | none =>
        rw [huy] at hf
        simp at hf
      | some uy =>
        rw [huy] at hf
        simp at hf
        have hdiv' : ux / 65536 = uy / 65536 := hf.1
        have huxv : (x:Int).toNat = ux := by simpa [unsigned] using hux
        have huyv : (y:Int).toNat = uy := by simpa [unsigned] using huy
        have hxc : (x:Int).toNat = x := by simp
        have hyc : (y:Int).toNat = y := by simp
        rw [← huxv, ← huyv, hxc, hyc] at hdiv'
        exact False.elim (h hdiv')

theorem lastLocal : ShardLocal.LastLocal.Goal := by
  unfold ShardLocal.LastLocal.Goal
  native_decide

end TinylambOpt.ResearchProofs.JitFrontiers.PackedKeys.Specific
