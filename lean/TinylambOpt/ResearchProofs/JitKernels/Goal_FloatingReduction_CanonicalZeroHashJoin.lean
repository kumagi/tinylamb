import Std
import TinylambOpt.ResearchGoals.JitKernels.FloatingReduction
import TinylambOpt.ResearchProofs.JitKernels.Common_FloatingReduction_Bits

/-! IEEE equality identifies `+0.0` and `-0.0`, but hash joins must use a
single bit pattern for that equivalence class. Replacing any zero operand by
positive zero before hashing is sound: the only non-injective case of IEEE
equality is exactly the pair of signed zeros, and every other IEEE-equal pair
of non-NaN floats decodes to the same finite or infinite value. -/
set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.JitKernels.Goal_FloatingReduction_CanonicalZeroHashJoin

open TinylambOpt.ResearchGoals.JitKernels.FloatingReduction
open TinylambOpt.ResearchProofs.JitKernels.FloatingReductionBits
open Float.Model
open Float.Model.UnpackedFloat

private theorem zero_beq_zero {x : Float} (s : Sign) (hx : x.toModel.unpack = .zero s) : x == 0 := by
  change Float.Model.beq x.toModel (0 : Float).toModel = true
  unfold Float.Model.beq
  rw [hx]
  have h0 : (0 : Float).toModel.unpack = .zero .positive := by rfl
  rw [h0]
  simp [UnpackedFloat.beq, UnpackedFloat.compare]

private theorem inf_not_beq_zero {x : Float} (s : Sign) (hx : x.toModel.unpack = .infinity s) : ¬ x == 0 := by
  intro h
  change Float.Model.beq x.toModel (0 : Float).toModel = true at h
  unfold Float.Model.beq at h
  rw [hx] at h
  have h0 : (0 : Float).toModel.unpack = .zero .positive := by rfl
  rw [h0] at h
  cases s <;> simp [UnpackedFloat.beq, UnpackedFloat.compare] at h

private theorem finite_not_beq_zero {x : Float} (s : Sign) (m : Nat) (e : Int) (hm : 0 < m)
    (hx : x.toModel.unpack = .finite s m e hm) : ¬ x == 0 := by
  intro h
  change Float.Model.beq x.toModel (0 : Float).toModel = true at h
  unfold Float.Model.beq at h
  rw [hx] at h
  have h0 : (0 : Float).toModel.unpack = .zero .positive := by rfl
  rw [h0] at h
  cases s <;> simp [UnpackedFloat.beq, UnpackedFloat.compare] at h

theorem sound : CanonicalZeroHashJoin.Goal := by
  intro a b hab
  change Float.Model.beq a.toModel b.toModel = true at hab
  unfold Float.Model.beq at hab
  cases ha : a.toModel.unpack with
  | notANumber =>
      rw [ha] at hab
      cases hb : b.toModel.unpack <;> simp [UnpackedFloat.beq, UnpackedFloat.compare] at hab
  | infinity s =>
      rw [ha] at hab
      have hb : b.toModel.unpack = .infinity s := by
        cases hbx : b.toModel.unpack with
        | infinity s2 =>
            rw [hbx] at hab
            cases s <;> cases s2 <;> simp [UnpackedFloat.beq, UnpackedFloat.compare] at hab ⊢
            all_goals rfl
        | notANumber => rw [hbx] at hab; simp [UnpackedFloat.beq, UnpackedFloat.compare] at hab
        | zero s2 => rw [hbx] at hab; cases s <;> simp [UnpackedFloat.beq, UnpackedFloat.compare] at hab
        | finite s2 m e hm =>
            rw [hbx] at hab
            cases s <;> cases s2 <;> simp [UnpackedFloat.beq, UnpackedFloat.compare] at hab
      simp [inf_not_beq_zero s ha, inf_not_beq_zero s hb]
      apply unpack_inj
      rw [ha, hb]
  | zero s =>
      rw [ha] at hab
      have hb : ∃ s2, b.toModel.unpack = .zero s2 := by
        cases hbx : b.toModel.unpack with
        | infinity s2 => rw [hbx] at hab; cases s <;> cases s2 <;> simp [UnpackedFloat.beq, UnpackedFloat.compare] at hab
        | notANumber => rw [hbx] at hab; simp [UnpackedFloat.beq, UnpackedFloat.compare] at hab
        | zero s2 => exact ⟨s2, rfl⟩
        | finite s2 m e hm => rw [hbx] at hab; cases s <;> cases s2 <;> simp [UnpackedFloat.beq, UnpackedFloat.compare] at hab
      obtain ⟨s2, hb⟩ := hb
      simp [zero_beq_zero s ha, zero_beq_zero s2 hb]
  | finite s m e hm =>
      rw [ha] at hab
      have hb : b.toModel.unpack = .finite s m e hm := by
        cases hbx : b.toModel.unpack with
        | infinity s2 => rw [hbx] at hab; cases s <;> cases s2 <;> simp [UnpackedFloat.beq, UnpackedFloat.compare] at hab
        | notANumber => rw [hbx] at hab; simp [UnpackedFloat.beq, UnpackedFloat.compare] at hab
        | zero s2 => rw [hbx] at hab; cases s <;> simp [UnpackedFloat.beq, UnpackedFloat.compare] at hab
        | finite s2 m2 e2 hm2 =>
            rw [hbx] at hab
            cases s <;> cases s2 <;> simp [UnpackedFloat.beq, UnpackedFloat.compare] at hab
            all_goals rcases hab with ⟨rfl, rfl⟩; rfl
      simp [finite_not_beq_zero s m e hm ha, finite_not_beq_zero s m e hm hb]
      apply unpack_inj
      rw [ha, hb]

end TinylambOpt.ResearchProofs.JitKernels.Goal_FloatingReduction_CanonicalZeroHashJoin
