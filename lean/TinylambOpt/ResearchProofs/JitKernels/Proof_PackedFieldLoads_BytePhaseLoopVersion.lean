import TinylambOpt.ResearchGoals.JitKernels.PackedFieldLoads

/-!
**Proved: a repeating packing phase keeps its byte/shift specialization.**
When `8 ∣ period * w`, advancing the lane index by `period` leaves the intra-byte
bit phase `i * w % 8` unchanged and moves the start byte by exactly
`period * w / 8`: the group's bit stride is a whole number of bytes. SQL
execution may therefore hoist per-field shift amounts and byte strides out of
the inner loop and reuse one specialized decode kernel for every lane of a
`period`-lane group, producing identical results.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false
set_option linter.unusedSimpArgs false
set_option linter.deprecated false

namespace TinylambOpt.ResearchProofs.JitKernels.PackedFieldLoads_BytePhaseLoopVersion
open TinylambOpt.ResearchGoals.JitKernels.PackedFieldLoads
open TinylambOpt.ResearchGoals.Compression.Wire

theorem sound : BytePhaseLoopVersion.Goal := by
  intro w i period hdvd
  obtain ⟨q, hq⟩ := hdvd
  change ((i + period) * w % 8, w * (i + period) / 8) =
    (i * w % 8, w * i / 8 + period * w / 8)
  have h1 : (i + period) * w = i * w + 8 * q := by
    rw [Nat.add_mul, hq]
  have h2 : w * (i + period) = w * i + 8 * q := by
    rw [Nat.mul_add, ← hq, Nat.mul_comm w period]
  have h3 : period * w / 8 = q := by
    rw [hq, Nat.mul_div_cancel_left _ (by decide : (0 : Nat) < 8)]
  have h4 : (i * w + 8 * q) % 8 = i * w % 8 := Nat.add_mul_mod_self_left _ _ q
  have h5 : (w * i + 8 * q) / 8 = w * i / 8 + q :=
    Nat.add_mul_div_left _ _ (by decide : (0 : Nat) < 8)
  rw [h1, h2, h3, Prod.mk.injEq, h4, h5]
  omega

end TinylambOpt.ResearchProofs.JitKernels.PackedFieldLoads_BytePhaseLoopVersion
