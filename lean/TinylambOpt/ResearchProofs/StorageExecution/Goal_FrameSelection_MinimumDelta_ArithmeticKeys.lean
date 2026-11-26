import TinylambOpt.ResearchGoals.StorageExecution.FrameSelection
import TinylambOpt.ResearchProofs.Compression.Wire

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_FrameSelection_MinimumDelta_ArithmeticKeys

open TinylambOpt.ResearchGoals.StorageExecution.FrameSelection
open TinylambOpt.ResearchGoals.StorageExecution.FrameSelection.MinimumDelta
open TinylambOpt.ResearchGoals.Compression.Wire

private theorem uleb_len (n : Nat) : (uleb n).length = ubytes n :=
  TinylambOpt.ResearchProofs.Compression.Wire.sound_VarintCharge n

/-- Along an arithmetic progression with step `d`, every predecessor residual
    is the same zigzag value `2*d`; `p` tracks the previous lane's multiplier. -/
private theorem delta_arith (d p : Nat) : ∀ n : Nat,
    delta (p * d) ((List.range n).map (fun i => (i + 1 + p) * d)) =
      List.replicate n (2 * d)
  | 0 => rfl
  | n + 1 => by
      rw [List.range_succ_eq_map, List.map_cons, List.map_map]
      change delta (p * d) ((0 + 1 + p) * d ::
          (List.range n).map (fun i => (i.succ + 1 + p) * d)) =
        List.replicate (n + 1) (2 * d)
      have hp : 0 + 1 + p = p + 1 := by omega
      rw [hp, delta]
      have hmul : (p + 1) * d = p * d + d := by simp [Nat.add_mul]
      have hsub : ((((p + 1) * d : Nat) : Int) - ((p * d : Nat) : Int)) = (d : Int) := by
        rw [hmul, Int.natCast_add]
        omega
      have hz : zig ((((p + 1) * d : Nat) : Int) - ((p * d : Nat) : Int)) = 2 * d := by
        rw [hsub]
        simp [zig]
      rw [hz, List.replicate_succ]
      congr 1
      simpa [Function.comp_def, Nat.succ_eq_add_one, Nat.add_assoc, Nat.add_comm, Nat.add_left_comm] using
        (delta_arith d (p + 1) n)

/-- All residuals vanish after subtracting the minimum, so the bit-packed body
    is empty and only the ULEB minimum plus the zero-width tag remain. -/
theorem sound_ArithmeticKeys : MinimumDelta.ArithmeticKeys.Goal := by
  intro d n hn
  have hds : delta 0 ((List.range n).map (fun i => (i + 1) * d)) =
      List.replicate n (2 * d) := by
    simpa using delta_arith d 0 n
  have hmin : (List.replicate n (2 * d)).min?.getD 0 = 2 * d := by
    rw [List.min?_replicate]
    simp [Nat.ne_of_gt hn]
  have hpeak : peak (List.replicate n 0) = 0 := by
    simp [peak, List.max?_replicate, Nat.ne_of_gt hn]
  have hpack : pack 0 (List.replicate n 0) = [] := by
    simp [pack, byteCount, little]
  rw [show write ((List.range n).map (fun i => (i + 1) * d)) =
      uleb (2 * d) ++ uleb 0 ++ pack 0 (List.replicate n 0) by
    simp [write, hds, hmin, hpeak, width]]
  rw [hpack]
  simp [uleb_len, ubytes, width]

end TinylambOpt.ResearchProofs.StorageExecution.Goal_FrameSelection_MinimumDelta_ArithmeticKeys
