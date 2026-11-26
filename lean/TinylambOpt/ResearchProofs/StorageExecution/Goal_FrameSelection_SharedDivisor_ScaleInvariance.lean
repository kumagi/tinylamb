import TinylambOpt.ResearchGoals.StorageExecution.FrameSelection
import TinylambOpt.ResearchProofs.Compression.Wire

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_FrameSelection_SharedDivisor_ScaleInvariance

open TinylambOpt.ResearchGoals.Compression.Wire
open TinylambOpt.ResearchGoals.StorageExecution
open TinylambOpt.ResearchGoals.StorageExecution.FrameSelection
open TinylambOpt.ResearchGoals.StorageExecution.FrameSelection.SharedDivisor

private theorem uleb_len (n : Nat) : (uleb n).length = ubytes n :=
  TinylambOpt.ResearchProofs.Compression.Wire.sound_VarintCharge n

/-- gcd distributes over a common factor, including the foldl accumulator. -/
private theorem foldl_gcd_map_mul (g : Nat) : ∀ (xs : List Nat) (acc : Nat),
    (xs.map (fun x => x * g)).foldl Nat.gcd (acc * g) = (xs.foldl Nat.gcd acc) * g := by
  intro xs
  induction xs with
  | nil =>
      intro acc
      simp
  | cons x xs ih =>
      intro acc
      simp only [List.map_cons, List.foldl_cons]
      rw [Nat.gcd_mul_right]
      rw [ih (Nat.gcd acc x)]

theorem sound_ScaleInvariance : SharedDivisor.ScaleInvariance.Goal := by
  intro xs g hxs hgcd hg
  have hscaled : (xs.map (fun x => x * g)).foldl Nat.gcd 0 = g := by
    have h := foldl_gcd_map_mul g xs 0
    rw [hgcd] at h
    simpa using h
  have hdivmap : (xs.map (fun x => x * g)).map (fun v => v / g) = xs := by
    rw [List.map_map]
    have hf : ((fun v => v / g) ∘ (fun x => x * g)) = id := by
      funext x
      exact Nat.mul_div_cancel x hg
    rw [hf]
    simp
  have hwrite_xs : write xs = uleb 1 ++ uleb (width (peak xs)) ++ pack (width (peak xs)) xs := by
    simp [write, hgcd]
  have hwrite_scaled : write (xs.map (fun x => x * g)) =
      uleb g ++ uleb (width (peak xs)) ++ pack (width (peak xs)) xs := by
    simp [write, hscaled, hdivmap]
  rw [hwrite_xs, hwrite_scaled]
  simp only [List.length_append]
  rw [uleb_len 1, uleb_len g, uleb_len (width (peak xs))]
  omega

/-! Truth was first confirmed by evaluation on small coprime inputs where
scaling by a positive factor leaves the framed byte count changed only by the
header varint. -/
private def check (xs : List Nat) (g : Nat) : Bool :=
  xs ≠ [] && xs.foldl Nat.gcd 0 == 1 && 0 < g &&
    (write (xs.map (fun x => x * g))).length + ubytes 1 == (write xs).length + ubytes g

#eval check [1] 1
#eval check [1] 7
#eval check [1, 2] 3
#eval check [0, 1] 5
#eval check [3, 4, 5] 2
#eval check [6, 10, 15] 11
#eval check [255, 256, 257] 3

end TinylambOpt.ResearchProofs.StorageExecution.Goal_FrameSelection_SharedDivisor_ScaleInvariance
