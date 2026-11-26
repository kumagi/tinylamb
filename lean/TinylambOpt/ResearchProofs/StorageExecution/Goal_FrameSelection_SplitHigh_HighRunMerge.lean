import TinylambOpt.ResearchGoals.StorageExecution.FrameSelection
import TinylambOpt.ResearchProofs.Compression.Wire

/-!
A scan can skip or partially decode compressed frames while preserving selected values
and their positions.

Proved

- `SplitHigh.HighRunMerge` — `∀ x y n m, x/256=y/256 → 0<n → 0<m → (write
  (List.replicate n x ++ List.replicate m y)).length = n+m+ubytes (x/256)+ubytes
  (n+m)`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_FrameSelection_SplitHigh_HighRunMerge

open TinylambOpt.ResearchGoals.Compression.Wire
open TinylambOpt.ResearchGoals.StorageExecution.FrameSelection
open TinylambOpt.ResearchGoals.StorageExecution.FrameSelection.SplitHigh

-- Truth check on small inputs satisfying the hypotheses (equal high bytes, n,m>0).
#guard (write (List.replicate 1 256 ++ List.replicate 1 257)).length == 1 + 1 + ubytes 1 + ubytes 2
#guard (write (List.replicate 3 257 ++ List.replicate 5 300)).length == 3 + 5 + ubytes 1 + ubytes 8
#guard (write (List.replicate 200 (128*256+7) ++ List.replicate 100 (128*256+255))).length ==
  200 + 100 + ubytes 128 + ubytes 300

private theorem uleb_len (n : Nat) : (uleb n).length = ubytes n :=
  TinylambOpt.ResearchProofs.Compression.Wire.sound_VarintCharge n

private theorem runs_replicate_succ (v : Nat) (n : Nat) :
    runs (List.replicate (n + 1) v) = [(v, n + 1)] := by
  induction n with
  | zero => simp [runs, List.replicate]
  | succ n ih =>
      change runs (v :: List.replicate (n + 1) v) = [(v, Nat.succ n + 1)]
      unfold runs
      rw [ih]
      simp

private theorem runs_replicate_pos (v : Nat) {n : Nat} (hn : 0 < n) :
    runs (List.replicate n v) = [(v, n)] := by
  obtain ⟨k, hk⟩ := Nat.exists_eq_succ_of_ne_zero (Nat.ne_of_gt hn)
  subst n
  simpa [Nat.succ_eq_add_one] using runs_replicate_succ v k

theorem sound_HighRunMerge : SplitHigh.HighRunMerge.Goal := by
  intro x y n m hxy hn hm
  have hpos : 0 < n + m := by omega
  have hy : y / 256 = x / 256 := hxy.symm
  have hruns : runs (List.replicate n (x / 256) ++ List.replicate m (y / 256)) =
      [(x / 256, n + m)] := by
    rw [hy, List.replicate_append_replicate]
    exact runs_replicate_pos (x / 256) hpos
  simp [write, hruns, uleb_len]
  omega

end TinylambOpt.ResearchProofs.StorageExecution.Goal_FrameSelection_SplitHigh_HighRunMerge
