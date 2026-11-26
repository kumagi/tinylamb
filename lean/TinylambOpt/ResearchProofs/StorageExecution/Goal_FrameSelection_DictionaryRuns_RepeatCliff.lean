import TinylambOpt.ResearchGoals.StorageExecution.FrameSelection
import TinylambOpt.ResearchProofs.Compression.Wire

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_FrameSelection_DictionaryRuns_RepeatCliff

open TinylambOpt.ResearchGoals.Compression.Wire
open TinylambOpt.ResearchGoals.StorageExecution.FrameSelection
open TinylambOpt.ResearchGoals.StorageExecution.FrameSelection.DictionaryRuns

-- Truth check on small inputs (including a large value and ULEB length boundaries).
#guard (write (List.replicate 1 0)).length == 6 + ubytes 1
#guard (write (List.replicate 3 123456)).length == 6 + ubytes 3
#guard (write (List.replicate 200 7)).length == 6 + ubytes 200
#guard (write (List.replicate 20000 (2^40 : Nat))).length == 6 + ubytes 20000

private theorem uleb_len_eq_ubytes (n : Nat) : (uleb n).length = ubytes n :=
  TinylambOpt.ResearchProofs.Compression.Wire.sound_VarintCharge n

private theorem little_length (n k : Nat) : (little n k).length = k := by
  simp [little]

private theorem eraseDups_replicate_succ (x : Nat) (n : Nat) :
    (List.replicate (n + 1) x).eraseDups = [x] := by
  induction n with
  | zero => simp [List.eraseDups_cons]
  | succ n ih => simp [List.replicate, List.eraseDups_cons]

private theorem runs_replicate_succ (v : Nat) (n : Nat) :
    runs (List.replicate (n + 1) v) = [(v, n + 1)] := by
  induction n with
  | zero => simp [runs, List.replicate]
  | succ n ih =>
      change runs (v :: List.replicate (n + 1) v) = [(v, Nat.succ n + 1)]
      unfold runs
      rw [ih]
      simp

theorem sound_RepeatCliff : RepeatCliff.Goal := by
  intro x n hn
  cases n with
  | zero => omega
  | succ n =>
      have hd : (List.replicate (n + 1) x).eraseDups = [x] := eraseDups_replicate_succ x n
      have hr : runs (List.replicate (n + 1) 0) = [(0, n + 1)] := runs_replicate_succ 0 n
      simp [DictionaryRuns.write, hd, hr, uleb_len_eq_ubytes, fixed, little_length]
      simp [ubytes, width]
      omega

end TinylambOpt.ResearchProofs.StorageExecution.Goal_FrameSelection_DictionaryRuns_RepeatCliff
