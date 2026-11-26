import TinylambOpt.ResearchGoals.StorageExecution.FrameSelection
import TinylambOpt.ResearchProofs.Compression.Wire

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_FrameSelection_DictionaryRuns_DictionaryBoundary

open TinylambOpt.ResearchGoals.Compression.Wire
open TinylambOpt.ResearchGoals.StorageExecution.FrameSelection
open TinylambOpt.ResearchGoals.StorageExecution.FrameSelection.DictionaryRuns

-- Truth checks on small inputs and on ULEB / fixed-width boundaries.
#guard (write (List.replicate 3 7 ++ List.replicate 5 9)).length == 11 + ubytes 3 + ubytes 5
#guard (write (List.replicate 1 0 ++ List.replicate 200 123456)).length == 11 + ubytes 1 + ubytes 200
#guard (write (List.replicate 5 123456 ++ List.replicate 1 0)).length == 11 + ubytes 5 + ubytes 1
#guard (write (List.replicate 20000 (2^40 : Nat) ++ List.replicate 3 0)).length ==
  11 + ubytes 20000 + ubytes 3

private theorem uleb_len_eq_ubytes (n : Nat) : (uleb n).length = ubytes n :=
  TinylambOpt.ResearchProofs.Compression.Wire.sound_VarintCharge n

private theorem little_length (n k : Nat) : (little n k).length = k := by
  simp [little]

private theorem eraseDups_replicate_succ (x : Nat) (n : Nat) :
    (List.replicate (n + 1) x).eraseDups = [x] := by
  induction n with
  | zero => simp [List.eraseDups_cons]
  | succ n ih => simp [List.replicate, List.eraseDups_cons]

private theorem eraseDups_replicate_pos (x : Nat) (m : Nat) (hm : 0 < m) :
    (List.replicate m x).eraseDups = [x] := by
  cases m with
  | zero => omega
  | succ k =>
      change (List.replicate (k + 1) x).eraseDups = [x]
      exact eraseDups_replicate_succ x k

private theorem filter_replicate_ne (a b : Nat) (h : b ≠ a) :
    ∀ k, (List.replicate k b).filter (fun x => ! x == a) = List.replicate k b := by
  intro k
  induction k with
  | zero => simp
  | succ k ih =>
      rw [List.replicate_succ, List.filter_cons]
      simp [h, ih]

private theorem eraseDups_two_runs (a b : Nat) (h : a ≠ b) :
    ∀ (n m : Nat), 0 < n → 0 < m →
      (List.replicate n a ++ List.replicate m b).eraseDups = [a, b] := by
  intro n m hn hm
  cases n with
  | zero => omega
  | succ n =>
      rw [List.replicate_succ, List.cons_append, List.eraseDups_cons]
      rw [List.filter_append]
      have hfa : (List.replicate n a).filter (fun x => ! x == a) = [] := by
        simp
      rw [hfa]
      simp
      rw [filter_replicate_ne a b (Ne.symm h)]
      exact eraseDups_replicate_pos b m hm

private theorem runs_replicate_succ (v : Nat) (n : Nat) :
    runs (List.replicate (n + 1) v) = [(v, n + 1)] := by
  induction n with
  | zero => simp [runs, List.replicate]
  | succ n ih =>
      change runs (v :: List.replicate (n + 1) v) = [(v, Nat.succ n + 1)]
      unfold runs
      rw [ih]
      simp

private theorem runs_replicate_pos (v : Nat) (m : Nat) (hm : 0 < m) :
    runs (List.replicate m v) = [(v, m)] := by
  cases m with
  | zero => omega
  | succ k =>
      change runs (List.replicate (k + 1) v) = [(v, k + 1)]
      exact runs_replicate_succ v k

private theorem runs_two_runs (v w : Nat) (h : v ≠ w) :
    ∀ (n m : Nat), 0 < n → 0 < m →
      runs (List.replicate n v ++ List.replicate m w) = [(v, n), (w, m)] := by
  intro n m hn hm
  cases n with
  | zero => omega
  | succ n =>
      induction n with
      | zero =>
          rw [List.replicate_succ, List.cons_append]
          change runs (v :: List.replicate m w) = [(v, 1), (w, m)]
          unfold runs
          rw [runs_replicate_pos w m hm]
          have h' : w ≠ v := Ne.symm h
          simp [h']
      | succ n ih =>
          rw [List.replicate_succ, List.cons_append]
          change runs (v :: (List.replicate (n + 1) v ++ List.replicate m w)) =
            [(v, Nat.succ n + 1), (w, m)]
          unfold runs
          rw [ih (by omega)]
          simp

theorem sound_DictionaryBoundary : DictionaryBoundary.Goal := by
  intro a b hab n m hn hm
  have hd : (List.replicate n a ++ List.replicate m b).eraseDups = [a, b] :=
    eraseDups_two_runs a b hab n m hn hm
  have hidx : (List.replicate n a ++ List.replicate m b).map [a, b].idxOf =
      List.replicate n 0 ++ List.replicate m 1 := by
    rw [List.map_append]
    simp [List.idxOf_cons, hab]
  have hr : runs (List.replicate n 0 ++ List.replicate m 1) = [(0, n), (1, m)] :=
    runs_two_runs 0 1 (by omega) n m hn hm
  simp [DictionaryRuns.write, hd, hidx, hr, fixed, little_length, uleb_len_eq_ubytes]
  have h0 : ubytes 0 = 1 := by decide
  have h1 : ubytes 1 = 1 := by decide
  have h2 : ubytes 2 = 1 := by decide
  rw [h0, h1, h2]
  omega

end TinylambOpt.ResearchProofs.StorageExecution.Goal_FrameSelection_DictionaryRuns_DictionaryBoundary
