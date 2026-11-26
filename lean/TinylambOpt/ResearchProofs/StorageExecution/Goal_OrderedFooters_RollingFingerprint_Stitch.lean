import TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters

/-!
A footer can guide sorted scans and key-range probes when its indexed summary is
consistent with the stored blocks.

Proved

- `RollingFingerprint.Stitch` — Compute a page boundary correction using only the two
  serialized states.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_OrderedFooters_RollingFingerprint_Stitch

open TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters
open TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters.RollingFingerprint

private def modulus : Nat := 65521
private def fpPow (xs : List Nat) : Nat := 257 ^ xs.length % modulus
private def fpHash (xs : List Nat) : Nat := xs.foldl (fun h x => (257 * h + x) % modulus) 0
private def rawHash (xs : List Nat) : Nat := xs.foldl (fun h x => 257 * h + x) 0

private theorem raw_fold (v : Nat) (xs : List Nat) :
    xs.foldl (fun h x => 257 * h + x) v = v * 257 ^ xs.length + rawHash xs := by
  induction xs generalizing v with
  | nil => simp [rawHash]
  | cons x xs ih =>
      simp only [List.foldl_cons]
      rw [ih]
      have hraw : rawHash (x :: xs) = x * 257 ^ xs.length + rawHash xs := by
        simpa [rawHash] using ih x
      rw [hraw]
      simp [Nat.pow_succ, Nat.add_mul, Nat.mul_assoc, Nat.mul_comm,
        Nat.add_assoc, Nat.add_comm, Nat.add_left_comm]

private theorem raw_append (a b : List Nat) :
    rawHash (a ++ b) = rawHash a * 257 ^ b.length + rawHash b := by
  rw [show rawHash (a ++ b) = b.foldl (fun h x => 257 * h + x) (rawHash a) by
    simp [rawHash, List.foldl_append]]
  exact raw_fold (rawHash a) b

private theorem fold_mod (v : Nat) (hv : v < modulus) (xs : List Nat) :
    xs.foldl (fun h x => (257 * h + x) % modulus) v =
      xs.foldl (fun h x => 257 * h + x) v % modulus := by
  induction xs generalizing v with
  | nil => simp [Nat.mod_eq_of_lt hv]
  | cons x xs ih =>
      simp only [List.foldl_cons]
      have hv' : (257 * v + x) % modulus < modulus := Nat.mod_lt _ (by decide)
      rw [ih ((257 * v + x) % modulus) hv']
      rw [raw_fold, raw_fold]
      simp only [Nat.add_mod, Nat.mul_mod, Nat.mod_mod]

private theorem fpHash_eq_raw_mod (xs : List Nat) : fpHash xs = rawHash xs % modulus := by
  simpa [fpHash, rawHash] using fold_mod 0 (by decide : 0 < modulus) xs

private theorem fpPow_append (a b : List Nat) : fpPow (a ++ b) = fpPow a * fpPow b % modulus := by
  simp [fpPow, List.length_append, Nat.pow_add, Nat.mul_mod]

private theorem fpHash_append (a b : List Nat) :
    fpHash (a ++ b) = (fpHash a * fpPow b + fpHash b) % modulus := by
  rw [fpHash_eq_raw_mod, fpHash_eq_raw_mod, fpHash_eq_raw_mod, fpPow, raw_append]
  simp only [Nat.add_mod, Nat.mul_mod, Nat.mod_mod]

private theorem stitch_agg : stitch agg := by
  intro a b
  simp only [agg]
  change (fpPow a * fpPow b % modulus, (fpHash a * fpPow b + fpHash b) % modulus) =
    (fpPow (a ++ b), fpHash (a ++ b))
  exact Prod.ext (fpPow_append a b).symm (fpHash_append a b).symm

theorem sound_Stitch : Stitch.Goal := stitch_agg

end TinylambOpt.ResearchProofs.StorageExecution.Goal_OrderedFooters_RollingFingerprint_Stitch
