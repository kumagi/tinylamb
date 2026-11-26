import TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters

/-!
A footer can guide sorted scans and key-range probes when its indexed summary is
consistent with the stored blocks.

Proved

- `RollingFingerprint.EqualWindowResidues` — `∀ a b pre z, a.length=b.length →
  (agg.reference a).2=(agg.reference b).2 → agg.reference (pre++a++z)=agg.reference
  (pre++b++z)`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_OrderedFooters_RollingFingerprint_EqualWindowResidues

open TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters
open TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters.RollingFingerprint

private def M : Nat := 65521
private def B : Nat := 257
private def f (h x : Nat) : Nat := (B * h + x) % M
private def hpow (xs : List Nat) : Nat := B ^ xs.length % M
private def hval (xs : List Nat) : Nat := xs.foldl f 0

private theorem hpow_lt (xs : List Nat) : hpow xs < M := Nat.mod_lt _ (by decide)

private theorem hval_lt (xs : List Nat) : hval xs < M := by
  unfold hval
  suffices ∀ (l : List Nat) (acc : Nat), acc < M → List.foldl f acc l < M from
    this xs 0 (by decide)
  intro l
  induction l with
  | nil => intro acc h; exact h
  | cons x xs ih => intro acc h; exact ih _ (Nat.mod_lt _ (by decide))

private theorem hpow_succ (xs : List Nat) (x : Nat) : hpow (x :: xs) = (B * hpow xs) % M := by
  simp [hpow, Nat.pow_succ, Nat.mul_mod, Nat.mul_comm]

private theorem hval_cons (xs : List Nat) (x : Nat) : hval (x :: xs) = xs.foldl f (x % M) := by
  simp [hval, f]

private theorem mod_add_mod (a b : Nat) (hb : b < M) : (a % M + b) % M = (a + b) % M := by
  calc
    (a % M + b) % M = (a % M + b % M) % M := by rw [Nat.mod_eq_of_lt hb]
    _ = (a + b) % M := by rw [← Nat.add_mod]

private theorem mul_mod_eq (a b : Nat) (hb : b < M) : (a % M * b) % M = (a * b) % M := by
  calc
    (a % M * b) % M = (a % M * (b % M)) % M := by rw [Nat.mod_eq_of_lt hb]
    _ = (a * b) % M := by rw [← Nat.mul_mod]

-- Folding with an arbitrary residue `init` prepends the polynomial weight of `init`.
private theorem hval_fold (init : Nat) (hinit : init < M) (xs : List Nat) :
    xs.foldl f init = (init * hpow xs % M + hval xs) % M := by
  induction xs generalizing init with
  | nil =>
      simp [hpow, hval, Nat.mod_eq_of_lt hinit]
  | cons x xs ih =>
      simp only [List.foldl_cons, f]
      rw [ih ((B * init + x) % M) (Nat.mod_lt _ (by decide))]
      rw [hpow_succ, hval_cons, ih (x % M) (Nat.mod_lt _ (by decide))]
      let r := hpow xs
      let hv := hval xs
      have hr : r < M := hpow_lt xs
      have hhv : hv < M := hval_lt xs
      change (((B * init + x) % M * r % M + hv) % M) =
        (init * (B * r % M) % M + ((x % M * r % M + hv) % M)) % M
      have hx : (B * init + x) % M = (B * init + x % M) % M := by
        calc
          (B * init + x) % M = (B * init % M + x % M) % M := Nat.add_mod (B * init) x M
          _ = (B * init % M + (x % M) % M) % M := by rw [Nat.mod_mod]
          _ = (B * init + x % M) % M := (Nat.add_mod (B * init) (x % M) M).symm
      have hBR : init * (B * r % M) % M = B * init * r % M := by
        calc
          init * (B * r % M) % M = (B * r) % M * init % M := by rw [Nat.mul_comm init ((B * r) % M)]
          _ = ((B * r) * init) % M := by rw [mul_mod_eq (B * r) init hinit]
          _ = B * init * r % M := by rw [Nat.mul_assoc, Nat.mul_comm r init, ← Nat.mul_assoc]
      have hL : (((B * init + x % M) % M * r % M + hv) % M) =
          ((B * init * r + x % M * r + hv) % M) := by
        calc
          (((B * init + x % M) % M * r % M + hv) % M)
              = (((B * init + x % M) * r) % M + hv) % M := by
                  rw [mul_mod_eq (B * init + x % M) r hr]
          _ = (((B * init + x % M) * r) % M + hv % M) % M := by rw [Nat.mod_eq_of_lt hhv]
          _ = (((B * init + x % M) * r + hv) % M) := by rw [← Nat.add_mod]
          _ = ((B * init * r + x % M * r + hv) % M) := by rw [Nat.add_mul]
      have hR : (init * (B * r % M) % M + ((x % M * r % M + hv) % M)) % M =
          ((B * init * r + x % M * r + hv) % M) := by
        calc
          (init * (B * r % M) % M + ((x % M * r % M + hv) % M)) % M
              = (B * init * r % M + ((x % M * r % M + hv) % M)) % M := by rw [hBR]
          _ = (B * init * r % M + (x % M * r + hv) % M) % M := by
                rw [mod_add_mod (x % M * r) hv hhv]
          _ = ((B * init * r + (x % M * r + hv)) % M) := by rw [← Nat.add_mod]
          _ = ((B * init * r + x % M * r + hv) % M) := by rw [Nat.add_assoc]
      rw [hx]
      exact hL.trans hR.symm

private theorem hval_append (a b : List Nat) : hval (a ++ b) = (hval a * hpow b % M + hval b) % M := by
  unfold hval
  rw [List.foldl_append]
  exact hval_fold (List.foldl f 0 a) (hval_lt a) b

private theorem equal_window_residues : EqualWindowResidues.Goal := by
  intro a b pre z hlen hlen_hash
  change hval a = hval b at hlen_hash
  have hpab : hpow a = hpow b := by
    simp [hpow, hlen]
  have hv_window : hval (pre ++ a ++ z) = hval (pre ++ b ++ z) := by
    simp only [hval_append]
    rw [hpab, hlen_hash]
  have hp_window : hpow (pre ++ a ++ z) = hpow (pre ++ b ++ z) := by
    simp [hpow, List.length_append, hlen]
  change (hpow (pre ++ a ++ z), hval (pre ++ a ++ z)) =
    (hpow (pre ++ b ++ z), hval (pre ++ b ++ z))
  exact Prod.ext hp_window hv_window

theorem sound_EqualWindowResidues : EqualWindowResidues.Goal := equal_window_residues

end TinylambOpt.ResearchProofs.StorageExecution.Goal_OrderedFooters_RollingFingerprint_EqualWindowResidues
