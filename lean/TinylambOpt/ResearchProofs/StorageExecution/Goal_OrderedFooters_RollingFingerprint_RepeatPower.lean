import TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters

/-!
A footer can guide sorted scans and key-range probes when its indexed summary is
consistent with the stored blocks.

Proved

- `RollingFingerprint.RepeatPower` — Evaluate grammar repetition by binary powers of
  the summary.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_OrderedFooters_RollingFingerprint_RepeatPower

open TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters
open TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters.RollingFingerprint

-- Binary powering of a fold is correct whenever `join` extends `reference` on
-- concatenated lists (i.e. whenever `stitch` holds).
private theorem repeatPower_of_stitch {σ : Type} (f : Fold σ) (h : stitch f) : repeatPower f := by
  intro xs n
  induction n using Nat.strongRecOn with
  | _ k ih =>
    unfold repeatState
    by_cases hk : k = 0
    · simp [hk]
    · simp only [hk, dite_false]
      have hdiv : k / 2 < k := Nat.div_lt_self (by omega) (by decide)
      have ih2 := ih (k / 2) hdiv
      let a := repeatState f (f.reference xs) (k / 2)
      have ha' : a = f.reference (List.replicate (k / 2) xs).flatten := by simpa [a] using ih2
      have hjoin : f.join a a = f.reference ((List.replicate (k / 2) xs).flatten ++ (List.replicate (k / 2) xs).flatten) := by
        rw [ha']
        exact h _ _
      have hrep2 : (List.replicate (k / 2) xs).flatten ++ (List.replicate (k / 2) xs).flatten =
          (List.replicate (k / 2 * 2) xs).flatten := by
        rw [← List.flatten_append]
        congr 1
        rw [show k / 2 * 2 = k / 2 + k / 2 by omega, ← List.replicate_append_replicate]
      by_cases he : k % 2 = 0
      · have hk2 : k / 2 * 2 = k := by omega
        have hcond : (k % 2 == 0) = true := by simp [he]
        simp only [hcond]
        change f.join a a = f.reference (List.replicate k xs).flatten
        rw [hjoin, hrep2, hk2]
      · have hodd : k % 2 = 1 := by omega
        have hk1 : k = k / 2 * 2 + 1 := by omega
        have hcond : (k % 2 == 0) = false := by simp [hodd]
        simp only [hcond]
        change f.join (f.join a a) (f.reference xs) = f.reference (List.replicate k xs).flatten
        rw [hjoin, hrep2, h]
        congr 1
        rw [hk1, show k / 2 * 2 + 1 = (k / 2 + k / 2) + 1 by omega, ← List.replicate_append_replicate,
            List.flatten_append, List.replicate_one, List.flatten_singleton]
        have heq : (k / 2 + k / 2 + 1) / 2 * 2 = k / 2 + k / 2 := by omega
        simp [heq]

-- Rolling fingerprint arithmetic. `M` is the fixed prime modulus and `B` the base.
private def M : Nat := 65521
private def B : Nat := 257
private def P (xs : List Nat) : Nat := B ^ xs.length % M
private def H (xs : List Nat) : Nat := xs.foldl (fun h x => (B * h + x) % M) 0

private theorem hpow_append (a b : List Nat) : P (a ++ b) = P a * P b % M := by
  simp [P, List.length_append, Nat.pow_add, Nat.mul_mod]

private theorem hval_snoc (xs : List Nat) (x : Nat) : H (xs ++ [x]) = (B * H xs + x) % M := by
  simp [H, List.foldl_append]

private theorem hpow_snoc (xs : List Nat) (x : Nat) : P (xs ++ [x]) = P xs * (B % M) % M := by
  simp [P, List.length_append, Nat.pow_add, Nat.pow_one, Nat.mul_mod, Nat.mul_comm]

private theorem hval_lt (xs : List Nat) : H xs < M := by
  unfold H
  suffices ∀ (l : List Nat) (acc : Nat), acc < M → List.foldl (fun h x => (B * h + x) % M) acc l < M from
    this xs 0 (by decide)
  intro l
  induction l with
  | nil => intro acc h; exact h
  | cons x xs ih => intro acc h; exact ih _ (Nat.mod_lt _ (by decide))

-- Congruence modulo the fixed modulus M, sufficient because all folded values
-- are already reduced below M.
private def MEq (a b : Nat) : Prop := a % M = b % M

private theorem MEq.symm {a b} (h : MEq a b) : MEq b a := Eq.symm h
private theorem MEq.trans {a b c} (h1 : MEq a b) (h2 : MEq b c) : MEq a c := Eq.trans h1 h2
private theorem MEq.of_mod (a : Nat) : MEq (a % M) a := by simp [MEq]
private theorem MEq.addRight {a b c} (h : MEq a b) : MEq (a + c) (b + c) := by
  unfold MEq at h ⊢
  rw [Nat.add_mod a c M, Nat.add_mod b c M, h]
private theorem MEq.addLeft {a b c} (h : MEq a b) : MEq (c + a) (c + b) := by
  unfold MEq at h ⊢
  rw [Nat.add_mod c a M, Nat.add_mod c b M, h]
private theorem MEq.mulLeft {a b c} (h : MEq a b) : MEq (c * a) (c * b) := by
  unfold MEq at h ⊢
  rw [Nat.mul_mod c a M, Nat.mul_mod c b M, h]
private theorem MEq.eq_of_lt {a b} (ha : a < M) (hb : b < M) (h : MEq a b) : a = b := by
  unfold MEq at h
  rw [Nat.mod_eq_of_lt ha, Nat.mod_eq_of_lt hb] at h
  exact h

private theorem dist_eq (b a p y x : Nat) : b * (a * p + y) + x = a * p * b + (b * y + x) := by
  rw [Nat.mul_add]
  ac_rfl

private theorem mod_arith (b a p y x : Nat) :
    (b * ((a * p % M + y) % M) + x) % M = (a * (p * b % M) % M + (b * y + x) % M) % M := by
  have hM : 0 < M := by decide
  apply MEq.eq_of_lt (Nat.mod_lt _ hM) (Nat.mod_lt _ hM)
  have hL : MEq ((b * ((a * p % M + y) % M) + x) % M) (a * p * b + (b * y + x)) := by
    apply MEq.trans (MEq.of_mod (b * ((a * p % M + y) % M) + x))
    apply MEq.trans (MEq.addRight (c := x) (MEq.mulLeft (c := b) (MEq.of_mod (a * p % M + y))))
    apply MEq.trans (MEq.addRight (c := x) (MEq.mulLeft (c := b) (MEq.addRight (c := y) (MEq.of_mod (a * p)))))
    rw [dist_eq]
    rfl
  have ht1 : MEq (a * (p * b % M) % M) (a * p * b) := by
    apply MEq.trans (MEq.of_mod (a * (p * b % M)))
    apply MEq.trans (MEq.mulLeft (c := a) (MEq.of_mod (p * b)))
    rw [Nat.mul_assoc]
    rfl
  have hR : MEq ((a * (p * b % M) % M + (b * y + x) % M) % M) (a * p * b + (b * y + x)) := by
    apply MEq.trans (MEq.of_mod (a * (p * b % M) % M + (b * y + x) % M))
    apply MEq.trans (MEq.addRight (c := (b * y + x) % M) ht1)
    exact MEq.addLeft (c := a * p * b) (MEq.of_mod (b * y + x))
  exact MEq.trans hL (MEq.symm hR)

private theorem hval_append (a b : List Nat) : H (a ++ b) = (H a * P b % M + H b) % M := by
  suffices hgen : ∀ r : List Nat, H (a ++ r.reverse) = (H a * P r.reverse % M + H r.reverse) % M from by
    simpa using hgen b.reverse
  intro r
  induction r with
  | nil =>
      simp [H, P]
      exact (Nat.mod_eq_of_lt (hval_lt a)).symm
  | cons y ys ih =>
      rw [List.reverse_cons, ← List.append_assoc, hval_snoc, ih, hval_snoc, hpow_snoc]
      simpa [B, M] using (mod_arith B (H a) (P ys.reverse) (H ys.reverse) y)

private theorem stitch_agg : stitch agg := by
  intro a b
  change (P a * P b % M, (H a * P b + H b) % M) = (P (a ++ b), H (a ++ b))
  apply Prod.ext
  · exact (hpow_append a b).symm
  · rw [hval_append]
    rw [Nat.add_mod (H a * P b) (H b) M, Nat.mod_eq_of_lt (hval_lt b)]

theorem sound_RepeatPower : RollingFingerprint.RepeatPower.Goal := repeatPower_of_stitch agg stitch_agg

end TinylambOpt.ResearchProofs.StorageExecution.Goal_OrderedFooters_RollingFingerprint_RepeatPower
