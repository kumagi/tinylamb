import TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_OrderedFooters_RollingFingerprint_PrefixCheckpoint

open TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters
open TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters.RollingFingerprint

private def M : Nat := 65521
private def B : Nat := 257
private def hpow (xs : List Nat) : Nat := B ^ xs.length % M
private def hval (xs : List Nat) : Nat := xs.foldl (fun h x => (B * h + x) % M) 0

private theorem hpow_append (a b : List Nat) : hpow (a ++ b) = hpow a * hpow b % M := by
  simp [hpow, List.length_append, Nat.pow_add, Nat.mul_mod]

private theorem hval_snoc (xs : List Nat) (x : Nat) : hval (xs ++ [x]) = (B * hval xs + x) % M := by
  simp [hval, List.foldl_append]

private theorem hpow_snoc (xs : List Nat) (x : Nat) : hpow (xs ++ [x]) = hpow xs * B % M := by
  simp [hpow, List.length_append, Nat.pow_add, Nat.pow_one, Nat.mul_mod, Nat.mul_comm,
    Nat.mod_eq_of_lt (by decide : B < M)]

private theorem hval_lt (xs : List Nat) : hval xs < M := by
  unfold hval
  suffices ∀ (l : List Nat) (acc : Nat), acc < M → List.foldl (fun h x => (B * h + x) % M) acc l < M from
    this xs 0 (by decide)
  intro l
  induction l with
  | nil => intro acc h; exact h
  | cons x xs ih => intro acc h; exact ih _ (Nat.mod_lt _ (by decide))

/-- Induction from the right end of a list (no Mathlib `reverseRecOn` here). -/
private theorem snoc_induction {motive : List Nat → Prop}
    (h_nil : motive []) (h_snoc : ∀ xs x, motive xs → motive (xs ++ [x])) :
    ∀ xs, motive xs := by
  have hrev : ∀ ys : List Nat, motive ys.reverse := by
    intro ys
    induction ys with
    | nil => simpa using h_nil
    | cons a ys ih =>
        rw [List.reverse_cons]
        exact h_snoc ys.reverse a ih
  intro xs
  simpa using hrev xs.reverse

private theorem mod_outer_left (a c : Nat) : (a + c) % M = (a % M + c) % M :=
  (Nat.mod_add_mod a M c).symm

private theorem mod_outer_right (a c : Nat) : (a + c) % M = (a + c % M) % M :=
  (Nat.add_mod_mod a c M).symm

private theorem mod_mul_right_lemma (a b : Nat) : (a * (b % M)) % M = (a * b) % M := by
  calc (a * (b % M)) % M = (a % M * ((b % M) % M)) % M := Nat.mul_mod a (b % M) M
    _ = (a % M * (b % M)) % M := congrArg (fun t => (a % M * t) % M) (Nat.mod_mod b M)
    _ = (a * b) % M := (Nat.mul_mod a b M).symm

private theorem mod_add_congr_left {a b c : Nat} (h : a % M = b % M) : (a + c) % M = (b + c) % M := by
  calc (a + c) % M = (a % M + c) % M := mod_outer_left a c
    _ = (b % M + c) % M := by rw [h]
    _ = (b + c) % M := Nat.mod_add_mod b M c

private theorem finger_step (T Ps Hs x : Nat) :
    (B * ((T * Ps % M + Hs) % M) + x) % M =
      (T * (Ps * B % M) % M + (B * Hs + x) % M) % M := by
  let Y := T * Ps % M + Hs
  calc
    (B * (Y % M) + x) % M
        = ((B * (Y % M)) % M + x) % M := mod_outer_left _ _
    _ = ((B * Y) % M + x) % M := congrArg (fun t => (t + x) % M) (mod_mul_right_lemma B Y)
    _ = (B * Y + x) % M := Nat.mod_add_mod _ _ _
    _ = (B * (T * Ps % M) + B * Hs + x) % M := by
          exact congrArg (fun t => (t + x) % M) (Nat.mul_add B (T * Ps % M) Hs)
    _ = (B * (T * Ps % M) + (B * Hs + x)) % M := congrArg (fun t => t % M) (Nat.add_assoc _ _ _)
    _ = (T * (Ps * B) + (B * Hs + x)) % M := by
          apply mod_add_congr_left
          calc (B * (T * Ps % M)) % M = (B * (T * Ps)) % M := by
                 exact mod_mul_right_lemma B (T * Ps)
               _ = (T * (Ps * B)) % M := by congr 1; ac_rfl
    _ = (T * (Ps * B % M) + (B * Hs + x)) % M := by
          apply mod_add_congr_left
          exact (mod_mul_right_lemma T (Ps * B)).symm
    _ = (T * (Ps * B % M) + (B * Hs + x) % M) % M := mod_outer_right _ _
    _ = (T * (Ps * B % M) % M + (B * Hs + x) % M) % M := mod_outer_left _ _

private theorem hval_append (a b : List Nat) : hval (a ++ b) = (hval a * hpow b % M + hval b) % M := by
  refine snoc_induction (motive := fun bs => hval (a ++ bs) = (hval a * hpow bs % M + hval bs) % M) ?_ ?_ b
  · simp [hval, hpow]
    simpa [hval] using (Nat.mod_eq_of_lt (hval_lt a)).symm
  · intro bs x ih
    rw [← List.append_assoc, hval_snoc, ih, hval_snoc, hpow_snoc]
    exact finger_step (hval a) (hpow bs) (hval bs) x

private theorem hval_append_join (a b : List Nat) : (hval a * hpow b + hval b) % M = hval (a ++ b) := by
  rw [hval_append]
  simp only [Nat.add_mod, Nat.mod_mod, Nat.mod_eq_of_lt (hval_lt b)]

private theorem stitchAgg : stitch agg := by
  intro a b
  simp only [agg]
  exact Prod.ext (hpow_append a b).symm (hval_append_join a b)

theorem sound_PrefixCheckpoint : RollingFingerprint.PrefixCheckpoint.Goal := by
  intro init ps
  induction ps generalizing init with
  | nil => simp
  | cons p ps ih =>
      simp only [List.map_cons, List.scanl_cons]
      rw [stitchAgg init p]
      rw [ih (init ++ p)]
      simp [List.range_succ_eq_map, Function.comp_def, List.append_assoc]

end TinylambOpt.ResearchProofs.StorageExecution.Goal_OrderedFooters_RollingFingerprint_PrefixCheckpoint
