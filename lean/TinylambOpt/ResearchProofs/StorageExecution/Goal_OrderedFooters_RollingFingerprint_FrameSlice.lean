import TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters

/-!
A footer can guide sorted scans and key-range probes when its indexed summary is
consistent with the stored blocks.

Proved

- `RollingFingerprint.FrameSlice` — Use only the two ragged boundaries and whole-page
  summaries for a window.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_OrderedFooters_RollingFingerprint_FrameSlice

open TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters
open TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters.RollingFingerprint

/-! Ground check on small inputs (the probe must return `true`). -/

private def gen : Nat → List (List Nat)
  | 0 => [[]]
  | n + 1 => let p := gen n; p ++ p.flatMap (fun xs => [0 :: xs, 1 :: xs, 2 :: xs])

private def probe (n : Nat) : Bool :=
  let ls := gen n
  ls.all (fun a => ls.all (fun p => ls.all (fun z =>
    let ps := [p, p ++ z]
    (List.range (a.length + 1)).all (fun lo =>
      (List.range 5).all (fun hi =>
        agg.join (agg.reference (a.drop lo))
            (agg.join (combine agg ps) (agg.reference (z.take hi))) ==
          agg.reference (((a ++ ps.flatten ++ z).drop lo).take
            (a.length - lo + ps.flatten.length + hi)))))))

#eval probe 1
#eval probe 2

/-! ## Modular helper facts (this environment has no Mathlib) -/

private def M : Nat := 65521
private def B : Nat := 257
private def step (h x : Nat) : Nat := (B * h + x) % M
private def hpow (xs : List Nat) : Nat := B ^ xs.length % M
private def hval (xs : List Nat) : Nat := xs.foldl step 0

private def Cong (a b : Nat) : Prop := a % M = b % M

private theorem eq_of_cong_of_lt {a b : Nat} (ha : a < M) (hb : b < M) (h : Cong a b) : a = b := by
  unfold Cong at h
  rw [Nat.mod_eq_of_lt ha, Nat.mod_eq_of_lt hb] at h
  exact h

private theorem cong_mod (a : Nat) : Cong (a % M) a := by
  unfold Cong
  rw [Nat.mod_mod]

private theorem cong_add_right {a b : Nat} (h : Cong a b) (c : Nat) : Cong (a + c) (b + c) := by
  unfold Cong at h ⊢
  calc (a + c) % M = (a % M + c % M) % M := Nat.add_mod a c M
       _ = (b % M + c % M) % M := by rw [h]
       _ = (b + c) % M := (Nat.add_mod b c M).symm

private theorem cong_add_left {a b : Nat} (h : Cong a b) (c : Nat) : Cong (c + a) (c + b) := by
  unfold Cong at h ⊢
  calc (c + a) % M = (c % M + a % M) % M := Nat.add_mod c a M
       _ = (c % M + b % M) % M := by rw [h]
       _ = (c + b) % M := (Nat.add_mod c b M).symm

private theorem cong_mul_right {a b : Nat} (h : Cong a b) (c : Nat) : Cong (a * c) (b * c) := by
  unfold Cong at h ⊢
  calc (a * c) % M = (a % M * (c % M)) % M := Nat.mul_mod a c M
       _ = (b % M * (c % M)) % M := by rw [h]
       _ = (b * c) % M := (Nat.mul_mod b c M).symm

private theorem cong_trans {a b c : Nat} (hab : Cong a b) (hbc : Cong b c) : Cong a c := by
  unfold Cong at hab hbc ⊢
  rw [hab, hbc]

private theorem cong_mod_mul_add (u p v : Nat) :
    ((u % M) * p + v) % M = (u * p + v) % M := by
  have h1 : Cong ((u % M) * p) (u * p) := cong_mul_right (cong_mod u) p
  have h2 : Cong ((u % M) * p + v) (u * p + v) := cong_add_right h1 v
  unfold Cong at h2
  exact h2

private theorem cong_mod_add_mod_mul_add (u v p w : Nat) :
    (u + ((v % M) * p + w) % M) % M = (u + v * p + w) % M := by
  have h1 : Cong ((v % M) * p) (v * p) := cong_mul_right (cong_mod v) p
  have h2 : Cong ((v % M) * p + w) (v * p + w) := cong_add_right h1 w
  have h3 : Cong (((v % M) * p + w) % M) (v * p + w) := cong_trans (cong_mod _) h2
  have h4 := cong_add_left h3 u
  unfold Cong at h4
  rw [Nat.add_assoc]
  exact h4

private theorem poly_shift (acc x p v : Nat) :
    (B * acc + x) * p + v = acc * (p * B) + x * p + v := by
  rw [Nat.add_mul, Nat.mul_assoc, Nat.mul_comm B (acc * p), ← Nat.mul_assoc]

/-! ## Stitch for the polynomial fingerprint -/

private theorem hpow_append (a b : List Nat) : hpow (a ++ b) = hpow a * hpow b % M := by
  simp [hpow, List.length_append, Nat.pow_add, Nat.mul_mod]

private theorem hval_lt (xs : List Nat) : hval xs < M := by
  unfold hval
  suffices ∀ (l : List Nat) (acc : Nat), acc < M → l.foldl step acc < M from
    this xs 0 (by decide)
  intro l
  induction l with
  | nil => intro acc h; exact h
  | cons x xs ih =>
      intro acc h
      apply ih
      unfold step
      exact Nat.mod_lt _ (by decide)

private theorem foldl_mod (b : List Nat) (acc : Nat) (hacc : acc < M) :
    b.foldl step acc = (acc * B ^ b.length + hval b) % M := by
  induction b generalizing acc with
  | nil =>
      simpa [hval] using (Nat.mod_eq_of_lt hacc).symm
  | cons x xs ih =>
      simp only [List.foldl_cons]
      have hacc' : step acc x < M := by
        unfold step
        exact Nat.mod_lt _ (by decide)
      rw [ih (step acc x) hacc']
      have hxacc : x % M < M := Nat.mod_lt _ (by decide)
      have hvx := ih (x % M) hxacc
      have hval_cons : hval (x :: xs) = (x % M * B ^ xs.length + hval xs) % M := by
        simpa [hval, step, Nat.zero_mul, Nat.zero_add] using hvx
      rw [hval_cons]
      change (((B * acc + x) % M) * B ^ xs.length + hval xs) % M =
        (acc * B ^ (xs.length + 1) + (x % M * B ^ xs.length + hval xs) % M) % M
      rw [Nat.pow_succ]
      rw [cong_mod_mul_add (B * acc + x) (B ^ xs.length) (hval xs)]
      rw [cong_mod_add_mod_mul_add (acc * (B ^ xs.length * B)) x (B ^ xs.length) (hval xs)]
      rw [poly_shift acc x (B ^ xs.length) (hval xs)]

private theorem hval_append (a b : List Nat) :
    hval (a ++ b) = (hval a * hpow b + hval b) % M := by
  change (a ++ b).foldl step 0 = (a.foldl step 0 * hpow b + b.foldl step 0) % M
  rw [List.foldl_append]
  have hlt : a.foldl step 0 < M := by simpa [hval] using hval_lt a
  rw [foldl_mod b (a.foldl step 0) hlt]
  have hmul : Cong (a.foldl step 0 * B ^ b.length) (a.foldl step 0 * hpow b) := by
    unfold Cong hpow
    calc (a.foldl step 0 * B ^ b.length) % M = (a.foldl step 0 % M * (B ^ b.length % M)) % M := Nat.mul_mod _ _ _
         _ = (a.foldl step 0 % M * ((B ^ b.length % M) % M)) % M := by rw [Nat.mod_mod]
         _ = (a.foldl step 0 * (B ^ b.length % M)) % M :=
             (Nat.mul_mod (a.foldl step 0) (B ^ b.length % M) M).symm
  unfold Cong at hmul
  calc (a.foldl step 0 * B ^ b.length + b.foldl step 0) % M =
      ((a.foldl step 0 * B ^ b.length) % M + (b.foldl step 0) % M) % M := Nat.add_mod _ _ _
       _ = ((a.foldl step 0 * hpow b) % M + (b.foldl step 0) % M) % M := by rw [hmul]
       _ = (a.foldl step 0 * hpow b + b.foldl step 0) % M := (Nat.add_mod _ _ _).symm

private theorem stitch_agg : stitch agg := by
  intro a b
  simp only [agg]
  apply Prod.ext
  · simpa [hpow, B, M] using (hpow_append a b).symm
  · change (a.foldl step 0 * hpow b + b.foldl step 0) % M = (a ++ b).foldl step 0
    exact (hval_append a b).symm

/-! ## FrameSlice is a generic consequence of stitch -/

private theorem foldl_reference (h : stitch agg) (l : List (List Nat)) (acc : List Nat) :
    List.foldl agg.join (agg.reference acc) (l.map agg.reference) =
      agg.reference (acc ++ l.flatten) := by
  induction l generalizing acc with
  | nil => simp
  | cons p ps ih =>
      simp only [List.map_cons, List.foldl, List.flatten_cons]
      rw [h acc p, ih (acc ++ p)]
      simp only [List.append_assoc]

private theorem combine_reference (h : stitch agg) (ps : List (List Nat)) :
    combine agg ps = agg.reference ps.flatten := by
  simpa [combine] using foldl_reference h ps []

private theorem slice_concat (a p z : List Nat) (lo hi : Nat) (hlo : lo ≤ a.length) :
    a.drop lo ++ p ++ z.take hi =
      ((a ++ p ++ z).drop lo).take (a.length - lo + p.length + hi) := by
  rw [show a ++ p ++ z = a ++ (p ++ z) by simp only [List.append_assoc],
    List.drop_append_of_le_length hlo, List.take_append]
  have hd : (a.drop lo).length = a.length - lo := by simp
  rw [hd]
  have hN : a.length - lo + p.length + hi - (a.length - lo) = p.length + hi := by omega
  rw [hN, List.take_append]
  have hp : p.length + hi - p.length = hi := by omega
  rw [hp]
  have htakeA : List.take (a.length - lo + p.length + hi) (a.drop lo) = a.drop lo :=
    List.take_of_length_le (by simp [hd]; omega)
  have htakeP : List.take (p.length + hi) p = p :=
    List.take_of_length_le (by omega)
  rw [htakeA, htakeP]
  exact List.append_assoc _ _ _

theorem sound_FrameSlice : RollingFingerprint.FrameSlice.Goal := by
  intro a ps z lo hi hlo
  rw [combine_reference stitch_agg, stitch_agg, stitch_agg]
  refine congrArg agg.reference ?_
  simpa only [List.append_assoc] using slice_concat a ps.flatten z lo hi hlo

end TinylambOpt.ResearchProofs.StorageExecution.Goal_OrderedFooters_RollingFingerprint_FrameSlice
