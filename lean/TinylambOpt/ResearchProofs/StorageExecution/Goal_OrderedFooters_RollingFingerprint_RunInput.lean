import TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters

/-!
A footer can guide sorted scans and key-range probes when its indexed summary is
consistent with the stored blocks.

Proved

- `RollingFingerprint.RunInput` — Consume the lengths on a run-encoded column directly.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_OrderedFooters_RollingFingerprint_RunInput

open TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters
open TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters.RollingFingerprint

/-! Ground check on small inputs (the probe must return `true`). -/

private def probe : Bool :=
  (List.range 6).all (fun _n =>
    (List.range 6).all (fun v =>
      (List.range 6).all (fun k =>
        let rs := [(v, k), (v + 1, k), (v, k + 1)]
        (rs.map (fun (x, c) => repeatState agg (agg.reference [x]) c)).foldl agg.join
            (agg.reference []) ==
          agg.reference (rs.flatMap (fun (x, c) => List.replicate c x)))))

#eval probe

/-! Stitch for the polynomial fingerprint: work with the raw polynomial hash
and reduce modulo only at the end, so the append law needs no modular algebra. -/

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

private theorem replicate_add_eq {α : Type} (a b : Nat) (x : α) :
    List.replicate (a + b) x = List.replicate a x ++ List.replicate b x := by
  induction a with
  | zero => simp
  | succ a ih =>
      rw [Nat.succ_add, List.replicate_succ, List.replicate_succ, ih]
      simp

/-! `repeatState` computes the join-power of a state, hence on a reachable
one-element state it is the fingerprint of the corresponding run. -/

private theorem repeatState_single (v n : Nat) :
    repeatState agg (agg.reference [v]) n = agg.reference (List.replicate n v) := by
  let rep : Nat → Nat × Nat := fun k => agg.reference (List.replicate k v)
  have hdouble : ∀ k, agg.join (rep k) (rep k) = rep (k + k) := by
    intro k
    dsimp [rep]
    rw [stitch_agg]
    congr 1
    exact (replicate_add_eq k k v).symm
  have hsucc : ∀ k, agg.join (rep k) (agg.reference [v]) = rep (k + 1) := by
    intro k
    dsimp [rep]
    rw [stitch_agg]
    congr 1
    rw [replicate_add_eq k 1 v]
    simp
  exact Nat.strongRecOn n (fun n ih => by
    by_cases hn : n = 0
    · rw [repeatState]
      simp only [hn, dite_true]
      rfl
    · have hhalf : n / 2 < n := Nat.div_lt_self (by omega) (by decide)
      have ihh := ih (n / 2) hhalf
      rw [repeatState]
      simp only [hn, dite_false]
      rw [ihh]
      by_cases heven : n % 2 == 0
      · simp [heven]
        rw [hdouble]
        congr 1
        congr 1
        have hm : n % 2 = 0 := by simpa using heven
        have hdiv := Nat.mod_add_div n 2
        omega
      · simp [heven]
        rw [hdouble, hsucc]
        congr 1
        congr 1
        have hm_lt : n % 2 < 2 := Nat.mod_lt _ (by decide)
        have hm_ne : n % 2 ≠ 0 := by
          intro hzero
          apply heven
          simp [hzero]
        have hm : n % 2 = 1 := by omega
        have hdiv := Nat.mod_add_div n 2
        omega)

private theorem foldl_reference (h : stitch agg) (l : List (List Nat)) (acc : List Nat) :
    List.foldl agg.join (agg.reference acc) (l.map agg.reference) =
      agg.reference (acc ++ l.flatten) := by
  induction l generalizing acc with
  | nil => simp
  | cons p ps ih =>
      simp only [List.map_cons, List.foldl, List.flatten_cons]
      rw [h acc p, ih (acc ++ p)]
      simp only [List.append_assoc]

private theorem flatMap_eq_map_flatten {α β : Type} (f : α → List β) (rs : List α) :
    rs.flatMap f = (rs.map f).flatten := by
  induction rs with
  | nil => simp
  | cons a rs ih => simp [ih]

private theorem map_repeat_eq (rs : List (Nat × Nat)) :
    rs.map (fun (v, n) => repeatState agg (agg.reference [v]) n) =
      rs.map (fun (v, n) => agg.reference (List.replicate n v)) := by
  induction rs with
  | nil => simp
  | cons p rs ih =>
      rcases p with ⟨v, n⟩
      simp [repeatState_single]

theorem sound_RunInput : RunInput.Goal := by
  intro rs
  rw [map_repeat_eq, flatMap_eq_map_flatten]
  simpa [Function.comp_def] using
    (foldl_reference stitch_agg (rs.map (fun (v, n) => List.replicate n v)) [])

end TinylambOpt.ResearchProofs.StorageExecution.Goal_OrderedFooters_RollingFingerprint_RunInput
