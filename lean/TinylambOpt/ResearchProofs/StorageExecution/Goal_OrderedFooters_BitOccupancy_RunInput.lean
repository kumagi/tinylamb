import TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters

/-!
A footer can guide sorted scans and key-range probes when its indexed summary is
consistent with the stored blocks.

Proved

- `BitOccupancy.RunInput` — Consume the lengths on a run-encoded column directly.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_OrderedFooters_BitOccupancy_RunInput

open TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters
open TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters.BitOccupancy

/-! ## Small-input truth checks (all probes must return `true`). -/

private def lhs (rs : List (Nat × Nat)) :=
  (rs.map (fun (v, n) => repeatState agg (agg.reference [v]) n)).foldl agg.join (agg.reference [])
private def rhs (rs : List (Nat × Nat)) :=
  agg.reference (rs.flatMap (fun (v, n) => List.replicate n v))

#eval lhs [(0, 2), (1, 3), (63, 1), (64, 2)] == rhs [(0, 2), (1, 3), (63, 1), (64, 2)]
#eval lhs [(1, 0), (2, 0)] == rhs [(1, 0), (2, 0)]

private def probe : Bool :=
  (List.range 6).all (fun v =>
    (List.range 6).all (fun n =>
      (List.range 6).all (fun w =>
        (List.range 6).all (fun m =>
          let rs := [(v, n), (w, m), (v, m)]
          lhs rs == rhs rs))))
#eval probe

/-! ## The bit-occupancy fold stitches. -/

private theorem foldl_or (bit : Nat → Nat) (l : List Nat) (c : Nat) :
    l.foldl (fun a x => a ||| bit x) c = c ||| l.foldl (fun a x => a ||| bit x) 0 := by
  induction l generalizing c with
  | nil => simp
  | cons x xs ih =>
      simp only [List.foldl_cons]
      calc
        xs.foldl (fun a x => a ||| bit x) (c ||| bit x)
            = (c ||| bit x) ||| xs.foldl (fun a x => a ||| bit x) 0 := ih (c ||| bit x)
        _ = c ||| (bit x ||| xs.foldl (fun a x => a ||| bit x) 0) := by
              simp [Nat.or_assoc]
        _ = c ||| xs.foldl (fun a x => a ||| bit x) (bit x) := by
              rw [ih (bit x)]
        _ = c ||| xs.foldl (fun a x => a ||| bit x) (0 ||| bit x) := by simp

private theorem stitch_agg : stitch agg := by
  intro a b
  change (a.length + b.length,
      a.foldl (fun s x => s ||| 2 ^ (x % 64)) 0 ||| b.foldl (fun s x => s ||| 2 ^ (x % 64)) 0) =
    ((a ++ b).length, (a ++ b).foldl (fun s x => s ||| 2 ^ (x % 64)) 0)
  apply Prod.ext
  · simp
  · change a.foldl (fun s x => s ||| 2 ^ (x % 64)) 0 ||| b.foldl (fun s x => s ||| 2 ^ (x % 64)) 0 =
      (a ++ b).foldl (fun s x => s ||| 2 ^ (x % 64)) 0
    rw [List.foldl_append]
    exact (foldl_or (fun x => 2 ^ (x % 64)) b (a.foldl (fun s x => s ||| 2 ^ (x % 64)) 0)).symm

/-! ## Binary-power derivation from the stitch law. -/

private theorem repeatPower_agg : repeatPower agg := by
  intro xs n
  induction n using Nat.strongRecOn with
  | _ k ih =>
    unfold repeatState
    by_cases hk : k = 0
    · simp [hk]
    · simp only [hk, dite_false]
      have hdiv : k / 2 < k := Nat.div_lt_self (by omega) (by decide)
      have ih2 := ih (k / 2) hdiv
      let a := repeatState agg (agg.reference xs) (k / 2)
      have ha' : a = agg.reference (List.replicate (k / 2) xs).flatten := by simpa [a] using ih2
      have hjoin : agg.join a a =
          agg.reference ((List.replicate (k / 2) xs).flatten ++ (List.replicate (k / 2) xs).flatten) := by
        rw [ha']
        exact stitch_agg _ _
      have hrep2 : (List.replicate (k / 2) xs).flatten ++ (List.replicate (k / 2) xs).flatten =
          (List.replicate (k / 2 * 2) xs).flatten := by
        rw [← List.flatten_append]
        congr 1
        rw [show k / 2 * 2 = k / 2 + k / 2 by omega, ← List.replicate_append_replicate]
      by_cases he : k % 2 = 0
      · have hk2 : k / 2 * 2 = k := by omega
        have hcond : (k % 2 == 0) = true := by simp [he]
        simp only [hcond, ite_true]
        change agg.join a a = agg.reference (List.replicate k xs).flatten
        rw [hjoin, hrep2, hk2]
      · have hodd : k % 2 = 1 := by omega
        have hk1 : k = k / 2 * 2 + 1 := by omega
        have hcond : (k % 2 == 0) = false := by simp [hodd]
        simp only [hcond]
        change agg.join (agg.join a a) (agg.reference xs) = agg.reference (List.replicate k xs).flatten
        rw [hjoin, hrep2, stitch_agg]
        congr 1
        rw [hk1, show k / 2 * 2 + 1 = (k / 2 + k / 2) + 1 by omega, ← List.replicate_append_replicate,
            List.flatten_append, List.replicate_one, List.flatten_singleton]
        have heq : (k / 2 + k / 2 + 1) / 2 * 2 = k / 2 + k / 2 := by omega
        simp [heq]

/-! ## General run-input derivation. -/

private theorem foldl_reference (l : List (List Nat)) (acc : List Nat) :
    List.foldl agg.join (agg.reference acc) (l.map agg.reference) =
      agg.reference (acc ++ l.flatten) := by
  induction l generalizing acc with
  | nil => simp
  | cons p ps ih =>
      simp only [List.map_cons, List.foldl, List.flatten_cons]
      rw [stitch_agg acc p, ih (acc ++ p)]
      simp only [List.append_assoc]

private theorem flatMap_eq_map_flatten {α β : Type} (f : α → List β) (rs : List α) :
    rs.flatMap f = (rs.map f).flatten := by
  induction rs with
  | nil => simp
  | cons a rs ih => simp [ih]

private theorem replicate_singleton_flatten (v n : Nat) :
    (List.replicate n [v]).flatten = List.replicate n v := by
  induction n with
  | zero => simp
  | succ n => simp

private theorem map_repeat_eq (rs : List (Nat × Nat)) :
    rs.map (fun (v, n) => repeatState agg (agg.reference [v]) n) =
      rs.map (fun (v, n) => agg.reference (List.replicate n v)) := by
  induction rs with
  | nil => simp
  | cons p rs ih =>
      rcases p with ⟨v, n⟩
      simp only [List.map_cons]
      rw [repeatPower_agg [v] n, replicate_singleton_flatten, ih]

theorem sound_RunInput : RunInput.Goal := by
  intro rs
  rw [map_repeat_eq, flatMap_eq_map_flatten]
  simpa [Function.comp_def] using
    (foldl_reference (rs.map (fun (v, n) => List.replicate n v)) [])

end TinylambOpt.ResearchProofs.StorageExecution.Goal_OrderedFooters_BitOccupancy_RunInput
