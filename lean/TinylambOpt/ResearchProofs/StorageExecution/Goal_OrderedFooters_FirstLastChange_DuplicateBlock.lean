import TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters

/-!
A footer can guide sorted scans and key-range probes when its indexed summary is
consistent with the stored blocks.

Proved

- `FirstLastChange.DuplicateBlock` — `∀ xs k, xs≠[] → 0<k → let n := ((agg.reference
  xs).map (fun r => r.2.2)).getD 0 ((agg.reference (List.replicate k xs).flatten).map
  (fun r => r.2.2)).getD 0 = k*n-(if xs.head?==xs.getLast? then k-1 else 0)`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_OrderedFooters_FirstLastChange_DuplicateBlock

open TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters
open TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters.FirstLastChange

/-! ## Small-input truth checks -/

private def gen : Nat → List (List Nat)
  | 0 => [[]]
  | n + 1 => let p := gen n; p ++ p.flatMap (fun xs => [0 :: xs, 1 :: xs, 2 :: xs])

private def runs (xs : List Nat) : Nat :=
  ((agg.reference xs).map (fun r => r.2.2)).getD 0

private def changes (xs : List Nat) : Nat :=
  (edges xs).countP (fun p : Nat × Nat => p.1 != p.2)

private def probe (n kmax : Nat) : Bool :=
  (gen n).all (fun xs => xs ≠ [] → (List.range kmax).all (fun k' =>
    let k := k' + 1
    runs (List.replicate k xs).flatten = k * runs xs -
      (if xs.head? == xs.getLast? then k - 1 else 0)))

#eval (probe 3 8, probe 4 8, probe 5 8)

/-! ## Edge-count of a concatenation -/

private theorem edges_cons (x : Nat) (zs : List Nat) (hzs : zs ≠ []) :
    edges (x :: zs) = (x, zs.head?.getD 0) :: edges zs := by
  cases zs with
  | nil => simp at hzs
  | cons y ys => simp [edges]

private theorem edges_append_nonempty (a b : List Nat) (ha : a ≠ []) (hb : b ≠ []) :
    edges (a ++ b) = edges a ++ ((a.getLast?.getD 0, b.head?.getD 0) :: edges b) := by
  induction a with
  | nil => simp at ha
  | cons x xs ih =>
      cases xs with
      | nil =>
          cases b with
          | nil => simp at hb
          | cons y ys => simp [edges, List.getLast?_cons]
      | cons y ys =>
          have hne : (y :: ys) ++ b ≠ [] := by simp
          have hhead : (((y :: ys) ++ b).head?.getD 0) = y := by simp
          rw [show (x :: y :: ys) ++ b = x :: ((y :: ys) ++ b) by simp]
          rw [edges_cons x ((y :: ys) ++ b) hne, hhead]
          rw [edges_cons x (y :: ys) (by simp)]
          rw [ih (by simp)]
          simp [List.getLast?_cons]

private theorem changes_append (a b : List Nat) (ha : a ≠ []) (hb : b ≠ []) :
    changes (a ++ b) = changes a + changes b +
      (decide (a.getLast?.getD 0 ≠ b.head?.getD 0)).toNat := by
  rw [changes, changes, changes]
  rw [edges_append_nonempty a b ha hb, List.countP_append, List.countP_cons]
  by_cases h : a.getLast?.getD 0 ≠ b.head?.getD 0 <;> simp [h] <;> omega

/-! ## Basic facts about `runs` and repeated blocks -/

private theorem runs_eq_one_add_changes (xs : List Nat) (hxs : xs ≠ []) :
    runs xs = 1 + changes xs := by
  cases xs with
  | nil => contradiction
  | cons y ys => simp [runs, changes, agg]

private theorem flatten_replicate_nonempty (xs : List Nat) (k : Nat) (hxs : xs ≠ []) (hk : 0 < k) :
    (List.replicate k xs).flatten ≠ [] := by
  cases xs with
  | nil => contradiction
  | cons y ys =>
      cases k with
      | zero => omega
      | succ k => simp

private theorem getLast?_flatten_replicate (xs : List Nat) (k : Nat) (hk : 0 < k) :
    ((List.replicate k xs).flatten).getLast? = xs.getLast? := by
  induction k with
  | zero => omega
  | succ k ih =>
      cases k with
      | zero => simp
      | succ k =>
          rw [show k + 1 + 1 = (k + 1) + 1 by omega]
          rw [← List.replicate_append_replicate]
          simp only [List.flatten_append, List.replicate_one, List.flatten_singleton]
          rw [List.getLast?_append]
          cases xs with
          | nil => simp
          | cons y ys => simp [List.getLast?_cons]

private theorem getLast?_cons_eq (x : Nat) (xs : List Nat) :
    (x :: xs).getLast? = some (xs.getLast?.getD x) := by
  have h := List.getLast?_eq_some_getLast (l := x :: xs) (by simp)
  have hlast : (x :: xs).getLast (by simp) = xs.getLast?.getD x := by
    have h' := List.getLast?_eq_some_getLast (l := x :: xs) (by simp)
    rw [List.getLast?_cons] at h'
    simpa using h'.symm
  rw [hlast] at h
  exact h

private theorem boundary_delta (xs : List Nat) (hxs : xs ≠ []) :
    (decide (xs.getLast?.getD 0 ≠ xs.head?.getD 0)).toNat =
      (if xs.head? == xs.getLast? then 0 else 1) := by
  cases xs with
  | nil => contradiction
  | cons y ys =>
      rw [getLast?_cons_eq y ys]
      simp only [List.head?_cons, Option.getD_some]
      by_cases h : ys.getLast?.getD y = y
      · simp [h]
      · have h' : y ≠ ys.getLast?.getD y := by
          intro he
          exact h he.symm
        simp [h, h']

/-! ## Closed form for the edge count of a repeated block -/

private theorem changes_replicate (xs : List Nat) (hxs : xs ≠ []) :
    ∀ k : Nat, 0 < k →
      changes (List.replicate k xs).flatten =
        k * changes xs + (k - 1) * (if xs.head? == xs.getLast? then 0 else 1) := by
  intro k hk
  induction k with
  | zero => omega
  | succ k ih =>
      cases k with
      | zero => simp
      | succ k =>
          have hkpos : 0 < Nat.succ k := Nat.succ_pos _
          have ih' := ih hkpos
          have ha : (List.replicate (Nat.succ k) xs).flatten ≠ [] :=
            flatten_replicate_nonempty xs (Nat.succ k) hxs hkpos
          rw [show k + 1 + 1 = (k + 1) + 1 by omega]
          rw [← List.replicate_append_replicate]
          simp only [List.flatten_append, List.replicate_one, List.flatten_singleton]
          rw [changes_append _ _ ha hxs, ih']
          rw [getLast?_flatten_replicate xs (Nat.succ k) hkpos]
          rw [boundary_delta xs hxs]
          simp [Nat.succ_mul]
          omega

/-! ## Goal -/

theorem sound_DuplicateBlock : FirstLastChange.DuplicateBlock.Goal := by
  intro xs k hxs hk
  change runs (List.replicate k xs).flatten = k * runs xs -
    (if xs.head? == xs.getLast? then k - 1 else 0)
  rw [runs_eq_one_add_changes _ (flatten_replicate_nonempty xs k hxs hk)]
  rw [runs_eq_one_add_changes xs hxs]
  rw [changes_replicate xs hxs k hk]
  by_cases h : xs.head? == xs.getLast?
  · simp [h, Nat.mul_add]
    omega
  · simp [h, Nat.mul_add]
    omega

end TinylambOpt.ResearchProofs.StorageExecution.Goal_OrderedFooters_FirstLastChange_DuplicateBlock
