import TinylambOpt.ResearchGoals.BenchmarkRematch.Common

set_option maxHeartbeats 0
namespace TinylambOpt.ResearchProofs.BenchmarkRematch.Nsum
open TinylambOpt.ResearchGoals.BenchmarkRematch

theorem nsum_eq (xs : List (Option Int)) :
    nsum xs = if (xs.filterMap id).isEmpty then none else some (xs.filterMap id).sum := rfl

theorem fm_rep_none (v : Nat) : (List.replicate v (none : Option Int)).filterMap id = [] := by
  induction v with
  | zero => rfl
  | succ v ih => simp [List.replicate_succ, ih]

theorem fm_rep_some (v : Nat) (x : Int) : (List.replicate v (some x)).filterMap id = List.replicate v x := by
  induction v with
  | zero => rfl
  | succ v ih => simp [List.replicate_succ, ih]

theorem fm_flatMap_rep (xs : List (Option Int)) (v : Nat) :
    (xs.flatMap (List.replicate v)).filterMap id = (xs.filterMap id).flatMap (List.replicate v) := by
  induction xs with
  | nil => rfl
  | cons a xs ih =>
    cases a with
    | none => simp [List.flatMap_cons, List.filterMap_append, fm_rep_none, ih]
    | some x => simp [List.flatMap_cons, List.filterMap_append, fm_rep_some, ih]

theorem flatMap_rep_sum (ys : List Int) (v : Nat) :
    (ys.flatMap (List.replicate v)).sum = (v : Int) * ys.sum := by
  induction ys with
  | nil => simp
  | cons y ys ih =>
    simp [List.flatMap_cons, List.sum_append_int, ih, Int.mul_add]

theorem flatMap_rep_length {α : Type} (ys : List α) (v : Nat) :
    (ys.flatMap (List.replicate v)).length = ys.length * v := by
  induction ys with
  | nil => simp
  | cons y ys ih => simp [List.flatMap_cons, ih, Nat.succ_mul, Nat.add_comm]

theorem nsum_rep_flat (xs : List (Option Int)) (v : Nat) :
    nsum (xs.flatMap (List.replicate v)) =
      if v == 0 then none else (nsum xs).map (fun x => (v : Int) * x) := by
  rw [nsum_eq, fm_flatMap_rep]
  by_cases hv : v = 0
  · subst hv; simp
  · have hv' : (v == 0) = false := by simpa using hv
    rw [hv', nsum_eq]
    have hlen := flatMap_rep_length (xs.filterMap id) v
    by_cases hq : (xs.filterMap id).isEmpty = true
    · have hq' : xs.filterMap id = [] := List.isEmpty_iff.mp hq
      simp [hq']
    · have hq' : (xs.filterMap id).isEmpty = false := by simpa using hq
      have hne : ¬ ((xs.filterMap id).flatMap (List.replicate v)).isEmpty = true := by
        rw [List.isEmpty_iff, ← List.length_eq_zero_iff, hlen]
        have : (xs.filterMap id).length ≠ 0 := by
          intro h0; rw [List.length_eq_zero_iff] at h0; simp [h0] at hq'
        exact Nat.mul_ne_zero this hv
      simp [hq', hne, flatMap_rep_sum]

theorem fm_flatten_rep (ys : List (Option Int)) (c : Nat) :
    ((List.replicate c ys).flatten).filterMap id = (List.replicate c (ys.filterMap id)).flatten := by
  induction c with
  | zero => rfl
  | succ c ih => simp [List.replicate_succ, List.filterMap_append, ih]

theorem flatten_rep_sum (ys : List Int) (c : Nat) :
    ((List.replicate c ys).flatten).sum = (c : Int) * ys.sum := by
  induction c with
  | zero => simp
  | succ c ih => simp [List.replicate_succ, List.sum_append_int, ih, Int.add_mul]; omega

theorem flatten_rep_length {α : Type} (ys : List α) (c : Nat) :
    ((List.replicate c ys).flatten).length = c * ys.length := by
  induction c with
  | zero => simp
  | succ c ih => simp [List.replicate_succ, ih, Nat.succ_mul]; omega

theorem nsum_flat_rep (ys : List (Option Int)) (c : Nat) :
    nsum ((List.replicate c ys).flatten) =
      if c == 0 then none else (nsum ys).map (fun x => (c : Int) * x) := by
  rw [nsum_eq, fm_flatten_rep]
  by_cases hc : c = 0
  · subst hc; simp
  · have hc' : (c == 0) = false := by simpa using hc
    rw [hc', nsum_eq]
    have hlen := flatten_rep_length (ys.filterMap id) c
    by_cases hq : (ys.filterMap id).isEmpty = true
    · have hq' : ys.filterMap id = [] := List.isEmpty_iff.mp hq
      simp [hq']
    · have hq' : (ys.filterMap id).isEmpty = false := by simpa using hq
      have hne : ¬ ((List.replicate c (ys.filterMap id)).flatten).isEmpty = true := by
        rw [List.isEmpty_iff, ← List.length_eq_zero_iff, hlen]
        have : (ys.filterMap id).length ≠ 0 := by
          intro h0; rw [List.length_eq_zero_iff] at h0; simp [h0] at hq'
        exact Nat.mul_ne_zero hc this
      simp [hq', hne, flatten_rep_sum]

end TinylambOpt.ResearchProofs.BenchmarkRematch.Nsum
