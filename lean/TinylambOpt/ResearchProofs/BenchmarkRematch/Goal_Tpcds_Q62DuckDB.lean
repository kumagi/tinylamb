import TinylambOpt.ResearchGoals.BenchmarkRematch.Tpcds

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false

namespace TinylambOpt.ResearchProofs.BenchmarkRematch.Tpcds_Q62DuckDB
open TinylambOpt.ResearchGoals.BenchmarkRematch
open TinylambOpt.ResearchGoals.BenchmarkRematch.Tpcds

/-- Filtering the four shipped boundaries reduces to four independent booleans. -/
private theorem flt : ∀ (p : Int → Bool) (a b c e : Bool),
    p 30 = a → p 60 = b → p 90 = c → p 120 = e →
      List.filter p [30, 60, 90, 120] =
        (if a then [30] else []) ++ (if b then [60] else []) ++
          (if c then [90] else []) ++ (if e then [120] else []) := by
  intro p a b c e h1 h2 h3 h4
  rw [List.filter_cons, List.filter_cons, List.filter_cons, List.filter_cons,
      List.filter_nil, h1, h2, h3, h4]
  cases a <;> cases b <;> cases c <;> cases e <;> simp

/-- The bucket number against the shipped boundaries [30,60,90,120] is exactly
the five CASE branches. -/
private theorem bk (d : Int) : bucket [30, 60, 90, 120] d =
    if d ≤ 30 then 0
    else if d ≤ 60 then 1
    else if d ≤ 90 then 2
    else if d ≤ 120 then 3
    else 4 := by
  have h5 : d ≤ 30 ∨ (30 < d ∧ d ≤ 60) ∨ (60 < d ∧ d ≤ 90) ∨ (90 < d ∧ d ≤ 120) ∨ 120 < d := by
    omega
  rcases h5 with h | h | h | h | h
  · unfold bucket
    rw [List.countP_eq_length_filter,
        flt (fun x => decide (x < d)) false false false false
          (decide_eq_false (by omega)) (decide_eq_false (by omega))
          (decide_eq_false (by omega)) (decide_eq_false (by omega))]
    rw [ite_eq_left h]
    rfl
  · unfold bucket
    rw [List.countP_eq_length_filter,
        flt (fun x => decide (x < d)) true false false false
          (decide_eq_true h.1) (decide_eq_false (by omega))
          (decide_eq_false (by omega)) (decide_eq_false (by omega))]
    rw [ite_eq_right (by omega : ¬(d ≤ 30)), ite_eq_left h.2]
    rfl
  · unfold bucket
    rw [List.countP_eq_length_filter,
        flt (fun x => decide (x < d)) true true false false
          (decide_eq_true (by omega)) (decide_eq_true h.1)
          (decide_eq_false (by omega)) (decide_eq_false (by omega))]
    rw [ite_eq_right (by omega : ¬(d ≤ 30)), ite_eq_right (by omega : ¬(d ≤ 60)), ite_eq_left h.2]
    rfl
  · unfold bucket
    rw [List.countP_eq_length_filter,
        flt (fun x => decide (x < d)) true true true false
          (decide_eq_true (by omega)) (decide_eq_true (by omega))
          (decide_eq_true h.1) (decide_eq_false (by omega))]
    rw [ite_eq_right (by omega : ¬(d ≤ 30)), ite_eq_right (by omega : ¬(d ≤ 60)),
        ite_eq_right (by omega : ¬(d ≤ 90)), ite_eq_left h.2]
    rfl
  · unfold bucket
    rw [List.countP_eq_length_filter,
        flt (fun x => decide (x < d)) true true true true
          (decide_eq_true (by omega)) (decide_eq_true (by omega))
          (decide_eq_true (by omega)) (decide_eq_true h)]
    rw [ite_eq_right (by omega : ¬(d ≤ 30)), ite_eq_right (by omega : ¬(d ≤ 60)),
        ite_eq_right (by omega : ¬(d ≤ 90)), ite_eq_right (by omega : ¬(d ≤ 120))]
    rfl

theorem sound : Q62DuckDB.Goal := by
  intro d
  have h5 : d ≤ 30 ∨ (30 < d ∧ d ≤ 60) ∨ (60 < d ∧ d ≤ 90) ∨ (90 < d ∧ d ≤ 120) ∨ 120 < d := by
    omega
  rcases h5 with h | h | h | h | h
  · rw [bk d, ite_eq_left h]
    simp only [Prod.mk.injEq]
    exact ⟨(decide_eq_true h).symm, (decide_eq_false (by omega)).symm,
      (decide_eq_false (by omega)).symm, (decide_eq_false (by omega)).symm,
      (decide_eq_false (by omega)).symm⟩
  · rw [bk d, ite_eq_right (by omega : ¬(d ≤ 30)), ite_eq_left h.2]
    simp only [Prod.mk.injEq]
    exact ⟨(decide_eq_false (by omega)).symm, (decide_eq_true h).symm,
      (decide_eq_false (by omega)).symm, (decide_eq_false (by omega)).symm,
      (decide_eq_false (by omega)).symm⟩
  · rw [bk d, ite_eq_right (by omega : ¬(d ≤ 30)), ite_eq_right (by omega : ¬(d ≤ 60)), ite_eq_left h.2]
    simp only [Prod.mk.injEq]
    exact ⟨(decide_eq_false (by omega)).symm, (decide_eq_false (by omega)).symm,
      (decide_eq_true h).symm, (decide_eq_false (by omega)).symm,
      (decide_eq_false (by omega)).symm⟩
  · rw [bk d, ite_eq_right (by omega : ¬(d ≤ 30)), ite_eq_right (by omega : ¬(d ≤ 60)),
        ite_eq_right (by omega : ¬(d ≤ 90)), ite_eq_left h.2]
    simp only [Prod.mk.injEq]
    exact ⟨(decide_eq_false (by omega)).symm, (decide_eq_false (by omega)).symm,
      (decide_eq_false (by omega)).symm, (decide_eq_true h).symm,
      (decide_eq_false (by omega)).symm⟩
  · rw [bk d, ite_eq_right (by omega : ¬(d ≤ 30)), ite_eq_right (by omega : ¬(d ≤ 60)),
        ite_eq_right (by omega : ¬(d ≤ 90)), ite_eq_right (by omega : ¬(d ≤ 120))]
    simp only [Prod.mk.injEq]
    exact ⟨(decide_eq_false (by omega)).symm, (decide_eq_false (by omega)).symm,
      (decide_eq_false (by omega)).symm, (decide_eq_false (by omega)).symm,
      (decide_eq_true h).symm⟩

#print axioms TinylambOpt.ResearchProofs.BenchmarkRematch.Tpcds_Q62DuckDB.sound
