import TinylambOpt.ResearchGoals.BenchmarkRematch.Tpcds

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false

namespace TinylambOpt.ResearchProofs.BenchmarkRematch.Tpcds_Q11DuckDB

open TinylambOpt.ResearchGoals.BenchmarkRematch
open TinylambOpt.ResearchGoals.BenchmarkRematch.Tpcds

/-- The key widener `k ↦ (k, attr k)` reflects boolean equality. -/
private theorem fBeq (attr : Nat → String) (x y : Nat) :
    ((x, attr x) == (y, attr y)) = (x == y) := by
  have red : ((x, attr x) == (y, attr y)) = (x == y && attr x == attr y) := rfl
  rw [red]
  cases h : (x == y : Bool) with
  | true =>
    have hx : x = y := Nat.eq_of_beq_eq_true h
    subst hx
    simp only [beq_self_eq_true, Bool.and_true]
  | false => rw [Bool.false_and]

private theorem transport (attr : Nat → String) (t : List Nat) (a : Nat) :
    (t.map (fun k => (k, attr k))).filter (fun b => !b == (a, attr a)) =
      (t.filter (fun k => !k == a)).map (fun k => (k, attr k)) := by
  rw [List.filter_map]
  refine congrArg (List.map (fun k => (k, attr k)))
    (List.filter_congr (l := t) (fun k hk => ?_))
  exact congrArg Bool.not (fBeq attr k a)

private theorem eraseDups_map (attr : Nat → String) :
    ∀ (l : List Nat), (l.map (fun k => (k, attr k))).eraseDups =
        l.eraseDups.map (fun k => (k, attr k)) := by
  have key : ∀ (n : Nat) (l : List Nat), l.length ≤ n →
      (l.map (fun k => (k, attr k))).eraseDups = l.eraseDups.map (fun k => (k, attr k)) := by
    intro n
    induction n with
    | zero =>
      intro l hl
      cases l with
      | nil => rfl
      | cons a t =>
        have h1 : (a :: t).length ≤ 0 := hl
        simp only [List.length_cons] at h1
        omega
    | succ m ih =>
      intro l hl
      cases l with
      | nil => rfl
      | cons a t =>
        have hlen : (a :: t).length = t.length + 1 := List.length_cons
        have ht : t.length ≤ m := by omega
        have hf : (t.filter (fun k => !k == a)).length ≤ m :=
          Nat.le_trans (List.length_filter_le _ _) ht
        rw [List.map_cons, List.eraseDups_cons, List.eraseDups_cons,
          transport attr t a, ih (t.filter (fun k => !k == a)) hf, List.map_cons]
  intro l
  exact key l.length l (Nat.le_refl _)

/-- Bucketing the wide rows on `(k, attr k)` is bucketing the sales rows on `k`. -/
private theorem filtW (sales : List (Nat × Option Int)) (attr : Nat → String) (k : Nat) :
    ((sales.map (fun x => ((x.1, attr x.1), x.2))).filter (fun r => r.1 == (k, attr k))).map
        Prod.snd =
      (sales.filter (fun r => r.1 == k)).map Prod.snd := by
  induction sales with
  | nil => rfl
  | cons j t ih =>
    rw [List.map_cons]
    cases h : (j.1 == k : Bool) with
    | true =>
      have hw : ((j.1, attr j.1) == (k, attr k)) = true := by
        rw [fBeq attr j.1 k]
        exact h
      rw [List.filter_cons_of_pos
          (p := fun r : (Nat × String) × Option Int => r.1 == (k, attr k)) hw,
        List.filter_cons_of_pos (p := fun r : Nat × Option Int => r.1 == k) h,
        List.map_cons, ih, List.map_cons]
    | false =>
      have hw : ¬((j.1, attr j.1) == (k, attr k)) = true := by
        intro hc
        rw [fBeq attr j.1 k] at hc
        rw [h] at hc
        exact absurd hc Bool.false_ne_true
      have hn : ¬(j.1 == k) = true := by
        rw [h]
        exact Bool.false_ne_true
      rw [List.filter_cons_of_neg
          (p := fun r : (Nat × String) × Option Int => r.1 == (k, attr k)) hw,
        List.filter_cons_of_neg (p := fun r : Nat × Option Int => r.1 == k) hn,
        ih]

theorem sound : Q11DuckDB.Goal := by
  intro sales attr ids wide wideKeys
  have h2 : (List.map Prod.fst sales).map (fun k => (k, attr k)) =
      sales.map (fun x => (x.1, attr x.1)) := by
    rw [List.map_map]
    exact List.map_congr_left (fun x _ => rfl)
  have hfst : wide.map Prod.fst = sales.map (fun x => (x.1, attr x.1)) := by
    show (sales.map (fun x => ((x.1, attr x.1), x.2))).map Prod.fst = _
    rw [List.map_map]
    exact List.map_congr_left (fun x _ => rfl)
  have hkeys : wideKeys = ids.map (fun k => (k, attr k)) := by
    show (wide.map Prod.fst).eraseDups = ids.map (fun k => (k, attr k))
    rw [hfst, ← h2, eraseDups_map attr (List.map Prod.fst sales)]
  rw [hkeys, List.map_map]
  refine List.map_congr_left (fun x _ => ?_)
  refine Prod.ext rfl (congrArg nsum ?_)
  exact (filtW sales attr x).symm

end TinylambOpt.ResearchProofs.BenchmarkRematch.Tpcds_Q11DuckDB
