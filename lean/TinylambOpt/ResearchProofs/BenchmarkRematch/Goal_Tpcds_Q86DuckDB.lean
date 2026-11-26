import TinylambOpt.ResearchGoals.BenchmarkRematch.Tpcds

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false

namespace TinylambOpt.ResearchProofs.BenchmarkRematch.Tpcds_Q86DuckDB
open TinylambOpt.ResearchGoals.BenchmarkRematch
open TinylambOpt.ResearchGoals.BenchmarkRematch.Tpcds

/-- On a descending list, `sales < ·` holds for a downward-closed tail, so
`takeWhile` agrees with `filter`. -/
private theorem ftw_desc (sales : Int) (l : List Int)
    (h : List.Pairwise (fun a b : Int => decide (b ≤ a) = true) l) :
    l.takeWhile (fun v => decide (sales < v)) = l.filter (fun v => decide (sales < v)) := by
  induction l with
  | nil => rfl
  | cons a as ih =>
    obtain ⟨h1, h2⟩ := List.pairwise_cons.mp h
    rw [List.takeWhile_cons, List.filter_cons]
    by_cases ha : decide (sales < a)
    · simp only [ha, ite_true]
      rw [ih h2]
    · simp only [ha, Bool.false_eq_true, ite_false]
      rw [eq_comm, List.filter_eq_nil_iff]
      intro b hb hpb
      have hle : b ≤ a := of_decide_eq_true (h1 b hb)
      have hlt : sales < b := of_decide_eq_true hpb
      exact absurd (decide_eq_true (Int.lt_of_lt_of_le hlt hle)) ha

theorem sound : Q86DuckDB.Goal := by
  intro xs tag parent sales
  have trans : ∀ a b c : Int, decide (a ≤ b) = true → decide (b ≤ c) = true →
      decide (a ≤ c) = true := by
    intro a b c h1 h2
    exact decide_eq_true (Int.le_trans (of_decide_eq_true h1) (of_decide_eq_true h2))
  have total : ∀ a b : Int, (decide (a ≤ b) || decide (b ≤ a)) = true := by
    intro a b
    rcases Int.le_total a b with h | h
    · rw [decide_eq_true h]; simp
    · rw [decide_eq_true h]; simp
  have hp : ((xs.filter (fun r => r.1 == (tag, parent))).map Prod.snd).mergeSort.Pairwise (fun x y : Int => decide (x ≤ y) = true) :=
    List.pairwise_mergeSort trans total _
  have hr : ((xs.filter (fun r => r.1 == (tag, parent))).map Prod.snd).mergeSort.reverse.Pairwise (fun a b : Int => decide (b ≤ a) = true) :=
    List.pairwise_reverse.mpr hp
  unfold countWhere
  show 1 + (List.takeWhile (fun v => decide (sales < v))
      (List.reverse ((List.map Prod.snd (List.filter (fun r => r.1 == (tag, parent)) xs)).mergeSort))).length =
    1 + (List.filter (fun r => (r.1 == (tag, parent)) && decide (sales < r.2)) xs).length
  rw [ftw_desc sales _ hr]
  have e1 : ((List.reverse ((xs.filter (fun r => r.1 == (tag, parent))).map Prod.snd).mergeSort)
      |>.filter (fun v => decide (sales < v))).length =
      (((xs.filter (fun r => r.1 == (tag, parent))).map Prod.snd).mergeSort
        |>.filter (fun v => decide (sales < v))).length :=
    ((List.reverse_perm _).filter (fun v => decide (sales < v))).length_eq
  have e2 : (((xs.filter (fun r => r.1 == (tag, parent))).map Prod.snd).mergeSort
      |>.filter (fun v => decide (sales < v))).length =
      ((xs.filter (fun r => r.1 == (tag, parent))).map Prod.snd
        |>.filter (fun v => decide (sales < v))).length :=
    ((List.mergeSort_perm _ _).filter (fun v => decide (sales < v))).length_eq
  rw [e1, e2, List.filter_map, List.length_map, List.filter_filter]
  rw [List.filter_congr (fun a _ => Bool.and_comm (decide (sales < a.snd)) (a.1 == (tag, parent)))]

#print axioms TinylambOpt.ResearchProofs.BenchmarkRematch.Tpcds_Q86DuckDB.sound
