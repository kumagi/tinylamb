import TinylambOpt.ResearchGoals.BenchmarkRematch.Click
import TinylambOpt.ResearchProofs.BenchmarkRematch.Group
import TinylambOpt.ResearchProofs.BenchmarkRematch.Nsum
import TinylambOpt.ResearchProofs.BenchmarkRematch.Topn
import TinylambOpt.ResearchProofs.BenchmarkRematch.Dedup

set_option maxHeartbeats 0
namespace TinylambOpt.ResearchProofs.BenchmarkRematch.Goal_Click_Q14ClickHouse
open TinylambOpt.ResearchGoals.BenchmarkRematch
open TinylambOpt.ResearchGoals.BenchmarkRematch.Click
open TinylambOpt.ResearchProofs.BenchmarkRematch.Group
open TinylambOpt.ResearchProofs.BenchmarkRematch.Nsum
open TinylambOpt.ResearchProofs.BenchmarkRematch.Topn
open TinylambOpt.ResearchProofs.BenchmarkRematch.Dedup

theorem sum_map_add_aux {α : Type} (l : List α) (f g : α → Nat) :
    (l.map (fun x => f x + g x)).sum = (l.map f).sum + (l.map g).sum := by
  induction l with
  | nil => rfl
  | cons a l ih => simp [ih]; omega

theorem zero_sum {α : Type} (l : List α) : (l.map (fun _ => 0)).sum = 0 := by
  induction l with
  | nil => rfl
  | cons a l ih => simpa using ih

theorem indicator_sum : ∀ (n j : Nat), j < n →
    ((List.range n).map (fun i => if j == i then 1 else 0)).sum = 1 := by
  intro n
  induction n with
  | zero => intro j h; omega
  | succ n ih =>
    intro j h
    rw [List.range_succ, List.map_append, List.sum_append_nat]
    by_cases hj : j = n
    · subst hj
      have : ((List.range j).map (fun i => if j == i then 1 else 0)).sum = 0 := by
        have : (List.range j).map (fun i => if j == i then 1 else 0) = (List.range j).map (fun _ => 0) := by
          apply List.map_congr_left
          intro i hi
          have := List.mem_range.mp hi
          simp; omega
        rw [this]; exact zero_sum _
      simpa using this
    · have := ih j (by omega)
      simp at this ⊢
      simp [this, hj]

theorem class_sum (n : Nat) (hn : 0 < n) : ∀ E : List Nat,
    ((List.range n).map (fun i => (E.filter (fun u => u % n == i)).length)).sum = E.length := by
  intro E
  induction E with
  | nil => exact zero_sum _
  | cons x E ih =>
    have h1 : ∀ i, ((x :: E).filter (fun u => u % n == i)).length
        = (if x % n == i then 1 else 0) + (E.filter (fun u => u % n == i)).length := by
      intro i
      by_cases h : x % n == i
      · simp [List.filter_cons, h]; omega
      · simp [List.filter_cons, h]
    simp only [h1]
    rw [sum_map_add_aux, ih, indicator_sum n (x % n) (Nat.mod_lt _ hn)]
    simp; omega

theorem sound : Q14ClickHouse.Goal := by
  intro users n hn
  have : ∀ i, (users.filter (fun u => u % n == i)).eraseDups.length
      = (users.eraseDups.filter (fun u => u % n == i)).length := by
    intro i; rw [ed_filter]
  simp only [this]
  exact class_sum n hn users.eraseDups

end TinylambOpt.ResearchProofs.BenchmarkRematch.Goal_Click_Q14ClickHouse

#print axioms TinylambOpt.ResearchProofs.BenchmarkRematch.Goal_Click_Q14ClickHouse.sound
