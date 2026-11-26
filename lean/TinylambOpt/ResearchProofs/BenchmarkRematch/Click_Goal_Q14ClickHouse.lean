import TinylambOpt.ResearchGoals.BenchmarkRematch.Click

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.BenchmarkRematch.Click.Click_Goal_Q14ClickHouse
open Std
open TinylambOpt.ResearchGoals.BenchmarkRematch
open TinylambOpt.ResearchGoals.BenchmarkRematch.Click

theorem filter_filter_comm {α : Type} (p q : α → Bool) (l : List α) :
    (l.filter p).filter q = (l.filter q).filter p := by
  simp [List.filter_filter, Bool.and_comm]

theorem filter_filter_ne_of_false {α : Type} [BEq α] [LawfulBEq α]
    (a : α) (l : List α) (p : α → Bool) (hp : p a = false) :
    (l.filter p).filter (fun x => !x == a) = l.filter p := by
  induction l with
  | nil => simp
  | cons b l ih =>
      have ih' : (List.filter (fun x => !x == a && p x) l) = l.filter p := by
        simpa [List.filter_filter] using ih
      by_cases hba : b == a
      · have hb : b = a := beq_iff_eq.mp hba
        subst b
        simpa [hp] using congrArg (fun xs => a :: xs) ih'
      · have hnb : (!b == a) = true := by simpa using hba
        by_cases hpb : p b = true <;> simp [hnb, hpb, ih']

theorem filter_eraseDups_eq {α : Type} [BEq α] [LawfulBEq α]
    (p : α → Bool) : ∀ l : List α, (l.filter p).eraseDups = (l.eraseDups).filter p := by
  intro l
  generalize hn : l.length = n
  revert l
  induction n using Nat.strongRecOn with
  | ind n ih =>
      intro l hn
      cases l with
      | nil => simp
      | cons a l =>
          have hlen : (List.filter (fun x => !x == a) l).length < n := by
            rw [← hn]
            simp
            exact lt_of_le_of_lt (List.length_filter_le _ l) (by simp)
          have hrec := ih (List.filter (fun x => !x == a) l).length hlen
            (List.filter (fun x => !x == a) l) rfl
          simp only [List.filter_cons]
          by_cases hp : p a = true
          · rw [if_pos hp]
            rw [List.eraseDups_cons (a := a) (as := l.filter p)]
            rw [List.eraseDups_cons (a := a) (as := l)]
            simp only [List.filter_cons, hp, Bool.true_and]
            rw [filter_filter_comm]
            exact congrArg (fun xs => a :: xs) hrec
          · have hpfalse : p a = false := by
              cases hpa : p a <;> simp [hpa] at hp ⊢
            rw [if_neg hp]
            rw [List.eraseDups_cons (a := a) (as := l)]
            simp only [List.filter_cons, hp, Bool.false_and]
            have hl : l.filter p = (List.filter (fun x => !x == a) l).filter p := by
              rw [filter_filter_comm]
              exact (filter_filter_ne_of_false a l p hpfalse).symm
            rw [hl]
            exact hrec

theorem sum_range_eq_zero_of_le {k n : Nat} (h : n ≤ k) :
    ((List.range n).map (fun i => if k == i then 1 else 0)).sum = 0 := by
  induction n with
  | zero => simp
  | succ n ih =>
      rw [List.range_succ, List.map_append, List.sum_append]
      simp only [List.map_singleton, List.sum_cons, List.sum_nil, Nat.add_zero]
      have htail : n ≤ k := Nat.le_trans (Nat.le_succ n) h
      have hne : k ≠ n := by omega
      simpa [beq_iff_eq, hne] using ih htail

theorem sum_range_eq_one_of_lt {k n : Nat} (hk : k < n) :
    ((List.range n).map (fun i => if k == i then 1 else 0)).sum = 1 := by
  induction n with
  | zero => omega
  | succ n ih =>
      rw [List.range_succ, List.map_append, List.sum_append]
      simp only [List.map_singleton, List.sum_cons, List.sum_nil, Nat.add_zero]
      by_cases hklt : k < n
      · have hne : k ≠ n := by omega
        simpa [beq_iff_eq, hne] using ih hklt
      · have hkeq : k = n := by omega
        have hz := sum_range_eq_zero_of_le (k := n) (n := n) (Nat.le_refl n)
        simpa [hkeq, beq_iff_eq] using hz

theorem sum_map_zero {α : Type} (l : List α) : (l.map (fun _ => 0)).sum = 0 := by
  induction l with
  | nil => simp
  | cons a l ih => simp [ih]

theorem sum_map_add {α : Type} (l : List α) (f g : α → Nat) :
    (l.map (fun x => f x + g x)).sum = (l.map f).sum + (l.map g).sum := by
  induction l with
  | nil => simp
  | cons a l ih => simp [ih, Nat.add_assoc, Nat.add_comm, Nat.add_left_comm]

theorem sum_filter_range_eq_length {n : Nat} (hn : 0 < n) (xs : List Nat) :
    ((List.range n).map (fun i => (xs.filter (fun u => u % n == i)).length)).sum = xs.length := by
  induction xs with
  | nil =>
      simpa using sum_map_zero (List.range n)
  | cons a xs ih =>
      have hterm : ∀ i : Nat,
          ((a :: xs).filter (fun u => u % n == i)).length =
            (xs.filter (fun u => u % n == i)).length + (if a % n == i then 1 else 0) := by
        intro i
        by_cases h : a % n == i
        · have heq : a % n = i := beq_iff_eq.mp h
          simp [h, heq]
        · simp [h]
      calc
        ((List.range n).map (fun i => ((a :: xs).filter (fun u => u % n == i)).length)).sum
            = ((List.range n).map (fun i =>
                (xs.filter (fun u => u % n == i)).length + (if a % n == i then 1 else 0))).sum := by
                  apply congrArg List.sum
                  apply List.map_congr_left
                  intro i hi
                  exact hterm i
        _ = ((List.range n).map (fun i => (xs.filter (fun u => u % n == i)).length)).sum +
              ((List.range n).map (fun i => if a % n == i then 1 else 0)).sum := by
                  rw [sum_map_add]
        _ = xs.length + 1 := by
                  rw [ih, sum_range_eq_one_of_lt (Nat.mod_lt a hn)]
        _ = (a :: xs).length := by simp

theorem sound : TinylambOpt.ResearchGoals.BenchmarkRematch.Click.Q14ClickHouse.Goal := by
  intro users n hn
  calc
    ((List.range n).map (fun i => (users.filter (fun u => u % n == i)).eraseDups.length)).sum
        = ((List.range n).map (fun i => (users.eraseDups.filter (fun u => u % n == i)).length)).sum := by
            apply congrArg List.sum
            apply List.map_congr_left
            intro i hi
            exact congrArg List.length (filter_eraseDups_eq (fun u => u % n == i) users)
    _ = users.eraseDups.length := sum_filter_range_eq_length hn users.eraseDups
end TinylambOpt.ResearchProofs.BenchmarkRematch.Click.Click_Goal_Q14ClickHouse
