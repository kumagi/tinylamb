import TinylambOpt.ProposedGoals.BenchmarkRematch.ClickBench

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
set_option linter.deprecated false

namespace TinylambOpt.ResearchProofs.ProposedGoals.ClickBench.Goal_Q04_DisjointHashPartitionDistinctSumGoal

open TinylambOpt.ResearchGoals.QueryRewrites
open TinylambOpt.ProposedGoals.BenchmarkRematch.ClickBench

private theorem filter_filter_comm {α : Type} (p q : α → Bool) (l : List α) :
    (l.filter p).filter q = (l.filter q).filter p := by
  simp [List.filter_filter, Bool.and_comm]

private theorem filter_filter_ne_of_false {α : Type} [BEq α] [LawfulBEq α]
    (a : α) (l : List α) (p : α → Bool) (hp : p a = false) :
    (l.filter p).filter (fun x => !x == a) = l.filter p := by
  induction l with
  | nil => simp
  | cons b l ih =>
      have ih' : List.filter (fun x => !x == a && p x) l = l.filter p := by
        simpa [List.filter_filter] using ih
      by_cases hba : b == a
      · have hb : b = a := beq_iff_eq.mp hba
        subst b
        simpa [hp] using congrArg (fun xs => a :: xs) ih'
      · have hnb : (!b == a) = true := by simpa using hba
        by_cases hpb : p b = true <;> simp [hnb, hpb, ih']

private theorem filter_eraseDups_eq {α : Type} [BEq α] [LawfulBEq α]
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
            have hle := List.length_filter_le (fun x => !x == a) l
            omega
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

private theorem sum_map_zero {α : Type} (l : List α) : (l.map (fun _ => 0)).sum = 0 := by
  induction l with
  | nil => simp
  | cons a l ih => simp [ih]

private theorem sum_map_add {α : Type} (l : List α) (f g : α → Nat) :
    (l.map (fun x => f x + g x)).sum = (l.map f).sum + (l.map g).sum := by
  induction l with
  | nil => simp
  | cons a l ih => simp [ih, Nat.add_assoc, Nat.add_comm, Nat.add_left_comm]

private theorem sum_range_eq_zero_of_le {k n : Nat} (h : n ≤ k) :
    ((List.range n).map (fun i => if k == i then 1 else 0)).sum = 0 := by
  induction n with
  | zero => simp
  | succ n ih =>
      rw [List.range_succ, List.map_append, List.sum_append]
      simp only [List.map_singleton, List.sum_cons, List.sum_nil, Nat.add_zero]
      have htail : n ≤ k := Nat.le_trans (Nat.le_succ n) h
      have hne : k ≠ n := by omega
      simpa [beq_iff_eq, hne] using ih htail

private theorem sum_range_eq_one_of_lt {k n : Nat} (hk : k < n) :
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

private theorem sum_filter_range_eq_length {α : Type} [BEq α] [LawfulBEq α]
    (xs : List α) (part : α → Nat) (numParts : Nat)
    (hbound : ∀ x ∈ xs, part x < numParts) :
    ((List.range numParts).map (fun p => (xs.filter (fun x => part x == p)).length)).sum = xs.length := by
  induction xs with
  | nil => simpa using sum_map_zero (List.range numParts)
  | cons a xs ih =>
      have htail : ∀ x ∈ xs, part x < numParts := by
        intro x hx
        exact hbound x (by simp [hx])
      have hterm : ∀ p : Nat,
          ((a :: xs).filter (fun x => part x == p)).length =
            (xs.filter (fun x => part x == p)).length + (if part a == p then 1 else 0) := by
        intro p
        by_cases h : part a == p
        · have heq : part a = p := beq_iff_eq.mp h
          simp [h, heq]
        · simp [h]
      calc
        ((List.range numParts).map (fun p =>
            ((a :: xs).filter (fun x => part x == p)).length)).sum
            = ((List.range numParts).map (fun p =>
                (xs.filter (fun x => part x == p)).length +
                  (if part a == p then 1 else 0))).sum := by
                  apply congrArg List.sum
                  apply List.map_congr_left
                  intro p hp
                  exact hterm p
        _ = ((List.range numParts).map (fun p =>
              (xs.filter (fun x => part x == p)).length)).sum +
              ((List.range numParts).map (fun p => if part a == p then 1 else 0)).sum := by
                  rw [sum_map_add]
        _ = xs.length + 1 := by
              rw [ih htail, sum_range_eq_one_of_lt (hbound a (by simp))]
        _ = (a :: xs).length := by simp

private theorem sum_range_partition_distinct {α : Type} [BEq α] [LawfulBEq α]
    (xs : List α) (part : α → Nat) (numParts : Nat)
    (hbound : ∀ x ∈ xs, part x < numParts) :
    ((List.range numParts).map (fun p =>
      (xs.filter (fun x => part x == p)).eraseDups.length)).sum = xs.eraseDups.length := by
  calc
    ((List.range numParts).map (fun p =>
      (xs.filter (fun x => part x == p)).eraseDups.length)).sum
        = ((List.range numParts).map (fun p =>
            (xs.eraseDups.filter (fun x => part x == p)).length)).sum := by
              apply congrArg List.sum
              apply List.map_congr_left
              intro p hp
              exact congrArg List.length (filter_eraseDups_eq (fun x => part x == p) xs)
    _ = xs.eraseDups.length := by
      apply sum_filter_range_eq_length
      intro x hx
      exact hbound x (List.mem_eraseDups.mp hx)

theorem sound : Q04_DisjointHashPartitionDistinctSumGoal := by
  intro xs part numParts hbound
  exact (sum_range_partition_distinct xs part numParts hbound).symm

end TinylambOpt.ResearchProofs.ProposedGoals.ClickBench.Goal_Q04_DisjointHashPartitionDistinctSumGoal
