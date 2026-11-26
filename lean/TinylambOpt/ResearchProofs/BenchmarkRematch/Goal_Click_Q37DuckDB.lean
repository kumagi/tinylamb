import TinylambOpt.ResearchGoals.BenchmarkRematch.Click

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false

namespace TinylambOpt.ResearchProofs.BenchmarkRematch.Click_Q37DuckDB
open TinylambOpt.ResearchGoals.BenchmarkRematch.Click
open TinylambOpt.ResearchGoals.BenchmarkRematch

private theorem sum_le_sum {β : Type} (f g : β → Nat) :
    ∀ (l : List β), (∀ x ∈ l, f x ≤ g x) → (l.map f).sum ≤ (l.map g).sum := by
  intro l
  induction l with
  | nil => intro _; simp
  | cons x t ih =>
    intro h
    simp only [List.map_cons, List.sum_cons, List.forall_mem_cons] at h ⊢
    exact Nat.add_le_add h.1 (ih h.2)

private theorem page_bound (i : Nat) (page : List Nat) :
    page.count i ≤ (if page.contains i then page.length else 0) := by
  by_cases h : i ∈ page
  · have hc : page.contains i = true := by simpa using h
    rw [if_pos hc]
    exact List.count_le_length
  · have hcn : ¬(page.contains i = true) := by
      intro hh
      apply h
      simpa using hh
    rw [if_neg hcn]
    rw [List.count_eq_zero_of_not_mem h]
    exact Nat.le_refl 0

private theorem filter_le (keys : List Nat) (p q : Nat → Bool)
    (h : ∀ k ∈ keys, p k = true → q k = true) :
    (keys.filter p).length ≤ (keys.filter q).length := by
  induction keys with
  | nil => simp
  | cons x t ih =>
    have h' : ∀ k ∈ t, p k = true → q k = true := fun k hk => h k (List.mem_cons_of_mem x hk)
    have hi := ih h'
    by_cases hp : (p x) = true
    · have hx : (q x) = true := h x List.mem_cons_self hp
      simp only [List.filter_cons, if_pos hp, if_pos hx, List.length_cons]
      omega
    · by_cases hqx : (q x) = true
      · simp only [List.filter_cons, if_neg hp, if_pos hqx, List.length_cons]
        omega
      · simp only [List.filter_cons, if_neg hp, if_neg hqx]
        omega

theorem sound : Q37DuckDB.Goal := by
  intro seen remaining keys i threshold hnd
  simp only [countWhere]
  have hflat : (remaining.flatten).count i = ((remaining.map fun page => page.count i)).sum := by
    induction remaining with
    | nil => rfl
    | cons page t ih =>
      rw [List.flatten_cons, List.map_cons, List.sum_cons, List.count_append, ih]
  have hsum : ((remaining.map fun page => page.count i)).sum ≤
      ((remaining.map fun page => if page.contains i then page.length else 0)).sum :=
    sum_le_sum (fun page => page.count i)
      (fun page => if page.contains i then page.length else 0) remaining
      (fun page _ => page_bound i page)
  intro h10 hlt
  have hci : (seen.count i) + ((remaining.map fun page => page.count i)).sum < threshold :=
    Nat.lt_of_le_of_lt (Nat.add_le_add_left hsum _) hlt
  refine Nat.le_trans h10 (filter_le keys (fun k => threshold ≤ seen.count k)
    (fun k => (seen ++ remaining.flatten).count i < (seen ++ remaining.flatten).count k) ?_)
  intro k _ hk
  have hk' : threshold ≤ seen.count k := of_decide_eq_true hk
  have h2 : (seen ++ remaining.flatten).count k = seen.count k + (remaining.flatten).count k :=
    List.count_append
  rw [List.count_append, hflat, h2]
  have key1 : (seen.count i) + ((remaining.map fun page => page.count i)).sum <
      seen.count k + (remaining.flatten).count k := by
    omega
  simpa using key1

end TinylambOpt.ResearchProofs.BenchmarkRematch.Click_Q37DuckDB
