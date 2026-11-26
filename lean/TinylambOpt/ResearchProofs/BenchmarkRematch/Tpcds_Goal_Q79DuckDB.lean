import TinylambOpt.ResearchGoals.BenchmarkRematch.Tpcds

set_option maxHeartbeats 0
namespace TinylambOpt.ResearchProofs.BenchmarkRematch.Tpcds.Goal_Q79DuckDB
open TinylambOpt.ResearchGoals.BenchmarkRematch
open TinylambOpt.ResearchGoals.BenchmarkRematch.Tpcds

theorem countP_all_eq {α : Type} (p : α → Bool) (a : α) : ∀ (l : List α),
    (∀ x ∈ l, x = a) → List.countP p l = if p a = true then l.length else 0 := by
  intro l
  induction l with
  | nil => simp
  | cons x xs ih =>
      intro hall
      have hx : x = a := hall x (by simp)
      subst x
      have hxs : ∀ y ∈ xs, y = a := by
        intro y hy
        exact hall y (by simp [hy])
      by_cases hpa : p a = true <;> simp [hpa, ih hxs]

theorem count_cons_of_ne {α : Type} [BEq α] [LawfulBEq α] (a x : α) (t : List α)
    (h : (x == a) = false) : List.count x (a :: t) = List.count x t := by
  rw [List.count_cons]
  have h' : (a == x) = false := by
    by_cases hax : (a == x) = true
    · have heq : a = x := by simpa using hax
      have hxa : (x == a) = true := by simpa [heq]
      simp [hxa] at h
    · simp [hax]
  simp [h']

theorem filter_map_pair {α β : Type} (p : α → Bool) (f : α → β) : ∀ l : List α,
    ((l.map (fun x => (x, f x))).filter (fun q => p q.1)).map Prod.snd =
      (l.filter p).map f := by
  intro l
  induction l with
  | nil => simp
  | cons a l ih =>
      by_cases h : p a = true <;> simp [h, ih]

theorem freq_sum {α : Type} [BEq α] [LawfulBEq α] (p : α → Bool) (xs : List α) :
    ((xs.eraseDups.filter p).map (fun x => xs.count x)).sum = xs.countP p := by
  cases xs with
  | nil => simp [List.countP_eq_length_filter]
  | cons a t =>
      let rest := t.filter (fun b : α => !b == a)
      have ih := freq_sum p rest
      have hne : ∀ x ∈ rest, (x == a) = false := by
        intro x hx
        simpa using (List.mem_filter.mp hx).2
      have hcnt_ne : ∀ x ∈ rest, List.count x (a :: t) = List.count x t := by
        intro x hx
        exact count_cons_of_ne a x t (hne x hx)
      have hsum_rest :
          ((rest.eraseDups.filter p).map (fun x => List.count x (a :: t))).sum =
            ((rest.eraseDups.filter p).map (fun x => List.count x t)).sum := by
        apply congrArg List.sum
        apply List.map_congr_left
        intro x hx
        have hxr : x ∈ rest := List.mem_eraseDups.mp (List.mem_filter.mp hx).1
        exact hcnt_ne x hxr
      have hcnt_rest : ∀ x ∈ rest, List.count x rest = List.count x t := by
        intro x hx
        have hq : (fun b : α => !b == a) x = true := by simpa [hne x hx]
        exact List.count_filter hq
      have hsum_rest2 :
          ((rest.eraseDups.filter p).map (fun x => List.count x rest)).sum =
            ((rest.eraseDups.filter p).map (fun x => List.count x t)).sum := by
        apply congrArg List.sum
        apply List.map_congr_left
        intro x hx
        have hxr : x ∈ rest := List.mem_eraseDups.mp (List.mem_filter.mp hx).1
        exact hcnt_rest x hxr
      rw [hsum_rest2] at ih
      have hsplit :
          List.countP p (a :: t) =
            (if p a = true then List.count a (a :: t) else 0) + List.countP p rest := by
        rw [List.countP_eq_countP_filter_add (a :: t) p (fun x => x == a)]
        have hall : ∀ x ∈ List.filter (fun x : α => x == a) (a :: t), x = a := by
          intro x hx
          exact beq_iff_eq.mp (List.mem_filter.mp hx).2
        have hfirst :
            List.countP p (List.filter (fun x : α => x == a) (a :: t)) =
              if p a = true then List.count a (a :: t) else 0 := by
          rw [countP_all_eq p a _ hall]
          rw [List.count_eq_length_filter]
        have hrest_eq : List.filter (fun x : α => !(x == a)) (a :: t) = rest := by
          simp [rest]
        rw [hfirst, hrest_eq]
      rw [List.eraseDups_cons]
      change (((a :: rest.eraseDups).filter p).map (fun x => List.count x (a :: t))).sum =
        List.countP p (a :: t)
      by_cases hpa : p a = true
      · simp only [List.filter_cons]
        rw [if_pos hpa]
        simp only [List.map_cons, List.sum_cons]
        rw [hsum_rest, ih, hsplit]
        simp [hpa]
      · simp only [List.filter_cons]
        rw [if_neg hpa]
        rw [hsum_rest, ih, hsplit]
        simp [hpa]
termination_by xs.length
decreasing_by
  simpa [rest] using Nat.lt_succ_of_le (List.length_filter_le (fun b : α => !b == a) t)

theorem sound : TinylambOpt.ResearchGoals.BenchmarkRematch.Tpcds.Q79DuckDB.Goal := by
  intro cities name
  have hfreq :
      ((frequency cities).filter (fun p => (p.1.take 30).toString == name) |>.map Prod.snd).sum =
        ((cities.eraseDups.filter (fun s => (s.take 30).toString == name)).map
          (fun x => cities.count x)).sum := by
    unfold frequency
    exact congrArg List.sum (filter_map_pair (α := String) (β := Nat)
      (fun s : String => (s.take 30).toString == name)
      (fun x : String => cities.count x) cities.eraseDups)
  rw [hfreq]
  rw [freq_sum]
  rw [List.countP_eq_length_filter]
  rfl

end TinylambOpt.ResearchProofs.BenchmarkRematch.Tpcds.Goal_Q79DuckDB

#print axioms TinylambOpt.ResearchProofs.BenchmarkRematch.Tpcds.Goal_Q79DuckDB.sound
