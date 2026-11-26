import TinylambOpt.ResearchGoals.November26.Analytics

set_option maxHeartbeats 0
set_option maxRecDepth 10000


namespace TinylambOpt.ResearchProofs.November26.Analytics.Goal_C2
open TinylambOpt.ResearchGoals.November26
open TinylambOpt.ResearchGoals.BenchmarkRematch

theorem sum_eq_count_mul_add_sum_ne (f : Nat → Nat) (a : Nat) : ∀ xs : List Nat,
    (xs.map f).sum = xs.count a * f a + ((xs.filter (fun x => ! x == a)).map f).sum
  | [] => by simp
  | x :: t => by
      have ih := sum_eq_count_mul_add_sum_ne f a t
      by_cases h : x == a
      · have hx : x = a := beq_iff_eq.mp h
        subst hx
        simp only [List.map_cons, List.sum_cons, List.count_cons, List.filter_cons, h]
        rw [ih]
        simp [Nat.add_mul]
        omega
      · simp [h, ih, List.count_cons, Nat.add_assoc, Nat.add_comm, Nat.add_left_comm]

theorem sum_map_eq_eraseDups_count (f : Nat → Nat) : ∀ xs : List Nat,
    (xs.map f).sum = (xs.eraseDups.map (fun a => xs.count a * f a)).sum
  | [] => by simp
  | a :: t => by
      let u := t.filter (fun x => ! x == a)
      have hdec := sum_eq_count_mul_add_sum_ne f a t
      have ihu := sum_map_eq_eraseDups_count f u
      have herase : (a :: t).eraseDups = a :: u.eraseDups := by
        simp [List.eraseDups_cons, u]
      have hcnt_b : ∀ b ∈ u.eraseDups, (a :: t).count b = u.count b := by
        intro b hb
        have hbu : b ∈ u := List.mem_eraseDups.mp hb
        have hnb' : (! b == a) = true := (List.mem_filter.mp hbu).2
        have hnb : (b == a) = false := by
          cases hbEq : b == a <;> simp [hbEq] at hnb' ⊢
        have hne : b ≠ a := by
          intro heq
          subst heq
          simp at hnb
        have hne' : a ≠ b := by
          intro heq
          exact hne heq.symm
        have hba : (a == b) = false := by
          simp [beq_iff_eq, hne']
        have hcount_u : u.count b = t.count b := by
          simpa [u] using (List.count_filter (p := fun x => ! x == a) (a := b) (l := t) (by simpa using hnb))
        simp [List.count_cons, hba, hcount_u]
      have htail : (u.eraseDups.map (fun b => (a :: t).count b * f b)).sum =
          (u.eraseDups.map (fun b => u.count b * f b)).sum := by
        exact congrArg List.sum (List.map_congr_left (fun b hb => by rw [hcnt_b b hb]))
      have hdec' : (t.map f).sum = t.count a * f a + (u.map f).sum := by
        simpa [u] using hdec
      have hhead : (a :: t).count a = t.count a + 1 := by simp [List.count_cons]
      rw [herase]
      simp only [List.map_cons, List.sum_cons]
      rw [hhead]
      rw [htail, ihu.symm, hdec']
      simp [Nat.add_mul, Nat.add_assoc, Nat.add_comm, Nat.add_left_comm]
termination_by xs => xs.length
decreasing_by
  simp [u]
  exact Nat.lt_of_le_of_lt (List.length_filter_le (fun x => ! x == a) t) (Nat.lt_succ_self t.length)

theorem length_flatMap_eq_sum_count (owned passed : List Nat) :
    (owned.flatMap (fun p => passed.filter (· == p))).length =
      (owned.map (fun p => passed.count p)).sum := by
  induction owned with
  | nil => rfl
  | cons p t ih =>
      simp [List.flatMap_cons, ih, List.count_eq_length_filter]

theorem sound : TinylambOpt.ResearchGoals.November26.Analytics.C2.Goal := by
  intro posts passedComments owner
  dsimp only
  rw [length_flatMap_eq_sum_count]
  rw [sum_map_eq_eraseDups_count]
  simp [frequency, Function.comp_def]


#print axioms TinylambOpt.ResearchProofs.November26.Analytics.Goal_C2.sound
end TinylambOpt.ResearchProofs.November26.Analytics.Goal_C2
