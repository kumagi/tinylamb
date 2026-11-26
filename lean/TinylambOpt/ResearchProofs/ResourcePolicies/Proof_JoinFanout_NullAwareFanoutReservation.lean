import TinylambOpt.ResearchGoals.ResourcePolicies.JoinFanout

/-!
Proof contribution for ResourcePolicies_JoinFanout_NullAwareFanoutReservation. The theorem below
records a planning or execution decision that preserves the modeled
SQL result, or an explicit counterexample when the proposed rewrite is
unsound.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false
set_option linter.unusedSimpArgs false
set_option linter.deprecated false
namespace TinylambOpt.ResearchProofs.ResourcePolicies.JoinFanout.Goal_NullAwareFanoutReservation

open TinylambOpt.ResearchGoals.ResourcePolicies.JoinFanout

open TinylambOpt.ResearchGoals.QueryRewrites

private theorem sum_map_filter_ne (f : Nat → Nat) (a : Nat) (as : List Nat) :
    (as.map f).sum = as.count a * f a + ((as.filter (fun b => !(b == a))).map f).sum := by
  induction as with
  | nil => simp
  | cons b t ih =>
      by_cases hba : (b == a) = true
      · have hbeq : b = a := by simpa using hba
        subst b
        simp only [List.map_cons, List.sum_cons, List.count_cons_self]
        have hfilt : (a :: t).filter (fun b => !(b == a)) = t.filter (fun b => !(b == a)) := by
          simp
        rw [hfilt, ih]
        rw [Nat.add_mul]
        omega
      · have hbne : b ≠ a := by simpa using hba
        simp only [List.map_cons, List.sum_cons, List.count_cons_of_ne hbne]
        have hfilt : (b :: t).filter (fun b => !(b == a)) = b :: t.filter (fun b => !(b == a)) := by
          simp [hba]
        rw [hfilt]
        simp only [List.map_cons, List.sum_cons]
        rw [ih]
        omega

private theorem count_cons_filter (a x : Nat) (as : List Nat)
    (hx : x ∈ as.filter (fun b => !(b == a))) :
    (a :: as).count x = (as.filter (fun b => !(b == a))).count x := by
  simp only [List.mem_filter] at hx
  have hxa : (x == a) = false := by simpa using hx.2
  have hax : a ≠ x := by
    intro h
    subst x
    simp at hxa
  rw [List.count_cons_of_ne hax]
  rw [List.count_filter]
  simpa [hxa]

private theorem map_count_eq (a : Nat) (as : List Nat) : ∀ (l : List Nat),
    (∀ x ∈ l, x ∈ as.filter (fun b => !(b == a))) →
    l.map (fun x => (x, (a :: as).count x)) =
      l.map (fun x => (x, (as.filter (fun b => !(b == a))).count x))
  | [], _ => by simp
  | x :: t, h => by
      have hx := h x (by simp)
      simp only [List.map_cons]
      rw [count_cons_filter a x as hx]
      rw [map_count_eq a as t (fun y hy => h y (by simp [hy]))]

private theorem sum_group (f : Nat → Nat) : ∀ (xs : List Nat),
    ((frequency xs).map (fun (k, n) => n * f k)).sum = (xs.map f).sum
  | [] => by simp [frequency]
  | a :: as => by
      let r := as.filter (fun b => !(b == a))
      have ih_r := sum_group f r
      have hcons : (a :: as).count a = as.count a + 1 := by simp
      have hfreq : frequency (a :: as) = (a, (a :: as).count a) :: frequency r := by
        dsimp [r]
        unfold frequency
        rw [List.eraseDups_cons]
        simp only [List.map_cons]
        rw [map_count_eq a as (List.filter (fun b => !(b == a)) as).eraseDups
          (fun x hx => (List.mem_eraseDups).1 hx)]
      rw [hfreq]
      simp only [List.map_cons, List.sum_cons]
      rw [hcons, ih_r, sum_map_filter_ne f a as]
      change (as.count a + 1) * f a + ((as.filter (fun b => !(b == a))).map f).sum =
        f a + (as.count a * f a + ((as.filter (fun b => !(b == a))).map f).sum)
      rw [Nat.add_mul]
      omega
termination_by xs => xs.length
decreasing_by
  exact Nat.lt_of_le_of_lt (by simpa [r] using List.length_filter_le (fun b => !(b == a)) as) (by simp)

private theorem product_eq_sum_count (a b : Keys) :
    product a b = ((a.filterMap id).map (fun k => b.count (some k))).sum := by
  unfold product hist
  exact sum_group (fun k => b.count (some k)) (a.filterMap id)

private theorem filter_hit_none (b : Keys) : (b.filter (hit none)).length = 0 := by
  simp [hit]

private theorem filter_hit_some (k : Nat) (b : Keys) :
    (b.filter (hit (some k))).length = b.count (some k) := by
  rw [List.count_eq_length_filter]
  apply congrArg List.length
  apply List.filter_congr
  intro j hj
  cases j with
  | none => simp [hit]
  | some j => simp [hit, Bool.beq_comm]

theorem sound : TinylambOpt.ResearchGoals.ResourcePolicies.JoinFanout.NullAwareFanoutReservation.Goal := by
  intro a b
  rw [product_eq_sum_count]
  unfold pairs
  rw [List.length_flatMap]
  induction a with
  | nil => simp
  | cons k ks ih =>
      cases k with
      | none => simp [filter_hit_none, ih]
      | some k =>
          simp only [List.filterMap_cons, id_eq, List.map_cons, List.sum_cons]
          rw [ih, filter_hit_some]

end TinylambOpt.ResearchProofs.ResourcePolicies.JoinFanout.Goal_NullAwareFanoutReservation
