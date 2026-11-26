import TinylambOpt.ResearchGoals.QueryRewrites.AnalyticalJoins

/-!
# ParentReplyCountAndScoreTotals

Grouping qualifying replies by parent preserves their count and score sum when the
totals are attributed to the parent's year.

The executor can probe parent messages once per group rather than once per reply.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
set_option linter.unnecessarySimpa false
set_option linter.deprecated false


namespace TinylambOpt.ResearchProofs.QueryRewrites.AnalyticalJoins.ParentReplyCountAndScoreTotals
open TinylambOpt.ResearchGoals.QueryRewrites
open TinylambOpt.ResearchGoals.QueryRewrites

theorem sum_map_filter_ne (f : Nat → Nat) (a : Nat) (as : List Nat) :
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

theorem count_cons_filter (a x : Nat) (as : List Nat)
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

theorem map_count_eq (a : Nat) (as : List Nat) : ∀ (l : List Nat),
    (∀ x ∈ l, x ∈ as.filter (fun b => !(b == a))) →
    l.map (fun x => (x, (a :: as).count x)) =
      l.map (fun x => (x, (as.filter (fun b => !(b == a))).count x))
  | [], _ => by simp
  | x :: t, h => by
      have hx := h x (by simp)
      simp only [List.map_cons]
      rw [count_cons_filter a x as hx]
      rw [map_count_eq a as t (fun y hy => h y (by simp [hy]))]

theorem sum_group (f : Nat → Nat) : ∀ (xs : List Nat),
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

theorem total_map_filter_ne (f : Nat → Int) (a : Nat) (as : List Nat) :
    (as.map f).sum = (as.count a : Int) * f a + ((as.filter (fun b => !(b == a))).map f).sum := by
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
        rw [Int.natCast_add, Int.add_mul]
        simp [Int.add_assoc, Int.add_comm, Int.add_left_comm]
      · have hbne : b ≠ a := by simpa using hba
        simp only [List.map_cons, List.sum_cons, List.count_cons_of_ne hbne]
        have hfilt : (b :: t).filter (fun b => !(b == a)) = b :: t.filter (fun b => !(b == a)) := by
          simp [hba]
        rw [hfilt]
        simp only [List.map_cons, List.sum_cons]
        rw [ih]
        omega

theorem count_cons_filter_int (a x : Nat) (as : List Nat)
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
  simp [hxa]

theorem map_count_eq_int (a : Nat) (as : List Nat) : ∀ (l : List Nat),
    (∀ x ∈ l, x ∈ as.filter (fun b => !(b == a))) →
    l.map (fun x => (x, (a :: as).count x)) =
      l.map (fun x => (x, (as.filter (fun b => !(b == a))).count x))
  | [], _ => by simp
  | x :: t, h => by
      have hx := h x (by simp)
      simp only [List.map_cons]
      rw [count_cons_filter_int a x as hx]
      rw [map_count_eq_int a as t (fun y hy => h y (by simp [hy]))]

theorem total_group (f : Nat → Int) : ∀ (xs : List Nat),
    total (frequency xs) (fun (k, n) => (n : Int) * f k) = (xs.map f).sum
  | [] => by simp [total, frequency]
  | a :: as => by
      let r := as.filter (fun b => !(b == a))
      have ih_r := total_group f r
      have ih_r' : ((frequency r).map (fun (k, n) => (n : Int) * f k)).sum = (r.map f).sum := by
        simpa [total] using ih_r
      have hcons : (a :: as).count a = as.count a + 1 := by simp
      have hfreq : frequency (a :: as) = (a, (a :: as).count a) :: frequency r := by
        dsimp [r]
        unfold frequency
        rw [List.eraseDups_cons]
        simp only [List.map_cons]
        rw [map_count_eq_int a as (List.filter (fun b => !(b == a)) as).eraseDups
          (fun x hx => (List.mem_eraseDups).1 hx)]
      rw [hfreq]
      simp only [total, List.map_cons, List.sum_cons]
      rw [hcons, ih_r', total_map_filter_ne f a as]
      change (((as.count a + 1 : Nat) : Int) * f a) +
          ((as.filter (fun b => !(b == a))).map f).sum =
        f a + ((as.count a : Int) * f a + ((as.filter (fun b => !(b == a))).map f).sum)
      rw [Int.natCast_add, Int.add_mul]
      simp [Int.add_assoc, Int.add_comm, Int.add_left_comm]
termination_by xs => xs.length
decreasing_by
  simp_wf
  exact Nat.lt_of_le_of_lt (by simpa [r] using List.length_filter_le (fun b => !(b == a)) as) (by simp)

theorem len_flatMap (cast : List Nat) (years : Nat → List Int) :
    (cast.flatMap years).length = (cast.map (fun k => (years k).length)).sum := by
  induction cast with
  | nil => simp
  | cons k t ih => simp [ih]

theorem sum_flatMap (cast : List Nat) (years : Nat → List Int) :
    (cast.flatMap years).sum = (cast.map (fun k => (years k).sum)).sum := by
  induction cast with
  | nil => simp
  | cons k t ih => simp [ih, List.sum_append]

theorem sound : TinylambOpt.ResearchGoals.QueryRewrites.AnalyticalJoins.ParentReplyCountAndScoreTotals.Goal := by
  intro children parents
  let replies := fun p => (forKey children (some p)).filter (0 < ·)
  change ((parents.flatMap replies).length, (parents.flatMap replies).sum) =
    (((frequency parents).map (fun (p, n) => n * (replies p).length)).sum,
     total (frequency parents) (fun (p, n) => (n : Int) * (replies p).sum))
  apply Prod.ext
  · rw [len_flatMap]
    exact (sum_group (fun p => (replies p).length) parents).symm
  · rw [sum_flatMap]
    exact (total_group (fun p => (replies p).sum) parents).symm
end TinylambOpt.ResearchProofs.QueryRewrites.AnalyticalJoins.ParentReplyCountAndScoreTotals
