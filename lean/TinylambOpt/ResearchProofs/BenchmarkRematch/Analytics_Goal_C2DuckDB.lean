import TinylambOpt.ResearchGoals.BenchmarkRematch.Analytics
import TinylambOpt.ResearchGoals.BenchmarkRematch.Analytics

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.BenchmarkRematch.Analytics.Goal_C2DuckDB
open TinylambOpt.ResearchGoals.BenchmarkRematch

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

set_option maxHeartbeats 0

theorem len_flatMap_filter_count (commenters eligible : List Nat) :
    (commenters.flatMap (fun k => eligible.filter (· == k))).length =
      (commenters.map (fun k => eligible.count k)).sum := by
  induction commenters with
  | nil => simp
  | cons k t ih =>
      simp [ih, List.count_eq_length_filter]

theorem sound : TinylambOpt.ResearchGoals.BenchmarkRematch.Analytics.C2DuckDB.Goal := by
  intro commenters eligibleUsers
  dsimp only
  rw [sum_group (fun k => eligibleUsers.count k) commenters]
  exact (len_flatMap_filter_count commenters eligibleUsers).symm

end TinylambOpt.ResearchProofs.BenchmarkRematch.Analytics.Goal_C2DuckDB

#print axioms TinylambOpt.ResearchProofs.BenchmarkRematch.Analytics.Goal_C2DuckDB.sound
