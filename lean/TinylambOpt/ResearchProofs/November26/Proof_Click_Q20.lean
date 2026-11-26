import TinylambOpt.ResearchGoals.November26.Click

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
set_option linter.unusedVariables false

namespace TinylambOpt.ResearchProofs.November26.Click.Goal_Q20
open TinylambOpt.ResearchGoals.BenchmarkRematch
open TinylambOpt.ResearchGoals.November26.Click

theorem count_zero_of_all_gt (xs : List Int) (t : Int) (h : ∀ x ∈ xs, t < x) : xs.count t = 0 := by
  induction xs with
  | nil => simp
  | cons a xs ih =>
      have hne : (a == t) = false := by
        simp
        have := h a (by simp)
        omega
      have ht : ∀ x ∈ xs, t < x := by
        intro x hx
        exact h x (by simp [hx])
      simp [List.count_cons, hne, ih ht]

theorem takeWhile_len_mono (xs : List Int) (t : Int) :
    (xs.takeWhile (fun x => x < t)).length ≤ (xs.takeWhile (fun x => x ≤ t)).length := by
  induction xs with
  | nil => simp
  | cons a xs ih =>
      by_cases hlt : a < t
      · have hle : a ≤ t := by omega
        simp [hlt, hle, ih]
      · by_cases hle : a ≤ t
        · simp [hlt, hle]
        · simp [hlt, hle]

theorem takeWhile_le_len_of_all_ge_sorted : ∀ (xs : List Int) t, xs.Pairwise (fun a b => a ≤ b) →
    (∀ x ∈ xs, t ≤ x) → (xs.takeWhile (fun x => x ≤ t)).length = xs.count t
  | [], t, hs, hall => by simp
  | a :: xs, t, hs, hall => by
      have hs' := List.pairwise_cons.mp hs
      have hge : t ≤ a := hall a (by simp)
      by_cases hale : a ≤ t
      · have heq : a = t := by omega
        subst a
        have ht : ∀ x ∈ xs, t ≤ x := by
          intro x hx
          exact hall x (by simp [hx])
        have ih := takeWhile_le_len_of_all_ge_sorted xs t hs'.2 ht
        simp [hale, List.count_cons_self, ih]
      · have hgt : t < a := by omega
        have hcnt : xs.count t = 0 := count_zero_of_all_gt xs t (by
          intro x hx
          have hx' : a ≤ x := hs'.1 x hx
          omega)
        have hne : (a == t) = false := by
          simp
          omega
        simp [hale, hne, hcnt, List.count_cons]

theorem count_le_sub_lt : ∀ (xs : List Int) t, xs.Pairwise (fun a b => a ≤ b) →
    (xs.takeWhile (fun x => x ≤ t)).length - (xs.takeWhile (fun x => x < t)).length = xs.count t
  | [], t, hs => by simp
  | a :: xs, t, hs => by
      have hs' := List.pairwise_cons.mp hs
      have hge_all : ∀ x ∈ xs, a ≤ x := hs'.1
      have hs_tail : xs.Pairwise (fun a b : Int => a ≤ b) := hs'.2
      have ih := count_le_sub_lt xs t hs_tail
      have hmono := takeWhile_len_mono xs t
      by_cases halt : a < t
      · have hale : a ≤ t := by omega
        have hne : (a == t) = false := by
          simp
          omega
        simp [halt, hale, List.count_cons, hne]
        omega
      · have ht_le_a : t ≤ a := by omega
        by_cases hale : a ≤ t
        · have heq : a = t := by omega
          subst a
          have hall : ∀ x ∈ xs, t ≤ x := by
            intro x hx
            exact hge_all x hx
          have hc := takeWhile_le_len_of_all_ge_sorted xs t hs_tail hall
          simp [halt, hale, List.count_cons_self, hc]
        · have hgt : t < a := by omega
          have hcnt : xs.count t = 0 := count_zero_of_all_gt xs t (by
            intro x hx
            have hx' : a ≤ x := hge_all x hx
            omega)
          have hne : (a == t) = false := by
            simp
            omega
          simp [halt, hale, hcnt, hne, List.count_cons]

theorem sound : TinylambOpt.ResearchGoals.November26.Click.Q20.Goal := by
  intro ids target hs
  exact count_le_sub_lt ids target hs
end TinylambOpt.ResearchProofs.November26.Click.Goal_Q20
