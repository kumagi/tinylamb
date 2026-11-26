import TinylambOpt.ResearchGoals.QueryRewrites.AnalyticalJoins

/-!
# CompanyWeightedMovieYearTotals

Aggregating company matches by movie and weighting the title's year by their
multiplicity preserves the modeled count and year sum.

The executor can look up each title once per movie key and account for all qualifying
company matches with weights.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000


namespace TinylambOpt.ResearchProofs.QueryRewrites.AnalyticalJoins.CompanyWeightedMovieYearTotals
open TinylambOpt.ResearchGoals.QueryRewrites
open TinylambOpt.ResearchGoals.QueryRewrites

theorem forKey_cons (rest : List (Nat × Int)) (k : Nat) (y : Int) (c : Nat) :
    forKey ((k,y)::rest) c = if k == c then y :: forKey rest c else forKey rest c := by
  by_cases h : (k == c) = true <;> simp [forKey, h]

theorem len_one (rest : List (Nat × Int)) (k : Nat) (y : Int) (companies : List Nat) :
    (companies.flatMap (fun c => forKey ((k,y)::rest) c)).length =
      companies.count k + (companies.flatMap (forKey rest)).length := by
  induction companies with
  | nil => simp
  | cons c cs ih =>
      rw [List.flatMap_cons, forKey_cons]
      by_cases h : (k == c) = true
      · have hkc : k = c := by simpa using h
        subst c
        simp [ih]
        omega
      · have hkc : k ≠ c := by intro h'; exact h (by simp [h'])
        have hck : c ≠ k := fun h' => hkc h'.symm
        simp [h, hck, ih]
        omega

theorem sum_map_zero_nat (l : List Nat) : (l.map (fun _ => 0)).sum = 0 := by
  induction l with
  | nil => simp
  | cons x xs ih => simp [ih]

theorem flatMap_empty_sum (l : List Nat) : (l.flatMap (fun _ => ([] : List Int))).sum = 0 := by
  induction l with
  | nil => simp
  | cons x xs ih => simp [ih]

theorem sum_one (rest : List (Nat × Int)) (k : Nat) (y : Int) (companies : List Nat) :
    (companies.flatMap (fun c => forKey ((k,y)::rest) c)).sum =
      y * (companies.count k : Int) + (companies.flatMap (forKey rest)).sum := by
  induction companies with
  | nil => simp
  | cons c cs ih =>
      rw [List.flatMap_cons, forKey_cons]
      by_cases h : (k == c) = true
      · have hkc : k = c := by simpa using h
        subst c
        simp [ih, Int.mul_add, Int.add_assoc, Int.add_comm]
      · have hkc : k ≠ c := by intro h'; exact h (by simp [h'])
        have hck : c ≠ k := fun h' => hkc h'.symm
        simp [h, hck, ih]
        omega

theorem len_total (movieYears : List (Nat × Int)) (companies : List Nat) :
    (companies.flatMap (forKey movieYears)).length =
      (movieYears.map (fun r => companies.count r.1)).sum := by
  induction movieYears with
  | nil => simpa [forKey] using sum_map_zero_nat companies
  | cons r rest ih =>
      rcases r with ⟨k,y⟩
      rw [len_one, ih]
      simp

theorem sum_total (movieYears : List (Nat × Int)) (companies : List Nat) :
    (companies.flatMap (forKey movieYears)).sum =
      total movieYears (fun (k,y) => y*(companies.count k:Int)) := by
  induction movieYears with
  | nil =>
      change (companies.flatMap (fun _ => ([] : List Int))).sum = 0
      simpa using flatMap_empty_sum companies
  | cons r rest ih =>
      rcases r with ⟨k,y⟩
      rw [sum_one, ih]
      simp [total]

theorem sound : TinylambOpt.ResearchGoals.QueryRewrites.AnalyticalJoins.CompanyWeightedMovieYearTotals.Goal := by
  intro movieYears companies
  dsimp only
  exact Prod.ext (len_total movieYears companies) (sum_total movieYears companies)
end TinylambOpt.ResearchProofs.QueryRewrites.AnalyticalJoins.CompanyWeightedMovieYearTotals
